use std::{collections::HashMap, io::ErrorKind, sync::Arc};

use async_channel::{Receiver, RecvError, SendError, Sender, TryRecvError};
use image::{DynamicImage, ImageBuffer, Rgb, RgbImage};
use jpeg_encoder::{ColorType, Encoder, EncodingError};

use rocket::futures::Stream;
use tokio::{
    select,
    sync::{MappedMutexGuard, Mutex, MutexGuard},
    task::{JoinError, JoinHandle},
};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::instrument;
use uuid::Uuid;

use crate::{
    config::{IMAGE_CHUNK_SIZE, IMAGE_ENCODING_QUALITY},
    routes::images::ImageStream,
    services::image_service::r#gen::{Chunk, ImageGenerationParams, handle_chunk, vertical_chunks},
    util::channel_writer::blocking::ChannelWriter,
};

struct CompletedChunk {
    request_id: Uuid,
    chunk_id: u32,
    pixels: Vec<(u32, u32, Rgb<u8>)>,
}

impl std::fmt::Debug for CompletedChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompletedChunk")
            .field("request_id", &self.request_id)
            .field("chunk_id", &self.chunk_id)
            .field("pixels", &format_args!("{} pixels", self.pixels.len()))
            .finish()
    }
}

#[derive(Copy, Clone, Debug)]
struct RenderParams {
    pub height: u32,
    pub width: u32,
    pub index: u32,
}

impl From<ImageGenerationParams> for RenderParams {
    fn from(value: ImageGenerationParams) -> Self {
        Self {
            height: value.height,
            index: value.index,
            width: value.width,
        }
    }
}

#[derive(PartialEq, Eq, Copy, Clone, Debug, Hash)]
pub enum JobStatus {
    Idle,
    Rendering,
    RenderingComplete,
    Encoding,
}

impl Default for JobStatus {
    fn default() -> Self {
        Self::Idle
    }
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status = match self {
            Self::Idle => "idle",
            Self::Rendering => "rendering",
            Self::RenderingComplete => "rendering complete",
            Self::Encoding => "encoding",
        };

        f.write_str(status)
    }
}

struct ImageJob {
    request_id: Uuid,
    status: JobStatus,
    params: RenderParams,
    required_chunks: usize,
    pending_chunks: Vec<Chunk>,
    completed_chunks: Vec<CompletedChunk>,
    cancel_token: CancellationToken,
    transport: Option<tokio::sync::mpsc::Sender<Vec<u8>>>,
}

impl std::fmt::Debug for ImageJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImageJob")
            .field("request_id", &self.request_id)
            .field("status", &self.status)
            .field("params", &self.params)
            .field("required_chunks", &self.required_chunks)
            .field("pending_chunks", &self.pending_chunks.len())
            .field("completed_chunks", &self.completed_chunks.len())
            .field("finished", &"<oneshot::Sender>")
            .finish()
    }
}

impl ImageJob {
    #[instrument(
        skip(transport, chunks, params),
        fields(request_id = %request_id, chunks = chunks.len())
    )]
    pub fn new(
        request_id: Uuid,
        chunks: Vec<Chunk>,
        params: RenderParams,
        cancel_token: CancellationToken,
        transport: tokio::sync::mpsc::Sender<Vec<u8>>,
    ) -> Self {
        let required_chunks = chunks.len();
        Self {
            request_id,
            status: JobStatus::default(),
            required_chunks,
            params,
            pending_chunks: chunks,
            completed_chunks: Vec::new(),
            cancel_token,
            transport: Some(transport),
        }
    }
    fn update_status(&mut self, new_status: JobStatus) {
        tracing::debug!(
            from = ?self.status,
            to = ?new_status,
            "job state transition"
        );

        self.status = new_status;
    }
    fn request_dropped(&self) -> bool {
        self.cancel_token.is_cancelled()
    }
    #[instrument(skip_all)]
    pub fn take_chunk(&mut self) -> Result<RenderJob, ImageJobError> {
        if self.status == JobStatus::Idle {
            self.update_status(JobStatus::Rendering);
        }

        let expected = JobStatus::Rendering;
        if self.status != expected {
            return Err(ImageJobError::InvalidState(expected, self.status));
        }

        let chunk = self
            .pending_chunks
            .pop()
            .ok_or(ImageJobError::PendingChunkNotFound)?;

        tracing::debug!(
            completed = self.completed_chunks.len(),
            remaining = self.pending_chunks.len(),
            "render chunk acquired"
        );

        Ok(RenderJob {
            request_id: self.request_id,
            params: self.params,
            chunk,
        })
    }
    #[instrument(skip_all)]
    pub fn take_encode(&mut self) -> Result<EncodeJob, ImageJobError> {
        let expected = JobStatus::RenderingComplete;
        if self.status != expected {
            return Err(ImageJobError::InvalidState(expected, self.status));
        }

        self.update_status(JobStatus::Encoding);

        tracing::debug!("encode job created");

        Ok(EncodeJob {
            request_id: self.request_id,
            params: self.params,
            chunks: self.completed_chunks.drain(..).collect(),
            transport: self.transport.take().unwrap(),
        })
    }
    #[instrument(skip_all)]
    pub fn on_render_complete(
        &mut self,
        chunk: CompletedChunk,
    ) -> Result<JobStatus, ImageJobError> {
        let expected = JobStatus::Rendering;
        if self.status != expected {
            return Err(ImageJobError::InvalidState(expected, self.status));
        }

        self.completed_chunks.push(chunk);

        if self.completed_chunks.len() == self.required_chunks {
            self.update_status(JobStatus::RenderingComplete);
        }

        tracing::debug!(
            completed = self.completed_chunks.len(),
            required = self.required_chunks,
            "render chunk completed"
        );

        Ok(self.status)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ImageJobError {
    #[error("invalid job state: expected {0:?}, got {1:?}")]
    InvalidState(JobStatus, JobStatus),
    #[error("no pending chunk available")]
    PendingChunkNotFound,
}

struct JobStore {
    jobs: Arc<Mutex<HashMap<Uuid, ImageJob>>>,
}

impl JobStore {
    pub fn new() -> Self {
        Self {
            jobs: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl JobStore {
    #[instrument(skip_all)]
    pub async fn insert(&self, request_id: Uuid, job: ImageJob) -> Result<(), JobStoreError> {
        tracing::debug!("Inserting image job");

        let mut guard = self.jobs.lock().await;

        if guard.insert(request_id, job).is_some() {
            tracing::debug!("Duplicate image job detected");
            return Err(JobStoreError::DuplicateJobs);
        }

        tracing::debug!("Image job inserted successfully");
        Ok(())
    }
    #[instrument(skip_all)]
    pub async fn get_mut(
        &self,
        request_id: &Uuid,
    ) -> Result<MappedMutexGuard<'_, ImageJob>, JobStoreError> {
        tracing::debug!("Getting mutable image job");

        let guard = self.jobs.lock().await;

        MutexGuard::try_map(guard, |map| map.get_mut(request_id))
            .map(|job| {
                tracing::debug!("Image job found");
                job
            })
            .map_err(|_| {
                tracing::debug!("Image job not found");
                JobStoreError::JobNotFound
            })
    }
    #[instrument(skip_all)]
    pub async fn remove(&self, request_id: &Uuid) -> Result<ImageJob, JobStoreError> {
        tracing::debug!("Removing image job");

        let mut guard = self.jobs.lock().await;

        match guard.remove(request_id) {
            Some(job) => {
                tracing::debug!("Image job removed successfully");
                Ok(job)
            }
            None => {
                tracing::debug!("Image job not found");
                Err(JobStoreError::JobNotFound)
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum JobStoreError {
    #[error("multiple jobs created with the same request_id")]
    DuplicateJobs,
    #[error("job not found")]
    JobNotFound,
}

#[derive(Debug)]
struct RenderJob {
    request_id: Uuid,
    params: RenderParams,
    chunk: Chunk,
}

struct RenderWorker {
    worker_id: Uuid,
    store: Arc<JobStore>,
    render_rx: Receiver<Uuid>,
    encode_tx: Sender<Uuid>,
}

impl RenderWorker {
    fn new(store: &Arc<JobStore>, render_rx: &Receiver<Uuid>, encode_tx: &Sender<Uuid>) -> Self {
        Self {
            worker_id: Uuid::new_v4(),
            render_rx: render_rx.clone(),
            encode_tx: encode_tx.clone(),
            store: store.clone(),
        }
    }
}

impl RenderWorker {
    #[instrument(skip(self))]
    fn start(self) -> JoinHandle<()> {
        tokio::task::spawn(render_worker(self))
    }
}

#[instrument(skip(worker), fields(worker_id = %worker.worker_id))]
async fn render_worker(worker: RenderWorker) {
    while let Ok(request_id) = worker.render_rx.recv().await {
        tracing::debug!(
            %request_id,
            "render request received by worker"
        );

        process_jobtype(
            &worker.store,
            &worker.encode_tx,
            JobType::Render(request_id),
        )
        .await;
    }

    tracing::warn!("Render process finished");
}

async fn process_chunk(j: RenderJob) -> Result<CompletedChunk, RenderError> {
    let chunk_id = j.chunk.position;

    let pixels = tokio::task::spawn_blocking(move || {
        handle_chunk(j.params.index, j.params.width, j.params.height, j.chunk)
    })
    .await?;

    let chunk = CompletedChunk {
        request_id: j.request_id,
        chunk_id,
        pixels,
    };

    Ok(chunk)
}

#[derive(thiserror::Error, Debug)]
pub enum RenderError {
    #[error("JobStoreError: {0}")]
    StoreError(#[from] JobStoreError),
    #[error("Send failed {0}")]
    SendError(#[from] SendError<Uuid>),
    #[error("Join error occurred: {0}")]
    JoinError(#[from] JoinError),
}

struct EncodeJob {
    request_id: Uuid,
    params: RenderParams,
    chunks: Vec<CompletedChunk>,
    transport: tokio::sync::mpsc::Sender<Vec<u8>>,
}

impl std::fmt::Debug for EncodeJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncodeJob")
            .field("request_id", &self.request_id)
            .field("params", &self.params)
            .field("chunks", &self.chunks)
            .field("transport", &"<mpsc::Sender>")
            .finish()
    }
}

#[derive(Clone)]
struct EncodeWorker {
    worker_id: Uuid,
    store: Arc<JobStore>,
    render_rx: Receiver<Uuid>,
    encode_rx: Receiver<Uuid>,
    encode_tx: Sender<Uuid>,
}

impl EncodeWorker {
    fn new(
        store: &Arc<JobStore>,
        encode_tx: &Sender<Uuid>,
        encode_rx: &Receiver<Uuid>,
        render_rx: &Receiver<Uuid>,
    ) -> Self {
        Self {
            worker_id: Uuid::new_v4(),
            store: store.clone(),
            encode_rx: encode_rx.clone(),
            encode_tx: encode_tx.clone(),
            render_rx: render_rx.clone(),
        }
    }
}

impl EncodeWorker {
    fn start(self) -> JoinHandle<()> {
        tokio::task::spawn(encode_worker(self))
    }
}

#[instrument(skip(worker), fields(worker_id = %worker.worker_id))]
async fn encode_worker(worker: EncodeWorker) {
    loop {
        let job = match worker.encode_rx.try_recv() {
            Ok(j) => JobType::Encode(j),
            Err(TryRecvError::Empty) => match worker.render_rx.try_recv() {
                Ok(j) => JobType::Render(j),
                Err(TryRecvError::Empty) => {
                    match select! {
                        biased;
                        j = worker.encode_rx.recv() => j.map(JobType::Encode),
                        j = worker.render_rx.recv() => j.map(JobType::Render)
                    } {
                        Ok(j) => j,
                        Err(RecvError) => break,
                    }
                }
                Err(TryRecvError::Closed) => break,
            },
            Err(TryRecvError::Closed) => break,
        };

        tracing::debug!("request received by encode worker");

        process_jobtype(&worker.store, &worker.encode_tx, job).await;
    }

    tracing::warn!("Encode process finished");
}

enum JobType {
    Render(Uuid),
    Encode(Uuid),
}

impl JobType {
    pub fn request_id(&self) -> Uuid {
        match self {
            JobType::Encode(id) => *id,
            JobType::Render(id) => *id,
        }
    }
}

impl std::fmt::Debug for JobType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobType::Encode(_) => {
                if f.alternate() {
                    f.debug_tuple("Encode").field(&"..").finish()
                } else {
                    f.write_str("encode")
                }
            }
            JobType::Render(_) => {
                if f.alternate() {
                    f.debug_tuple("Render").field(&"..").finish()
                } else {
                    f.write_str("render")
                }
            }
        }
    }
}

async fn encode_and_transport(j: EncodeJob) -> Result<(), EncodeError> {
    let mut img: RgbImage = ImageBuffer::new(j.params.width, j.params.height);

    for chunk in j.chunks {
        for (x, y, pixel) in chunk.pixels {
            img.put_pixel(x, y, pixel);
        }
    }

    let channel_writer = ChannelWriter::new(j.transport);
    let capacity = *IMAGE_CHUNK_SIZE;
    let mut writer: std::io::BufWriter<ChannelWriter> =
        std::io::BufWriter::with_capacity(capacity, channel_writer);

    let mut handle = tokio::task::spawn_blocking(move || {
        let quality = *IMAGE_ENCODING_QUALITY;
        let mut encoder = Encoder::new(&mut writer, quality);

        encoder.set_progressive(true);

        let dyn_img = DynamicImage::ImageRgb8(img);
        let rgb = dyn_img.to_rgb8();

        encoder.encode(
            &rgb,
            rgb.width() as u16,
            rgb.height() as u16,
            ColorType::Rgb,
        )?;

        Ok::<(), EncodeError>(())
    });

    let warning = tokio::time::sleep(std::time::Duration::from_secs(3));
    tokio::pin!(warning);

    tokio::select! {
        biased;

        result = &mut handle => {
            result??;
        }

        _ = &mut warning => {
            tracing::warn!(
                "Encoding took longer than 3 seconds; continuing to wait for completion"
            );

            handle.await??;
        }
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum EncodeError {
    #[error("EncodingError: {0}")]
    EncodingError(#[from] jpeg_encoder::EncodingError),
    #[error("Join error occurred: {0}")]
    JoinError(#[from] JoinError),
    #[error("Encoding timed out")]
    Timeout,
}

async fn handle_request_drop(
    guard: MappedMutexGuard<'_, ImageJob>,
    request_id: Uuid,
    store: &Arc<JobStore>,
) -> Result<(), ProcessError> {
    drop(guard);
    tracing::debug!("skipping job because request was cancelled");
    store.remove(&request_id).await?;

    Ok(())
}

#[instrument(skip(store, encode_tx))]
async fn process_render_job(
    request_id: Uuid,
    store: &Arc<JobStore>,
    encode_tx: &Sender<Uuid>,
) -> Result<(), ProcessError> {
    let chunk = {
        let mut job = store.get_mut(&request_id).await?;

        if job.request_dropped() {
            handle_request_drop(job, request_id, store).await?;
            return Ok(());
        }

        job.take_chunk()?
    };

    let completed_chunk: CompletedChunk = process_chunk(chunk).await?;

    let status = {
        let mut job = store.get_mut(&request_id).await?;

        if job.request_dropped() {
            handle_request_drop(job, request_id, store).await?;
            return Ok(());
        }

        job.on_render_complete(completed_chunk)?
    };

    if status == JobStatus::RenderingComplete {
        tracing::debug!(
            %request_id,
            "sending encode request"
        );

        encode_tx
            .send(request_id)
            .await
            .map_err(|_| ProcessError::ChannelClosed)?;
    }

    Ok(())
}

#[instrument(skip(store))]
async fn process_encode_job(request_id: Uuid, store: &Arc<JobStore>) -> Result<(), ProcessError> {
    let encode_job = {
        let mut job: MappedMutexGuard<'_, ImageJob> = store.get_mut(&request_id).await?;

        if job.request_dropped() {
            handle_request_drop(job, request_id, store).await?;
            return Ok(());
        }

        job.take_encode()?
    };

    let encode_result = encode_and_transport(encode_job).await;

    {
        let job = store.get_mut(&request_id).await?;

        if job.request_dropped() {
            handle_request_drop(job, request_id, store).await?;
            return Ok(());
        }

        if let Err(err) = encode_result {
            let broken_pipe = match &err {
                EncodeError::EncodingError(EncodingError::IoError(io_error)) => {
                    io_error.kind() == ErrorKind::BrokenPipe
                }
                _ => false,
            };

            if broken_pipe {
                handle_request_drop(job, request_id, store).await?;
                return Ok(());
            }

            return Err(ProcessError::EncodeError(err));
        }
    }

    if let Err(err) = store.remove(&request_id).await {
        tracing::error!(error = %err, "Error occurred while removing job");
    }

    Ok(())
}

#[instrument(skip(store, encode_tx))]
async fn process_jobtype(store: &Arc<JobStore>, encode_tx: &Sender<Uuid>, job_type: JobType) {
    let request_id = job_type.request_id();

    let result = match job_type {
        JobType::Render(request_id) => process_render_job(request_id, store, encode_tx).await,
        JobType::Encode(request_id) => process_encode_job(request_id, store).await,
    };

    if let Err(err) = result {
        tracing::error!(error = %err, "Error occurred while processing job");

        if let Err(err) = store.remove(&request_id).await {
            tracing::error!(error = %err, "Error occurred while removing job");
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ProcessError {
    #[error("ImageJobError: {0}")]
    ImageJobError(#[from] ImageJobError),
    #[error("JobStoreError: {0}")]
    JobStoreError(#[from] JobStoreError),
    #[error("RenderError: {0}")]
    RenderError(#[from] RenderError),
    #[error("EncodeError: {0}")]
    EncodeError(#[from] EncodeError),
    #[error("Channel closed")]
    ChannelClosed,
}

pub struct Scheduler {
    store: Arc<JobStore>,
    render_tx: Sender<Uuid>,
    _encoders: Vec<tokio::task::JoinHandle<()>>,
    _renderers: Vec<tokio::task::JoinHandle<()>>,
}

impl Scheduler {
    pub fn new(encoders: usize, renderers: usize) -> Self {
        let (encode_tx, encode_rx) = async_channel::bounded(200);
        let (render_tx, render_rx) = async_channel::bounded(200);
        let store = Arc::new(JobStore::new());

        Self {
            _encoders: (0..encoders)
                .map(|_| EncodeWorker::new(&store, &encode_tx, &encode_rx, &render_rx).start())
                .collect(),
            _renderers: (0..renderers)
                .map(|_| RenderWorker::new(&store, &render_rx, &encode_tx).start())
                .collect(),
            store,
            render_tx,
        }
    }
    pub fn get_client(&self) -> SchedulerClient {
        SchedulerClient {
            store: self.store.clone(),
            render_tx: self.render_tx.clone(),
        }
    }
}

const MIN_CHUNK: u32 = 500;

#[derive(Clone)]
pub struct SchedulerClient {
    store: Arc<JobStore>,
    render_tx: Sender<Uuid>,
}

impl SchedulerClient {
    pub async fn handle_request(
        &self,
        request: ImageGenerationParams,
    ) -> Result<ImageJobStream, SchedulerError> {
        let (transport, transport_rx) = tokio::sync::mpsc::channel(64);
        let cancel_token = CancellationToken::new();
        let drop_guard = cancel_token.clone().drop_guard();

        let request_id = Uuid::new_v4();

        let chunks = vertical_chunks(
            request.height,
            request.width,
            (request.height / MIN_CHUNK).max(1),
        );

        let parts = chunks.len();
        let params = RenderParams::from(request);
        let job = ImageJob::new(request_id, chunks, params, cancel_token, transport);

        self.store.insert(request_id, job).await?;

        for _ in 0..parts {
            tracing::debug!(
                %request_id,
                "sending render request"
            );

            if let Err(_) = self.render_tx.send(request_id).await {
                self.store.remove(&request_id).await?;

                return Err(SchedulerError::ChannelClosed);
            }
        }

        Ok(ImageJobStream::new(&request_id, transport_rx, drop_guard))
    }
}

// impl From<async_channel::TrySendError<Uuid>> for SchedulerError {
//     fn from(value: async_channel::TrySendError<Uuid>) -> Self {
//         match value {
//             async_channel::TrySendError::Closed(_) => {
//                 return SchedulerError::ChannelClosed;
//             }
//             async_channel::TrySendError::Full(_) => {
//                 return SchedulerError::TooManyRequests;
//             }
//         }
//     }
// }

#[derive(thiserror::Error, Debug)]
pub enum SchedulerError {
    #[error("JobStoreError: {0}")]
    JobStoreError(#[from] JobStoreError),
    // #[error("Too many requests")]
    // TooManyRequests,
    #[error("Channel closed")]
    ChannelClosed,
}

pub struct ImageJobStream {
    pub request_id: Uuid,
    pub transport: tokio::sync::mpsc::Receiver<Vec<u8>>,
    _drop_guard: DropGuard,
}

impl ImageJobStream {
    fn new(
        request_id: &Uuid,
        transport: tokio::sync::mpsc::Receiver<Vec<u8>>,
        drop_guard: DropGuard,
    ) -> Self {
        Self {
            request_id: *request_id,
            transport,
            _drop_guard: drop_guard,
        }
    }
}

impl ImageStream for ImageJobStream {}

impl Stream for ImageJobStream {
    type Item = Vec<u8>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();

        this.transport.poll_recv(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

#[derive(Clone)]
pub struct ImageJobService {
    scheduler: SchedulerClient,
}

impl ImageJobService {
    pub fn new(scheduler: SchedulerClient) -> Self {
        Self { scheduler }
    }
}

impl ImageJobService {
    pub async fn handle_into(
        &self,
        request: ImageGenerationParams,
    ) -> Result<ImageJobStream, SchedulerError> {
        Ok(self.scheduler.handle_request(request).await?)
    }
}
