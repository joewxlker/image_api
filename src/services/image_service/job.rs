use std::{collections::HashMap, sync::Arc};

use async_channel::{Receiver, RecvError, SendError, Sender, TryRecvError};
use image::{DynamicImage, ImageBuffer, Rgb, RgbImage};
use jpeg_encoder::{ColorType, Encoder};

use tokio::{
    select,
    sync::{MappedMutexGuard, Mutex, MutexGuard},
    task::{JoinError, JoinHandle},
};
use tracing::instrument;
use uuid::Uuid;

use crate::{
    config::{IMAGE_CHUNK_SIZE, IMAGE_ENCODING_QUALITY},
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

#[derive(PartialEq, Copy, Clone, Debug)]
pub enum JobStatus {
    Idle,
    Rendering,
    RenderingComplete,
    Encoding,
    Failed,
    Cancelled,
}

struct ImageJob {
    request_id: Uuid,
    status: JobStatus,
    params: RenderParams,
    required_chunks: usize,
    pending_chunks: Vec<Chunk>,
    completed_chunks: Option<Vec<CompletedChunk>>,
    transport: tokio::sync::mpsc::Sender<Vec<u8>>,
}

impl std::fmt::Debug for ImageJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImageJob")
            .field("request_id", &self.request_id)
            .field("status", &self.status)
            .field("params", &self.params)
            .field("required_chunks", &self.required_chunks)
            .field("pending_chunks", &self.pending_chunks.len())
            .field(
                "completed_chunks",
                &self.completed_chunks.as_ref().map(Vec::len),
            )
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
        transport: tokio::sync::mpsc::Sender<Vec<u8>>,
    ) -> Self {
        let required_chunks = chunks.len();
        Self {
            request_id: request_id,
            status: JobStatus::Idle,
            required_chunks,
            params,
            pending_chunks: chunks,
            completed_chunks: Some(Vec::new()),
            transport,
        }
    }
    #[instrument(
        skip(self),
        fields(request_id = %self.request_id, initial_status = ?self.status)
    )]
    pub fn take_chunk(&mut self) -> Result<RenderJob, ImageJobError> {
        if self.status == JobStatus::Idle {
            tracing::debug!(
                from = ?self.status,
                to = ?JobStatus::Rendering,
                "job state transition"
            );
            self.status = JobStatus::Rendering;
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
            completed = self.completed_chunks.as_ref().map(Vec::len).unwrap_or(0),
            remaining = self.pending_chunks.len(),
            "render chunk completed"
        );

        Ok(RenderJob {
            request_id: self.request_id,
            params: self.params,
            chunk,
        })
    }
    #[instrument(
        skip(self),
        fields(request_id = %self.request_id, initial_status = ?self.status)
    )]
    pub fn take_encode(&mut self) -> Result<EncodeJob, ImageJobError> {
        let expected = JobStatus::RenderingComplete;
        if self.status != expected {
            return Err(ImageJobError::InvalidState(expected, self.status));
        }

        tracing::debug!(
            from = ?self.status,
            to = ?JobStatus::Encoding,
            "job state transition"
        );
        self.status = JobStatus::Encoding;

        tracing::debug!("encode job created");

        Ok(EncodeJob {
            request_id: self.request_id,
            params: self.params,
            chunks: self.completed_chunks.take().unwrap(),
            transport: self.transport.clone(),
        })
    }
    #[instrument(
        skip(self),
        fields(request_id = %self.request_id, initial_status = ?self.status)
    )]
    pub fn on_render_complete(
        &mut self,
        chunk: CompletedChunk,
    ) -> Result<JobStatus, ImageJobError> {
        let expected = JobStatus::Rendering;
        if self.status != expected {
            return Err(ImageJobError::InvalidState(expected, self.status));
        }

        tracing::debug!(
            completed = self.completed_chunks.as_ref().map(Vec::len).unwrap_or(0),
            required = self.required_chunks,
            "render chunk completed"
        );

        if let Some(chunks) = &mut self.completed_chunks {
            chunks.push(chunk);

            if chunks.len() == self.required_chunks {
                tracing::debug!(
                    from = ?self.status,
                    to = ?JobStatus::RenderingComplete,
                    "job state transition"
                );
                self.status = JobStatus::RenderingComplete;
            }
        }

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
    #[instrument(skip(self, job))]
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
    #[instrument(skip(self))]
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
    #[instrument(skip(self))]
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
    #[error("invalid job state: expected {0:?}, got {1:?}")]
    InvalidJobState(JobStatus, JobStatus),
    #[error("rendering job requested but no pending chunks remain")]
    PendingChunkNotFound,
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
        tokio::task::spawn(async move {
            while let Ok(request_id) = self.render_rx.recv().await {
                tracing::debug!(
                    worker_id = ?self.worker_id,
                    %request_id,
                    "render request received by worker"
                );

                run_process(
                    &self.worker_id,
                    &self.store,
                    &self.encode_tx,
                    JobType::Render(request_id),
                )
                .await;
            }

            tracing::warn!(
                worker_id = ?self.worker_id,
                "Render process finished"
            );
        })
    }
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
    #[error("Rendering timed out")]
    Timeout,
    #[error("Job was aborted")]
    JobAborted,
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
    #[instrument(skip(self))]
    fn start(self) -> JoinHandle<()> {
        tokio::task::spawn(async move {
            loop {
                let job = match self.encode_rx.try_recv() {
                    Ok(j) => JobType::Encode(j),
                    Err(TryRecvError::Empty) => match self.render_rx.try_recv() {
                        Ok(j) => JobType::Render(j),
                        Err(TryRecvError::Empty) => {
                            match select! {
                                biased;
                                j = self.encode_rx.recv() => j.map(JobType::Encode),
                                j = self.render_rx.recv() => j.map(JobType::Render)
                            } {
                                Ok(j) => j,
                                Err(RecvError) => break,
                            }
                        }
                        Err(TryRecvError::Closed) => break,
                    },
                    Err(TryRecvError::Closed) => break,
                };

                let request_id = job.request_id();

                tracing::debug!(
                    worker_id = ?self.worker_id,
                    %request_id,
                    job_type = format!("{:?}", job),
                    "request received by encode worker"
                );

                run_process(&self.worker_id, &self.store, &self.encode_tx, job).await;
            }

            tracing::warn!(
                worker_id = ?self.worker_id,
                "Encode process finished"
            );
        })
    }
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

#[instrument]
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

    tokio::task::spawn_blocking(move || {
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
    })
    .await??;

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
    #[error("Job aborted")]
    JobAborted,
}

#[instrument(skip(store, encode_tx))]
async fn process(
    worker_id: &Uuid,
    store: &Arc<JobStore>,
    encode_tx: &Sender<Uuid>,
    job_type: JobType,
) -> Result<(), SchedulerError> {
    match job_type {
        JobType::Render(request_id) => {
            let chunk = {
                let mut job = store.get_mut(&request_id).await?;
                job.take_chunk()?
            };

            let completed_chunk = process_chunk(chunk).await?;

            let status = {
                let mut job = store.get_mut(&request_id).await?;
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
                    .map_err(|_| SchedulerError::ChannelClosed)?;
            }
        }
        JobType::Encode(request_id) => {
            let encode_job = {
                let mut job = store.get_mut(&request_id).await?;

                job.take_encode()?
            };

            encode_and_transport(encode_job).await?;

            store.remove(&request_id).await?;
        }
    };

    Ok(())
}

#[instrument(skip(store, encode_tx), fields(request_id = %job_type.request_id()))]
async fn run_process(
    worker_id: &Uuid,
    store: &Arc<JobStore>,
    encode_tx: &Sender<Uuid>,
    job_type: JobType,
) {
    let request_id = job_type.request_id();

    if let Err(err) = process(worker_id, &store, encode_tx, job_type).await {
        tracing::error!("Error occured while processing job: {err}");

        match store.get_mut(&request_id).await {
            Ok(mut j) => j.status = JobStatus::Failed,
            Err(err) => {
                tracing::error!("Failed to load job: {err}");

                return;
            }
        };
    }
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
                .into_iter()
                .map(|_| EncodeWorker::new(&store, &encode_tx, &encode_rx, &render_rx).start())
                .collect(),
            _renderers: (0..renderers)
                .into_iter()
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
        transport: tokio::sync::mpsc::Sender<Vec<u8>>,
    ) -> Result<(), SchedulerError> {
        let request_id = Uuid::new_v4();

        let chunks = vertical_chunks(
            request.height,
            request.width,
            (request.height / MIN_CHUNK).max(1),
        );

        let parts = chunks.len();
        let params = RenderParams::from(request);
        let job = ImageJob::new(request_id, chunks, params, transport);

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

        Ok(())
    }
}

impl From<async_channel::TrySendError<Uuid>> for SchedulerError {
    fn from(value: async_channel::TrySendError<Uuid>) -> Self {
        match value {
            async_channel::TrySendError::Closed(_) => {
                return SchedulerError::ChannelClosed;
            }
            async_channel::TrySendError::Full(_) => {
                return SchedulerError::TooManyRequests;
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum SchedulerError {
    #[error("ImageJobError: {0}")]
    ImageJobError(#[from] ImageJobError),
    #[error("JobStoreError: {0}")]
    JobStoreError(#[from] JobStoreError),
    #[error("RenderError: {0}")]
    RenderError(#[from] RenderError),
    #[error("EncodeError: {0}")]
    EncodeError(#[from] EncodeError),
    #[error("Too many requests")]
    TooManyRequests,
    #[error("Channel closed")]
    ChannelClosed,
}
