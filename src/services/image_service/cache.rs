use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::ErrorKind;
use std::ops::Deref;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use mmap_sync::guard::ReadResult;
use mmap_sync::synchronizer::{Synchronizer, SynchronizerError};
use rocket::futures::StreamExt;
use tokio_stream::Stream;
use tracing::instrument;
use uuid::Uuid;

use crate::actions::images::ImageOutput;
use crate::config::IMAGE_CACHE_GRACE_DURATION;
use crate::routes::images::ImageStream;
use crate::services::image_service::client::ImageClientError;
use crate::services::image_service::r#gen::ImageGenerationParams;
use crate::services::image_service::job::ImageJobService;

#[derive(rkyv::Archive, rkyv::Deserialize, rkyv::Serialize, Debug)]
#[archive_attr(derive(bytecheck::CheckBytes))]
struct ImageCacheItem {
    key: ImageKey,
    bytes: Vec<u8>,
    height: u32,
    width: u32,
    index: u32,
}

#[derive(rkyv::Archive, rkyv::Deserialize, rkyv::Serialize, Debug)]
#[archive_attr(derive(bytecheck::CheckBytes))]
pub struct ImageKey(String);

impl Deref for ImageKey {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&ImageKey> for opentelemetry::Value {
    fn from(value: &ImageKey) -> Self {
        opentelemetry::Value::String(value.0.clone().into())
    }
}

impl From<&ImageGenerationParams> for ImageKey {
    fn from(value: &ImageGenerationParams) -> Self {
        ImageKey::from(value.clone())
    }
}

impl From<ImageGenerationParams> for ImageKey {
    fn from(value: ImageGenerationParams) -> Self {
        ImageKey::new(value.index, value.height, value.width)
    }
}

impl ImageKey {
    pub fn new(index: u32, height: u32, width: u32) -> Self {
        let mut hasher = DefaultHasher::new();

        index.hash(&mut hasher);
        height.hash(&mut hasher);
        width.hash(&mut hasher);

        Self(format!("{:#x}", hasher.finish()))
    }
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl ImageCacheItem {
    fn new(index: u32, height: u32, width: u32, bytes: &Vec<u8>) -> Self {
        let key = ImageKey::new(index, height, width);

        Self {
            key,
            bytes: bytes.clone(),
            index,
            width,
            height,
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ImageMetadata {
    id: u32,
    key: String,
    is_cached: bool,
}

impl ImageMetadata {
    pub fn new(id: u32, key: String, is_cached: bool) -> Self {
        Self { id, key, is_cached }
    }
}

#[derive(Clone)]
pub struct MmapImageCache {
    path: PathBuf,
}

impl MmapImageCache {
    pub fn from_static_path(path: &'static PathBuf) -> Result<Self, MmapImageCacheFromPathError> {
        Self::from_path(path.to_path_buf())
    }
    pub fn from_path(path: PathBuf) -> Result<Self, MmapImageCacheFromPathError> {
        if !path.exists() {
            return Err(MmapImageCacheFromPathError::InvalidPath(
                InvalidPathError::DoesNotExist(path),
            ));
        }

        if !path.is_dir() {
            return Err(MmapImageCacheFromPathError::InvalidPath(
                InvalidPathError::NotADirectory(path),
            ));
        }

        let metadata = match path.metadata() {
            Ok(meta) => meta,
            Err(source) => {
                return Err(MmapImageCacheFromPathError::ReadMetadataError(
                    ReadMetadataError { source, path },
                ));
            }
        };

        if metadata.permissions().readonly() {
            return Err(MmapImageCacheFromPathError::InvalidPath(
                InvalidPathError::ReadOnly(path),
            ));
        }

        Ok(Self { path })
    }
}

#[derive(thiserror::Error, Debug)]
pub enum InvalidPathError {
    #[error("the path `{0}` does not exist")]
    DoesNotExist(PathBuf),
    #[error("the path `{0}` is not a directory")]
    NotADirectory(PathBuf),
    #[error("the path `{0}` is read-only")]
    ReadOnly(PathBuf),
}

#[derive(thiserror::Error, Debug)]
#[error("failed to read metadata of `{path}`: {source}")]
pub struct ReadMetadataError {
    #[source]
    source: std::io::Error,
    path: PathBuf,
}

#[derive(thiserror::Error, Debug)]
pub enum MmapImageCacheFromPathError {
    #[error("Cannot create `MmapImageCache`: {0}")]
    InvalidPath(InvalidPathError),

    #[error("Cannot create `MmapImageCache`: {0}")]
    ReadMetadataError(ReadMetadataError),
}

pub struct TimedReadResult<T> {
    inner: T,
    started_at: Instant,
    grace_duration: Duration,
}

impl<T> TimedReadResult<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            started_at: Instant::now(),
            grace_duration: *IMAGE_CACHE_GRACE_DURATION,
        }
    }
}

impl<T> std::ops::Deref for TimedReadResult<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> std::ops::DerefMut for TimedReadResult<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T> Drop for TimedReadResult<T> {
    fn drop(&mut self) {
        let elapsed = self.started_at.elapsed();
        if elapsed > self.grace_duration {
            tracing::warn!(
                elapsed_ms = elapsed.as_millis(),
                grace_ms = self.grace_duration.as_millis(),
                "ReadResult was held longer than IMAGE_CACHE_GRACE_DURATION"
            );
        }
    }
}

impl MmapImageCache {
    pub async fn read_image_bytes(
        &self,
        index: u32,
        height: u32,
        width: u32,
    ) -> Result<Option<Vec<u8>>, MmapImageCacheError> {
        let key = ImageKey::new(index, height, width);
        let path_prefix = self.path.join(key.as_str());
        let mut synchronizer = Synchronizer::new(path_prefix.as_os_str());

        let archive = match self.read_archived_image(&mut synchronizer, &key).await? {
            Some(r) => r,
            None => return Ok(None),
        };

        let mut bytes = vec![];
        archive.bytes.clone_into(&mut bytes);

        Ok(Some(bytes))
    }
    pub async fn read_image_metadata(
        &self,
        index: u32,
        height: u32,
        width: u32,
    ) -> Result<ImageMetadata, MmapImageCacheError> {
        let key = ImageKey::new(index, height, width);
        let path_prefix = self.path.join(key.as_str());
        let mut synchronizer = Synchronizer::new(path_prefix.as_os_str());

        match self.read_archived_image(&mut synchronizer, &key).await? {
            Some(_) => Ok(ImageMetadata::new(index, key.into_inner(), true)),
            None => Ok(ImageMetadata::new(index, key.into_inner(), false)),
        }
    }
    async fn read_archived_image<'a>(
        &'a self,
        synchronizer: &'a mut Synchronizer,
        key: &ImageKey,
    ) -> Result<Option<TimedReadResult<ReadResult<'a, ImageCacheItem>>>, MmapImageCacheError> {
        let v = synchronizer.version();
        let read = match unsafe { synchronizer.read::<ImageCacheItem>(false) } {
            Ok(read) => {
                tracing::trace!(
                    key = key.as_str(),
                    is_switched = format!("{}", read.is_switched()),
                    version = format!(
                        "{:?}",
                        v.map(|v| format!("{:?}", v))
                            .unwrap_or_else(|_| "NOT_FOUND".to_string())
                    )
                );

                TimedReadResult::new(read)
            }
            Err(err) => match &err {
                SynchronizerError::FailedDataRead(io) | SynchronizerError::FailedStateRead(io) => {
                    match io.kind() {
                        ErrorKind::NotFound => return Ok(None),
                        _ => return Err(err.into()),
                    }
                }
                _ => return Err(err.into()),
            },
        };

        Ok(Some(read))
    }
    pub async fn store_image_bytes(
        &self,
        params: ImageGenerationParams,
        bytes: &Vec<u8>,
    ) -> Result<(), MmapImageCacheError> {
        let key = ImageKey::new(params.index, params.height, params.width);
        let mut synchronizer = Synchronizer::new(self.path.join(key.as_str()).as_os_str());
        let item = ImageCacheItem::new(params.index, params.height, params.width, bytes);
        let grace_duration = *IMAGE_CACHE_GRACE_DURATION;
        synchronizer.write::<ImageCacheItem>(&item, grace_duration)?;

        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum MmapImageCacheError {
    #[error("Unhandled IO Error: {0}")]
    UnhandledIoError(#[from] std::io::Error),
    #[error("SynchronizerError: {0}")]
    SynchronizerError(#[from] SynchronizerError),
}

#[derive(Clone)]
pub struct ImageCacheService {
    cache: MmapImageCache,
    inner: ImageJobService,
}

impl ImageCacheService {
    pub fn new(cache: MmapImageCache, inner: ImageJobService) -> Self {
        Self { cache, inner }
    }
}

impl ImageCacheService {
    #[tracing::instrument(skip(self))]
    pub async fn handle_into(
        &self,
        params: ImageGenerationParams,
    ) -> Result<ImageOutput<impl ImageStream + use<>>, ImageClientError> {
        if !params.bypass_cache_read {
            if let Some(image_bytes) = self
                .cache
                .read_image_bytes(params.index, params.height, params.width)
                .await
                .map_err(ImageClientError::ImageCacheError)?
            {
                return Ok(ImageOutput::Bytes(image_bytes));
            }
        }

        let cache = self.cache.clone();
        let stream = self.inner.handle_into(params).await?;
        let request_id = stream.request_id;

        Ok(ImageOutput::Stream(Box::new(
            ImageCacheStream::new(stream, cache, request_id, params),
        )))
    }
}

pub struct ImageCacheStream<S: ImageStream> {
    pub params: ImageGenerationParams,
    pub request_id: Uuid,
    inner: S,
    image: Vec<u8>,
    cache: MmapImageCache,
}

impl<S: ImageStream> ImageStream for ImageCacheStream<S> {}

impl<S: ImageStream> ImageCacheStream<S> {
    pub fn new(
        inner: S,
        cache: MmapImageCache,
        request_id: Uuid,
        params: ImageGenerationParams,
    ) -> Self {
        Self {
            request_id,
            cache,
            params,
            image: Vec::new(),
            inner,
        }
    }
}

impl<S: ImageStream> Stream for ImageCacheStream<S> {
    type Item = S::Item;

    #[instrument(skip(self, cx))]
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;

        let this = self.get_mut();

        match this.inner.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(bytes)) => {
                this.image.extend_from_slice(bytes.as_ref());

                Poll::Ready(Some(bytes))
            }
            Poll::Ready(None) => {
                tracing::debug!(
                    request_id = %this.request_id,
                    "Image stream finished; spawning background cache write"
                );

                tokio::spawn(handle_cache_store(
                    this.request_id,
                    this.params,
                    std::mem::take(&mut this.image),
                    this.cache.clone(),
                ));

                Poll::Ready(None)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

#[instrument(skip(params, image, cache))]
async fn handle_cache_store(
    request_id: Uuid,
    params: ImageGenerationParams,
    image: Vec<u8>,
    cache: MmapImageCache,
) {
    tracing::debug!("cache write started");

    if let Err(err) = cache.store_image_bytes(params, &image).await {
        tracing::error!("cache write failed: {err}");
        return;
    }

    tracing::debug!("cache write completed");
}
