use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::ErrorKind;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::Poll;
use std::time::{Duration, Instant};

use mmap_sync::guard::ReadResult;
use mmap_sync::synchronizer::{Synchronizer, SynchronizerError};
use tower::Service;

use crate::services::image_service::client::ImageClientError;
use crate::services::image_service::r#gen::{ImageGenerationParams, ImageGeneratorService};

#[derive(rkyv::Archive, rkyv::Deserialize, rkyv::Serialize, Debug)]
#[archive_attr(derive(bytecheck::CheckBytes))]
struct ImageCacheItem {
    key: String,
    bytes: Vec<u8>,
    height: u32,
    width: u32,
    index: u32,
}

pub fn image_key(index: u32, height: u32, width: u32) -> String {
    let mut hasher = DefaultHasher::new();

    index.hash(&mut hasher);
    height.hash(&mut hasher);
    width.hash(&mut hasher);

    format!("{:#x}", hasher.finish())
}

impl ImageCacheItem {
    fn new(index: u32, height: u32, width: u32, bytes: &Vec<u8>) -> Self {
        let key = image_key(index, height, width);

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
    pub fn new(path: &str) -> Self {
        Self {
            path: PathBuf::from(path),
        }
    }
}

const GRACE_PERIOD: Duration = Duration::from_millis(10);

pub struct TimedReadResult<T> {
    inner: T,
    started_at: Instant,
    grace_period: Duration,
}

impl <T> TimedReadResult<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            started_at: Instant::now(),
            grace_period: GRACE_PERIOD
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
        if elapsed > self.grace_period {
            tracing::warn!(
                elapsed_ms = elapsed.as_millis(),
                grace_ms = self.grace_period.as_millis(),
                "ReadResult was held longer than GRACE_PERIOD"
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
        let key = image_key(index, height, width);
        let path_prefix = self.path.join(&key);
        let mut synchronizer = Synchronizer::new(path_prefix.as_os_str());

        let archive = match self.read_archived_image(&mut synchronizer, &key).await? {
            Some(r) => r,
            None => return Ok(None)
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
        let key = image_key(index, height, width);
        let path_prefix = self.path.join(&key);
        let mut synchronizer = Synchronizer::new(path_prefix.as_os_str());

        match self.read_archived_image(&mut synchronizer, &key).await? {
            Some(_) => Ok(ImageMetadata::new(index, key, true)),
            None => Ok(ImageMetadata::new(index, key, false)),
        }
    }
    async fn read_archived_image<'a>(
        &'a self,
        synchronizer: &'a mut Synchronizer,
        key: &String,
    ) -> Result<Option<TimedReadResult<ReadResult<'a, ImageCacheItem>>>, MmapImageCacheError> {
        let v = synchronizer.version();
        let read = match unsafe { synchronizer.read::<ImageCacheItem>(false) } {
            Ok(read) => {
                tracing::trace!(
                    key = key,
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
        index: u32,
        height: u32,
        width: u32,
        bytes: &Vec<u8>,
    ) -> Result<(), MmapImageCacheError> {
        let key = image_key(index, height, width);
        let mut synchronizer = Synchronizer::new(self.path.join(key).as_os_str());
        let item = ImageCacheItem::new(index, height, width, bytes);

        synchronizer.write::<ImageCacheItem>(&item, GRACE_PERIOD)?;

        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum MmapImageCacheError {
    #[error("Unhandled IO Error: {0}")]
    UnhandledIoError(#[from] std::io::Error),
    #[error("SynchronizerError: {0}")]
    SynchronizerError(#[from] SynchronizerError)
}

#[derive(Clone)]
pub struct ImageCacheService {
    cache: MmapImageCache,
    inner: ImageGeneratorService,
}

impl ImageCacheService {
    pub fn new(cache: MmapImageCache, inner: ImageGeneratorService) -> Self {
        Self { cache, inner }
    }
}

#[derive(Debug)]
pub enum ImageCacheServiceResult {
    Cached(Vec<u8>),
    Generated(Vec<u8>),
}

impl ImageCacheServiceResult {
    pub fn is_cached(&self) -> bool {
        match self {
            Self::Cached(_) => return true,
            _ => return false,
        }
    }
    pub fn bytes_owned(self) -> Vec<u8> {
        match self {
            Self::Cached(bytes) => bytes,
            Self::Generated(bytes) => bytes,
        }
    }
}

impl Service<ImageGenerationParams> for ImageCacheService {
    type Error = ImageClientError;
    type Response = ImageCacheServiceResult;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&mut self, req: ImageGenerationParams) -> Self::Future {
        let cache = self.cache.clone();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let read_result = cache
                .read_image_bytes(req.index, req.height, req.width)
                .await
                .map_err(ImageClientError::ImageCacheError)?;

            if let Some(image_bytes) = read_result {
                return Ok(ImageCacheServiceResult::Cached(image_bytes));
            }

            let image_bytes = inner.call(req).await?;

            let bytes = image_bytes.clone();
            tokio::task::spawn(async move {
                if let Err(err) = cache
                    .store_image_bytes(req.index, req.height, req.width, &bytes)
                    .await
                {
                    log::error!("Error occured while writing image to disk: {err}");
                }
            });

            Ok(ImageCacheServiceResult::Generated(image_bytes))
        })
    }
    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}
