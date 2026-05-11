use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::ErrorKind;
use std::ops::Deref;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use mmap_sync::guard::ReadResult;
use mmap_sync::synchronizer::{Synchronizer, SynchronizerError};

#[cfg(feature = "channeled")]
use {crate::util::tee_writer::non_blocking::TeeWriter, rocket::futures::AsyncWrite};

use crate::services::image_service::client::ImageClientError;
use crate::services::image_service::r#gen::{ImageGenerationParams, ImageGeneratorService};

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
            return Err(MmapImageCacheFromPathError::PathInvalid {
                path,
                kind: PathInvalidKind::DoesNotExist,
            });
        }

        if !path.is_dir() {
            return Err(MmapImageCacheFromPathError::PathInvalid {
                path,
                kind: PathInvalidKind::NotADirectory,
            });
        }

        let metadata = match path.metadata() {
            Ok(meta) => meta,
            Err(source) => {
                return Err(MmapImageCacheFromPathError::ReadMetadataError { source, path });
            }
        };

        if metadata.permissions().readonly() {
            return Err(MmapImageCacheFromPathError::PathInvalid {
                path,
                kind: PathInvalidKind::ReadOnly,
            });
        }

        Ok(Self { path })
    }
}

#[derive(thiserror::Error, Debug)]
pub enum PathInvalidKind {
    #[error("does not exist")]
    DoesNotExist,
    #[error("is not a directory")]
    NotADirectory,
    #[error("is read-only")]
    ReadOnly,
}

#[derive(thiserror::Error, Debug)]
pub enum MmapImageCacheFromPathError {
    #[error("Cannot create `MmapImageCache`: the path `{path}` {kind}")]
    PathInvalid {
        path: PathBuf,
        kind: PathInvalidKind,
    },

    #[error("Cannot create MmapImageCache: failed to read metadata of `{path}`: {source}")]
    ReadMetadataError {
        #[source]
        source: std::io::Error,
        path: PathBuf,
    },
}

const GRACE_PERIOD: Duration = Duration::from_millis(10);

pub struct TimedReadResult<T> {
    inner: T,
    started_at: Instant,
    grace_period: Duration,
}

impl<T> TimedReadResult<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            started_at: Instant::now(),
            grace_period: GRACE_PERIOD,
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
        index: u32,
        height: u32,
        width: u32,
        bytes: &Vec<u8>,
    ) -> Result<(), MmapImageCacheError> {
        let key = ImageKey::new(index, height, width);
        let mut synchronizer = Synchronizer::new(self.path.join(key.as_str()).as_os_str());
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
    SynchronizerError(#[from] SynchronizerError),
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

impl ImageCacheService {
    #[cfg(not(feature = "channeled"))]
    pub async fn handle(
        &self,
        params: ImageGenerationParams,
    ) -> Result<ImageCacheServiceResult, ImageClientError> {
        let read_result = self
            .cache
            .read_image_bytes(params.index, params.height, params.width)
            .await
            .map_err(ImageClientError::ImageCacheError)?;

        if let Some(image_bytes) = read_result {
            return Ok(ImageCacheServiceResult::Cached(image_bytes));
        }

        let image_bytes = self.inner.handle(params).await?;

        let bytes = image_bytes.clone();
        let cache = self.cache.clone();
        tokio::task::spawn(async move {
            if let Err(err) = cache
                .store_image_bytes(params.index, params.height, params.width, &bytes)
                .await
            {
                log::error!("Error occured while writing image to disk: {err}");
            }
        });

        Ok(ImageCacheServiceResult::Generated(image_bytes))
    }

    #[cfg(feature = "channeled")]
    pub async fn handle_into<W>(
        &self,
        params: ImageGenerationParams,
        mut writer: W,
    ) -> Result<ImageCacheServiceResult, ImageClientError>
    where
        W: AsyncWrite + Unpin,
    {
        if let Some(image_bytes) = self
            .cache
            .read_image_bytes(params.index, params.height, params.width)
            .await
            .map_err(ImageClientError::ImageCacheError)?
        {
            use rocket::futures::AsyncWriteExt;

            writer
                .write_all(&image_bytes)
                .await
                .map_err(ImageClientError::WriterError)?;

            return Ok(ImageCacheServiceResult::Cached(image_bytes));
        }

        let mut cache = vec![];
        {
            let mut t_writer = TeeWriter::new(writer, &mut cache);

            self.inner.handle_into(params, &mut t_writer).await?;

            // Dropping the writer here closes the stream
        }

        let size = cache.len();

        let image_cache = self.cache.clone();
        tokio::task::spawn(async move {
            if let Err(err) = image_cache
                .store_image_bytes(params.index, params.height, params.width, &cache)
                .await
            {
                tracing::error!("Error occurred while writing image to disk: {err}");
            }
        });

        Ok(ImageCacheServiceResult::Streamed(size))
    }
}

#[derive(Debug)]
pub enum ImageCacheServiceResult {
    Cached(Vec<u8>),
    Generated(Vec<u8>),

    #[cfg(feature = "channeled")]
    Streamed(usize),
}

impl ImageCacheServiceResult {
    pub fn is_cached(&self) -> bool {
        matches!(self, Self::Cached(_))
    }
    #[cfg(not(feature = "channeled"))]
    pub fn bytes_owned(self) -> Vec<u8> {
        match self {
            Self::Cached(bytes) | Self::Generated(bytes) => bytes,
        }
    }
    pub fn image_size(&self) -> u32 {
        match self {
            Self::Cached(bytes) | Self::Generated(bytes) => bytes.len() as u32,
            #[cfg(feature = "channeled")]
            Self::Streamed(size) => *size as u32,
        }
    }
}
