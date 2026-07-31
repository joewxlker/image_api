use tower::{Layer, ServiceBuilder};

use crate::services::image_service::{
    cache::{
        ImageCacheService, ImageCacheServiceResult, ImageMetadata, MmapImageCache,
        MmapImageCacheError,
    },
    r#gen::{ImageGenerationParams, ImageGeneratorService},
    job::{SchedulerClient, SchedulerError},
    metrics::{ImageMetrics, ImageMetricsService},
};

#[derive(Clone)]
pub struct ImageClient {
    inner: ImageMetricsService,
    cache: MmapImageCache,
}

impl ImageClient {
    pub fn new(generator: SchedulerClient, cache: MmapImageCache, metrics: ImageMetrics) -> Self {
        let inner = ServiceBuilder::new()
            .layer(ImageMetricsLayer(metrics.clone()))
            .layer(ImageCacheLayer(cache.clone()))
            .service(ImageGeneratorService::new(generator.clone()));

        Self { inner, cache }
    }

    pub async fn metadata(
        &self,
        index: u32,
        height: u32,
        width: u32,
    ) -> Result<ImageMetadata, ImageClientError> {
        let result = self
            .cache
            .read_image_metadata(index, height, width)
            .await
            .map_err(ImageClientError::ImageCacheError)?;

        Ok(result)
    }

    pub async fn image_into(
        &self,
        params: ImageGenerationParams,
        transport: tokio::sync::mpsc::Sender<Vec<u8>>,
    ) -> Result<ImageCacheServiceResult, ImageClientError> {
        self.inner.handle_into(params, transport).await
    }
}

#[derive(Clone)]
pub struct ImageMetricsLayer(pub ImageMetrics);

impl Layer<ImageCacheService> for ImageMetricsLayer {
    type Service = ImageMetricsService;

    fn layer(&self, inner: ImageCacheService) -> Self::Service {
        ImageMetricsService::new(self.0.clone(), inner)
    }
}

#[derive(Clone)]
pub struct ImageCacheLayer(pub MmapImageCache);

impl Layer<ImageGeneratorService> for ImageCacheLayer {
    type Service = ImageCacheService;

    fn layer(&self, inner: ImageGeneratorService) -> Self::Service {
        ImageCacheService::new(self.0.clone(), inner)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ImageClientError {
    #[error("Request timed out")]
    RequestTimedout,
    #[error("SchedulerError: {0}")]
    SchedulerError(#[from] SchedulerError),
    #[error("Failed to operate image cache: {0}")]
    ImageCacheError(#[source] MmapImageCacheError),
    #[error("Failed to write bytes to writer: {0}")]
    WriterError(#[source] std::io::Error),
    #[error("The request was dropped")]
    RequestDropped,
}
