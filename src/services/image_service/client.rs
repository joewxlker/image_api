#[cfg(feature = "channeled")]
use rocket::futures::AsyncWrite;

use tower::{Layer, ServiceBuilder};

use crate::services::image_service::{
    cache::{
        ImageCacheService, ImageCacheServiceResult, ImageMetadata, MmapImageCache,
        MmapImageCacheError,
    },
    r#gen::{ImageGenerationParams, ImageGenerator, ImageGeneratorError, ImageGeneratorService},
    metrics::{ImageMetrics, ImageMetricsService},
};

#[derive(Clone)]
pub struct ImageClient {
    inner: ImageMetricsService,
    cache: MmapImageCache,
}

impl ImageClient {
    pub fn new(generator: ImageGenerator, cache: MmapImageCache, metrics: ImageMetrics) -> Self {
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

    #[cfg(not(feature = "channeled"))]
    pub async fn image(
        &mut self,
        params: ImageGenerationParams,
    ) -> Result<ImageCacheServiceResult, ImageClientError> {
        self.inner.handle(params).await
    }

    #[cfg(feature = "channeled")]
    pub async fn image_into<W>(
        &self,
        params: ImageGenerationParams,
        writer: &mut W,
    ) -> Result<ImageCacheServiceResult, ImageClientError>
    where
        W: AsyncWrite + Unpin,
    {
        self.inner.handle_into(params, writer).await
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
    #[error("Failed to generate image: {0}")]
    ImageGenError(#[source] ImageGeneratorError),
    #[error("Failed to operate image cache: {0}")]
    ImageCacheError(#[source] MmapImageCacheError),
    #[cfg(feature = "channeled")]
    #[error("Failed to write bytes to writer: {0}")]
    WriterError(#[source] std::io::Error),
}
