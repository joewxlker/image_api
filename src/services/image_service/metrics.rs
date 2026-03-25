use std::{pin::Pin, task::Poll, time::Instant};

use tower::Service;
use tracing::instrument;

use crate::services::image_service::{
    cache::{ImageCacheService, ImageCacheServiceResult, image_key},
    client::ImageClientError,
    r#gen::ImageGenerationParams,
};

#[derive(Clone, Debug)]
pub struct ImageMetrics;

impl ImageMetrics {
    #[instrument(skip(result, start, self))]
    fn success(
        &self,
        start: &Instant,
        req: ImageGenerationParams,
        key: &str,
        result: &ImageCacheServiceResult,
    ) {
        if result.is_cached() {
            tracing::debug!("Loaded image from cache in {:?}", start.elapsed());
        } else {
            tracing::debug!("Generated image in {:?}", start.elapsed());
        }
    }
    #[instrument(skip(err, start, self))]
    fn error(
        &self,
        start: &Instant,
        req: ImageGenerationParams,
        key: &str,
        err: &ImageClientError,
    ) {
        tracing::error!("{err} occured in {:?}", start.elapsed());
    }
}

#[derive(Clone)]
pub struct ImageMetricsService {
    metrics: ImageMetrics,
    inner: ImageCacheService,
}

impl ImageMetricsService {
    pub fn new(metrics: ImageMetrics, inner: ImageCacheService) -> Self {
        Self { metrics, inner }
    }
}

impl Service<ImageGenerationParams> for ImageMetricsService {
    type Error = ImageClientError;
    type Response = ImageCacheServiceResult;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&mut self, req: ImageGenerationParams) -> Self::Future {
        let mut image_cache = self.inner.clone();
        let metrics = self.metrics.clone();
        let key = image_key(req.index, req.height, req.width);
        let start = Instant::now();

        Box::pin(async move {
            match image_cache.call(req).await {
                Ok(result) => {
                    metrics.success(&start, req, &key, &result);

                    Ok(result)
                }
                Err(err) => {
                    metrics.error(&start, req, &key, &err);

                    Err(err)
                }
            }
        })
    }
    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}
