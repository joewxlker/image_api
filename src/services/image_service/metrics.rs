use std::time::{Duration, Instant};

use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Histogram},
};

use tracing::instrument;

use crate::services::image_service::{
    cache::{ImageCacheService, ImageCacheServiceResult, ImageKey},
    client::ImageClientError,
    r#gen::ImageGenerationParams,
};

const TIME_BOUNDARIES: &[f64] = &[
    0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 3.0, 5.0, 8.0, 10.0, 15.0, 20.0,
];

#[derive(Clone, Debug)]
pub struct ImageMetrics {
    success_counter: Counter<u64>,
    error_counter: Counter<u64>,
    success_time: Histogram<f64>,
    image_size: Histogram<f64>,
    cache_hit_time: Histogram<f64>,
    cache_miss_time: Histogram<f64>,
    error_time: Histogram<f64>,
    cache_hit_counter: Counter<u64>,
    cache_miss_counter: Counter<u64>,
}

impl ImageMetrics {
    pub fn new() -> Self {
        let meter = global::meter("image_api");

        Self {
            success_counter: meter.u64_counter("image_service.success").build(),
            error_counter: meter.u64_counter("image_service.error").build(),

            success_time: meter
                .f64_histogram("image_service.success_time")
                .with_boundaries(TIME_BOUNDARIES.into())
                .with_unit("s")
                .with_description("Time taken for successful image processing")
                .build(),

            image_size: meter
                .f64_histogram("image_service.image_size")
                .with_unit("bytes")
                .with_description("Size of generated/served image in bytes")
                .build(),

            cache_hit_time: meter
                .f64_histogram("image_service.cache_hit_time")
                .with_boundaries(TIME_BOUNDARIES.into())
                .with_unit("s")
                .build(),

            cache_miss_time: meter
                .f64_histogram("image_service.cache_miss_time")
                .with_boundaries(TIME_BOUNDARIES.into())
                .with_unit("s")
                .build(),

            error_time: meter
                .f64_histogram("image_service.error_time")
                .with_boundaries(TIME_BOUNDARIES.into())
                .with_unit("s")
                .build(),

            cache_hit_counter: meter.u64_counter("image_service.cache_hit").build(),
            cache_miss_counter: meter.u64_counter("image_service.cache_miss").build(),
        }
    }

    #[instrument(skip(result, start, self))]
    pub fn success(
        &self,
        start: &Instant,
        req: ImageGenerationParams,
        key: &ImageKey,
        result: &ImageCacheServiceResult,
    ) {
        let duration = start.elapsed().as_secs_f64();

        self.success_counter.add(1, &[]);
        self.success_time.record(duration, &[]);
        self.image_size.record(result.image_size() as f64, &[]);

        if result.is_cached() {
            self.cache_hit_time.record(duration, &[]);
            self.cache_hit_counter.add(1, &[]);

            if start.elapsed() > Duration::from_millis(500) {
                tracing::warn!(
                    duration_ms = duration,
                    threshold_ms = 500,
                    "Slow cache hit: took longer than 500 ms"
                );
            }

            tracing::debug!("Loaded image from cache in {:?}", start.elapsed());
        } else {
            self.cache_miss_time.record(duration, &[]);
            self.cache_miss_counter.add(1, &[]);

            tracing::debug!("Generated image in {:?}", start.elapsed());
        }
    }

    #[instrument(skip(err, start, self))]
    pub fn error(
        &self,
        start: &Instant,
        req: ImageGenerationParams,
        key: &ImageKey,
        err: &ImageClientError,
    ) {
        let duration = start.elapsed().as_secs_f64();

        self.error_counter.add(1, &[KeyValue::new("key", key)]);

        self.error_time.record(duration, &[]);

        tracing::error!("{err} occurred in {:?}", start.elapsed());
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

impl ImageMetricsService {
    pub async fn handle_into(
        &self,
        params: ImageGenerationParams,
        transport: tokio::sync::mpsc::Sender<Vec<u8>>,
    ) -> Result<ImageCacheServiceResult, ImageClientError> {
        let key = ImageKey::from(params);
        let start = Instant::now();

        match self.inner.handle_into(params, transport).await {
            Ok(result) => {
                self.metrics.success(&start, params, &key, &result);

                Ok(result)
            }
            Err(err) => {
                self.metrics.error(&start, params, &key, &err);

                Err(err)
            }
        }
    }
}
