use std::{
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Histogram},
};
use rocket::futures::StreamExt;
use tokio_stream::Stream;
use tracing::instrument;

use crate::{
    actions::images::ImageOutput, routes::images::ImageStream, services::image_service::{
        cache::{ImageCacheService, ImageKey},
        client::ImageClientError,
        r#gen::ImageGenerationParams,
    },
};

const TIME_BOUNDARIES: &[f64] = &[
    0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 3.0, 5.0, 8.0, 10.0, 15.0, 20.0,
];

#[derive(Clone, Debug)]
pub struct ImageMetrics {
    success_counter: Counter<u64>,
    error_counter: Counter<u64>,

    success_time: Histogram<f64>,
    error_time: Histogram<f64>,
    image_size: Histogram<f64>,

    cache_hit_time: Histogram<f64>,
    cache_miss_time: Histogram<f64>,
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
                .with_description("Time taken to fully stream an image")
                .build(),

            error_time: meter
                .f64_histogram("image_service.error_time")
                .with_boundaries(TIME_BOUNDARIES.into())
                .with_unit("s")
                .build(),

            image_size: meter
                .f64_histogram("image_service.image_size")
                .with_unit("By")
                .with_description("Size of generated or served image")
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

            cache_hit_counter: meter.u64_counter("image_service.cache_hit").build(),

            cache_miss_counter: meter.u64_counter("image_service.cache_miss").build(),
        }
    }

    #[instrument(skip(self, start, result))]
    fn resolved(
        &self,
        start: &Instant,
        req: ImageGenerationParams,
        key: &ImageKey,
        result: &ImageOutput<impl ImageStream>,
    ) {
        let elapsed = start.elapsed();
        let duration = elapsed.as_secs_f64();

        if result.is_cached() {
            self.cache_hit_counter.add(1, &[]);
            self.cache_hit_time.record(duration, &[]);

            if elapsed > Duration::from_millis(500) {
                tracing::warn!(
                    duration_ms = elapsed.as_secs_f64() * 1_000.0,
                    threshold_ms = 500,
                    "Slow cache hit"
                );
            }

            tracing::debug!(?elapsed, "Image loaded from cache");
        } else {
            self.cache_miss_counter.add(1, &[]);
            self.cache_miss_time.record(duration, &[]);

            tracing::debug!(?elapsed, "Image generation started");
        }
    }

    #[instrument(skip(self, start, err))]
    fn error(
        &self,
        start: &Instant,
        req: ImageGenerationParams,
        key: &ImageKey,
        err: &ImageClientError,
    ) {
        let elapsed = start.elapsed();

        self.error_counter
            .add(1, &[KeyValue::new("key", key.to_string())]);

        self.error_time.record(elapsed.as_secs_f64(), &[]);

        tracing::error!(?elapsed, %err, "Image request failed");
    }

    fn stream_completed(&self, started_at: Instant, image_size: usize) {
        self.success_counter.add(1, &[]);
        self.success_time
            .record(started_at.elapsed().as_secs_f64(), &[]);
        self.image_size.record(image_size as f64, &[]);
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

    pub async fn handle_into(
        &self,
        params: ImageGenerationParams,
    ) -> Result<ImageOutput<impl ImageStream + use<>>, ImageClientError> {
        let key = ImageKey::from(params);
        let started_at = Instant::now();

        match self.inner.handle_into(params).await {
            Ok(result) => {
                self.metrics.resolved(&started_at, params, &key, &result);

                Ok(result.map_stream(|inner| {
                    MetricsStream::new(inner, self.metrics.clone(), started_at)
                }))
            }

            Err(err) => {
                self.metrics.error(&started_at, params, &key, &err);
                Err(err)
            }
        }
    }
}

struct MetricsStream<S> {
    inner: S,
    metrics: ImageMetrics,
    started_at: Instant,
    image_size: usize,
    completed: bool,
}

impl<S> MetricsStream<S> {
    fn new(inner: S, metrics: ImageMetrics, started_at: Instant) -> Self {
        Self {
            inner,
            metrics,
            started_at,
            image_size: 0,
            completed: false,
        }
    }
}

impl<S: ImageStream> ImageStream for MetricsStream<S> {}

impl<S: ImageStream> Stream for MetricsStream<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        match this.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(bytes)) => {
                this.image_size += bytes.as_ref().len();
                Poll::Ready(Some(bytes))
            }

            Poll::Ready(None) => {
                if !this.completed {
                    this.completed = true;
                    this.metrics
                        .stream_completed(this.started_at, this.image_size);
                }

                Poll::Ready(None)
            }

            Poll::Pending => Poll::Pending,
        }
    }
}
