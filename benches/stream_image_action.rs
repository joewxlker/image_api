#[cfg(feature = "channeled")]
use std::{
    fs::{create_dir_all, remove_dir_all},
    path::PathBuf,
    sync::atomic::{AtomicU32, Ordering},
};

#[cfg(feature = "channeled")]
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};

#[cfg(feature = "channeled")]
use platform::{
    actions::images::stream_image_action,
    services::image_service::{
        cache::MmapImageCache,
        client::ImageClient,
        r#gen::{ImageGenerationParams, ImageGenerator},
        metrics::ImageMetrics,
    },
};
#[cfg(feature = "channeled")]
use rocket::futures::{StreamExt, pin_mut};

#[cfg(feature = "channeled")]
pub fn initialize_empty_cache_dir(path: &PathBuf) {
    if path.exists() {
        remove_dir_all(path).unwrap();
    }

    create_dir_all(path).unwrap();
}

#[cfg(feature = "channeled")]
pub fn bench_handle_uncached(c: &mut Criterion) {
    let mut group = c.benchmark_group("stream_image_action");

    group
        .sample_size(10)
        .bench_function("stream_image_action_512x512", |b| {
            let runner = tokio::runtime::Runtime::new().unwrap();
            let image_cache_dir = PathBuf::from("./cache/benches/images");
            let iter_count = AtomicU32::new(0);

            initialize_empty_cache_dir(&image_cache_dir);

            b.to_async(runner).iter_batched(
                || {
                    let client = ImageClient::new(
                        ImageGenerator::new(),
                        MmapImageCache::from_path(image_cache_dir.clone()).unwrap(),
                        ImageMetrics::new(),
                    );

                    client
                },
                async |client| {
                    let index = iter_count.fetch_add(1, Ordering::Relaxed);

                    let params = ImageGenerationParams::build(index, 512, 512).unwrap();

                    let stream = stream_image_action(params, client.clone()).await;

                    let inner = stream.stream;

                    pin_mut!(inner);

                    while let Some(_) = inner.next().await {}

                    let result = stream.finished.await.unwrap().unwrap();

                    // only bench cache miss
                    assert!(!result.is_cached());
                },
                BatchSize::SmallInput,
            );
        });

    group
        .sample_size(10)
        .bench_function("stream_image_action_ttfb_512x512", |b| {
            let runner = tokio::runtime::Runtime::new().unwrap();
            let image_cache_dir = PathBuf::from("./cache/benches/images");
            let iter_count = AtomicU32::new(0);

            initialize_empty_cache_dir(&image_cache_dir);

            b.to_async(runner).iter_batched(
                || {
                    let client = ImageClient::new(
                        ImageGenerator::new(),
                        MmapImageCache::from_path(image_cache_dir.clone()).unwrap(),
                        ImageMetrics::new(),
                    );

                    client
                },
                async |client| {
                    let index = iter_count.fetch_add(1, Ordering::Relaxed);

                    let params = ImageGenerationParams::build(index, 512, 512).unwrap();

                    let stream = stream_image_action(params, client.clone()).await;

                    let inner = stream.stream;

                    pin_mut!(inner);

                    while let Some(_) = inner.next().await {
                        return;
                    }
                },
                BatchSize::SmallInput,
            );
        });

    group.finish();
}

#[cfg(feature = "channeled")]
criterion_group!(benches, bench_handle_uncached);
#[cfg(feature = "channeled")]
criterion_main!(benches);
