#[cfg(feature = "buffered")]
use std::{
    fs::{create_dir_all, remove_dir_all},
    path::PathBuf,
    sync::atomic::{AtomicU32, Ordering},
};

#[cfg(feature = "buffered")]
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
#[cfg(feature = "buffered")]
use platform::services::image_service::{
    cache::MmapImageCache,
    client::ImageClient,
    r#gen::{ImageGenerationParams, ImageGenerator},
    metrics::ImageMetrics,
};

#[cfg(feature = "buffered")]
pub fn initialize_empty_cache_dir(path: &PathBuf) {
    if path.exists() {
        remove_dir_all(path).unwrap();
    }

    create_dir_all(path).unwrap();
}

#[cfg(feature = "buffered")]
pub fn bench_handle_uncached(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_image_action");

    group
        .sample_size(10)
        .bench_function("get_image_action", |b| {
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

                    let result = client.image(params).await.unwrap();

                    // only bench cache miss
                    assert!(!result.is_cached());
                },
                BatchSize::SmallInput,
            );
        });

    group.finish();
}

#[cfg(feature = "buffered")]
criterion_group!(benches, bench_handle_uncached);
#[cfg(feature = "buffered")]
criterion_main!(benches);
