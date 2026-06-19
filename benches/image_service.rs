use std::{
    fs::{create_dir_all, remove_dir_all},
    path::PathBuf,
};

use criterion::{Bencher, Criterion, criterion_group, criterion_main, measurement::WallTime};

use criterion::BatchSize;

use platform::services::image_service::{
    cache::{ImageCacheServiceResult, MmapImageCache},
    client::ImageClient,
    r#gen::{ImageGenerationParams, ImageGenerator},
    metrics::ImageMetrics,
};

use pprof::criterion::{Output, PProfProfiler};

pub fn initialize_empty_cache_dir(path: &PathBuf) {
    if path.exists() {
        remove_dir_all(path).unwrap();
    }

    create_dir_all(path).unwrap();
}

fn prepare_request(
    image_cache_dir: PathBuf,
    index: u32,
    width: u32,
    height: u32,
    bypass_cache_read: bool,
) -> (ImageClient, ImageGenerationParams) {
    if !image_cache_dir.exists() {
        std::fs::create_dir_all(&image_cache_dir).unwrap();
    }

    let client = ImageClient::new(
        ImageGenerator::new(),
        MmapImageCache::from_path(image_cache_dir.clone()).unwrap(),
        ImageMetrics::new(),
    );

    let bypass_cache_read = Some(bypass_cache_read);
    let params = ImageGenerationParams::build(index, width, height, bypass_cache_read).unwrap();

    (client, params)
}

async fn run_request(
    client: ImageClient,
    params: ImageGenerationParams,
) -> ImageCacheServiceResult {
    let mut writer = vec![];
    client.image_into(params, &mut writer).await.unwrap()
}

fn cache_miss(b: &mut Bencher<'_, WallTime>, width: u32, height: u32) {
    let runner = tokio::runtime::Runtime::new().unwrap();
    let image_cache_dir = PathBuf::from("./.platform/benches/cache/images");

    b.to_async(runner).iter_batched(
        || prepare_request(image_cache_dir.clone(), 0, width, height, true),
        async |(client, params)| {
            let result = run_request(client, params).await;
            assert!(!result.is_cached());
        },
        BatchSize::SmallInput,
    );
}

fn cache_hit(b: &mut Bencher<'_, WallTime>, width: u32, height: u32) {
    let runner = tokio::runtime::Runtime::new().unwrap();
    let image_cache_dir = PathBuf::from("./.platform/benches/cache/images");
    let dir = image_cache_dir.clone();

    // Ensure index 0 exists
    runner.block_on(async move {
        let (client, params) = prepare_request(dir, 0, width, height, false);

        run_request(client, params).await;
    });

    b.to_async(runner).iter_batched(
        || prepare_request(image_cache_dir.clone(), 0, width, height, false),
        async |(client, params)| {
            let result = run_request(client, params).await;
            assert!(result.is_cached());
        },
        BatchSize::SmallInput,
    );
}

fn bench(c: &mut Criterion) {
    let mut cache_miss_group = c.benchmark_group(format!("cache_miss"));

    cache_miss_group
        .sample_size(10)
        .bench_function("small", |b| {
            cache_miss(b, 256, 256);
        });

    cache_miss_group
        .sample_size(10)
        .bench_function("medium", |b| {
            cache_miss(b, 512, 512);
        });

    cache_miss_group
        .sample_size(10)
        .bench_function("large", |b| {
            cache_miss(b, 1024, 1024);
        });

    cache_miss_group.finish();

    let mut cache_hit_group = c.benchmark_group(format!("cache_hit"));

    cache_hit_group
        .sample_size(100)
        .bench_function("small", |b| {
            cache_hit(b, 256, 256);
        });

    cache_hit_group
        .sample_size(100)
        .bench_function("medium", |b| {
            cache_hit(b, 512, 512);
        });

    cache_hit_group
        .sample_size(100)
        .bench_function("large", |b| {
            cache_hit(b, 1024, 1024);
        });

    cache_hit_group.finish();
}

fn with_profiler() -> Criterion {
    let frequency = 100;
    let output = Output::Flamegraph(None);
    let profiler = PProfProfiler::new(frequency, output);

    Criterion::default().with_profiler(profiler)
}

criterion_group!(
    name = benches;
    config = with_profiler();
    targets = bench
);

criterion_main!(benches);
