use std::{
    fs::{create_dir_all, remove_dir_all},
    path::PathBuf,
    sync::atomic::{AtomicU32, Ordering},
};

use criterion::{
    Bencher, Criterion, criterion_group, criterion_main,
    measurement::WallTime,
    profiler::{ExternalProfiler, Profiler},
};

use criterion::BatchSize;

use platform::services::image_service::{
    cache::{ImageCacheServiceResult, MmapImageCache},
    client::ImageClient,
    r#gen::{ImageGenerationParams, ImageGenerator},
    metrics::ImageMetrics,
};

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
) -> (ImageClient, ImageGenerationParams) {
    let client = ImageClient::new(
        ImageGenerator::new(),
        MmapImageCache::from_path(image_cache_dir.clone()).unwrap(),
        ImageMetrics::new(),
    );

    let params = ImageGenerationParams::build(index, width, height).unwrap();

    (client, params)
}

async fn run_request(
    client: ImageClient,
    params: ImageGenerationParams,
) -> ImageCacheServiceResult {
    #[cfg(feature = "buffered")]
    {
        client.image(params).await.unwrap()
    }

    #[cfg(feature = "channeled")]
    {
        let mut writer = vec![];
        client.image_into(params, &mut writer).await.unwrap()
    }
}

fn cache_miss(b: &mut Bencher<'_, WallTime>, width: u32, height: u32) {
    let runner = tokio::runtime::Runtime::new().unwrap();
    let iter_count = AtomicU32::new(0);
    let image_cache_dir = PathBuf::from("./cache/benches/images");
    initialize_empty_cache_dir(&image_cache_dir);

    b.to_async(runner).iter_batched(
        || {
            let index = iter_count.fetch_add(1, Ordering::Relaxed);

            prepare_request(image_cache_dir.clone(), index, width, height)
        },
        async |(client, params)| {
            let result = run_request(client, params).await;
            assert!(!result.is_cached());
        },
        BatchSize::SmallInput,
    );
}

fn cache_hit(b: &mut Bencher<'_, WallTime>, width: u32, height: u32) {
    let runner = tokio::runtime::Runtime::new().unwrap();
    let image_cache_dir = PathBuf::from("./cache/benches/static");

    // TODO - initialize image_cache_dir with asset to 
    // prevent cache miss on first 'run_request'

    b.to_async(runner).iter_batched(
        || prepare_request(image_cache_dir.clone(), 0, width, height),
        async |(client, params)| {
            let result = run_request(client, params).await;
            assert!(result.is_cached());
        },
        BatchSize::SmallInput,
    );
}

fn bench(c: &mut Criterion) {
    #[cfg(feature = "buffered")]
    let mode = "buffered";

    #[cfg(feature = "channeled")]
    let mode = "channeled";

    let mut cache_miss_group = c.benchmark_group(format!("{mode}/cache_miss"));

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

    let mut cache_hit_group = c.benchmark_group(format!("{mode}/cache_hit"));

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

struct CpuProfiler;

impl Profiler for CpuProfiler {
    fn start_profiling(&mut self, _benchmark_id: &str, benchmark_dir: &std::path::Path) {
        if !benchmark_dir.exists() {
            std::fs::create_dir_all(benchmark_dir).unwrap();
        }
        let file_path = benchmark_dir.join("profile.pb.gz");

        if !file_path.exists() {
            std::fs::File::create_new(&file_path).unwrap();
        }

        cpuprofiler::PROFILER
            .lock()
            .unwrap()
            .start(file_path.to_str().unwrap())
            .unwrap();
    }
    fn stop_profiling(&mut self, _benchmark_id: &str, _benchmark_dir: &std::path::Path) {
        cpuprofiler::PROFILER.lock().unwrap().stop().unwrap();
    }
}

fn with_profiler() -> Criterion {
    Criterion::default().with_profiler(CpuProfiler)
}

criterion_group!(
    name = benches;
    config = with_profiler();
    targets = bench
);

criterion_main!(benches);
