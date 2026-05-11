#[cfg(feature = "channeled")]
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
#[cfg(feature = "channeled")]
use platform::services::image_service::r#gen::{ImageGenerationParams, ImageGenerator};

#[cfg(feature = "channeled")]
fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_progressive");

    group.sample_size(100).bench_function("512x512", |b| {
        let runner = tokio::runtime::Runtime::new().unwrap();

        b.to_async(runner).iter_batched(
            || {
                let params = ImageGenerationParams::build(0, 512, 512).unwrap();
                let generator = ImageGenerator::new();

                (params, generator)
            },
            async |(params, generator)| {
                let mut writer = vec![];
                generator
                    .jpeg_progressive_into(params, &mut writer)
                    .await
                    .unwrap();
            },
            BatchSize::SmallInput,
        );
    });

    group.sample_size(40).bench_function("1024x1024", |b| {
        let runner = tokio::runtime::Runtime::new().unwrap();

        b.to_async(runner).iter_batched(
            || {
                let params = ImageGenerationParams::build(0, 1024, 1024).unwrap();
                let generator = ImageGenerator::new();

                (params, generator)
            },
            async |(params, generator)| {
                let mut writer = vec![];
                generator
                    .jpeg_progressive_into(params, &mut writer)
                    .await
                    .unwrap();
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench);

criterion_main!(benches);
