#[cfg(feature = "channeled")]
use criterion::{Criterion, criterion_group, criterion_main};
#[cfg(feature = "channeled")]
use platform::util::{
    channel_writer::non_blocking::ChannelWriter, tee_writer::non_blocking::TeeWriter,
};
#[cfg(feature = "channeled")]
use rocket::futures::AsyncWriteExt;

#[cfg(feature = "channeled")]
const MESSAGE_LENGTH: usize = 591140; // typical 512x512 image size
#[cfg(feature = "channeled")]
const CHANNEL_BUFFER_LENGTH: usize = 128; // max queue size of 75665920 bytes
#[cfg(feature = "channeled")]
const CHUNK_SIZE: usize = 256 * 1024; // current buffer size for progressive image generation into

#[cfg(feature = "channeled")]
pub fn handle_tee_writer_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("handle_tee_writer_bench");

    group
        .sample_size(100)
        .bench_function("into_vec_x_async_channel", |b| {
            let runner = tokio::runtime::Runtime::new().unwrap();

            b.to_async(runner).iter_batched(
                || {
                    let vec_out = Vec::new();
                    let (sender, receiver) = tokio::sync::mpsc::channel(CHANNEL_BUFFER_LENGTH);
                    let writer = ChannelWriter::new(sender);
                    let buf = vec![1; MESSAGE_LENGTH];

                    (writer, vec_out, receiver, buf)
                },
                async |(writer, mut vec_out, mut receiver, buf)| {
                    let drain =
                        tokio::spawn(async move { while let Some(_) = receiver.recv().await {} });

                    {
                        let mut writer = writer;
                        let mut tee_writer = TeeWriter::new(&mut writer, &mut vec_out);
                        tee_writer.write_all(&buf).await.unwrap();
                        tee_writer.close().await.unwrap();
                    }

                    drain.await.unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });

    group
        .sample_size(10)
        .bench_function("into_vec_x_async_channel_chunked", |b| {
            let runner = tokio::runtime::Runtime::new().unwrap();

            b.to_async(runner).iter_batched(
                || {
                    let vec_out = Vec::new();
                    let (sender, receiver) = tokio::sync::mpsc::channel(CHANNEL_BUFFER_LENGTH);
                    let writer = ChannelWriter::new(sender);
                    let buf = vec![1; MESSAGE_LENGTH];

                    (writer, vec_out, receiver, buf)
                },
                async |(writer, mut vec_out, mut receiver, buf)| {
                    let drain =
                        tokio::spawn(async move { while let Some(_) = receiver.recv().await {} });

                    {
                        let mut writer = writer;
                        let mut tee_writer = TeeWriter::new(&mut writer, &mut vec_out);

                        for chunk in buf.chunks(CHUNK_SIZE) {
                            tee_writer.write_all(chunk).await.unwrap();
                        }

                        tee_writer.close().await.unwrap();
                    }

                    drain.await.unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });

    group
        .sample_size(100)
        .bench_function("into_vec_x_vec", |b| {
            let runner = tokio::runtime::Runtime::new().unwrap();

            b.to_async(runner).iter_batched(
                || {
                    let buf = vec![1; MESSAGE_LENGTH];
                    let vec_out_one = Vec::new();
                    let vec_out_two = Vec::new();

                    (vec_out_one, vec_out_two, buf)
                },
                async |(mut vec_out_one, mut vec_out_two, buf)| {
                    let mut tee_writer = TeeWriter::new(&mut vec_out_one, &mut vec_out_two);
                    tee_writer.write_all(&buf).await.unwrap();
                    tee_writer.close().await.unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });

    group
        .sample_size(10)
        .bench_function("into_vec_x_vec_chunked", |b| {
            let runner = tokio::runtime::Runtime::new().unwrap();

            b.to_async(runner).iter_batched(
                || {
                    let buf = vec![1; MESSAGE_LENGTH];
                    let vec_out_one = Vec::new();
                    let vec_out_two = Vec::new();

                    (vec_out_one, vec_out_two, buf)
                },
                async |(mut vec_out_one, mut vec_out_two, buf)| {
                    let mut tee_writer = TeeWriter::new(&mut vec_out_one, &mut vec_out_two);

                    for chunk in buf.chunks(CHUNK_SIZE) {
                        tee_writer.write_all(chunk).await.unwrap();
                    }

                    tee_writer.close().await.unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });

    group.finish();
}

#[cfg(feature = "channeled")]
criterion_group!(benches, handle_tee_writer_bench);
#[cfg(feature = "channeled")]
criterion_main!(benches);
