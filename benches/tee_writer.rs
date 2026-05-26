#[cfg(feature = "channeled")]
use criterion::{Criterion, criterion_group, criterion_main};
#[cfg(feature = "channeled")]
use platform::{
    config::{IMAGE_CHUNK_SIZE, IMAGE_ROUTE_HANDLER_QUEUE_SIZE, TEE_WRITER_MESSAGE_SIZE},
    util::{channel_writer::non_blocking::ChannelWriter, tee_writer::non_blocking::TeeWriter},
};
#[cfg(feature = "channeled")]
use rocket::futures::AsyncWriteExt;

#[cfg(feature = "channeled")]
pub fn handle_tee_writer_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("handle_tee_writer_bench");

    group
        .sample_size(100)
        .bench_function("into_channel_writer", |b| {
            let runner = tokio::runtime::Runtime::new().unwrap();

            b.to_async(runner).iter_batched(
                || {
                    let vec_out = Vec::new();
                    let (sender, receiver) =
                        tokio::sync::mpsc::channel(*IMAGE_ROUTE_HANDLER_QUEUE_SIZE);
                    let writer = ChannelWriter::new(sender);
                    let buf = vec![0; *TEE_WRITER_MESSAGE_SIZE];

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
        .bench_function("buffered_into_channel_writer", |b| {
            let runner = tokio::runtime::Runtime::new().unwrap();

            b.to_async(runner).iter_batched(
                || {
                    let vec_out = Vec::new();
                    let (sender, receiver) =
                        tokio::sync::mpsc::channel(*IMAGE_ROUTE_HANDLER_QUEUE_SIZE);
                    let writer = ChannelWriter::new(sender);
                    let buf = vec![1; *TEE_WRITER_MESSAGE_SIZE];

                    (writer, vec_out, receiver, buf)
                },
                async |(writer, mut vec_out, mut receiver, buf)| {
                    let drain =
                        tokio::spawn(async move { while let Some(_) = receiver.recv().await {} });

                    {
                        let mut writer = writer;
                        let mut tee_writer = TeeWriter::new(&mut writer, &mut vec_out);

                        for chunk in buf.chunks(*IMAGE_CHUNK_SIZE) {
                            tee_writer.write_all(chunk).await.unwrap();
                        }

                        tee_writer.close().await.unwrap();
                    }

                    drain.await.unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });

    group.sample_size(100).bench_function("into_vec", |b| {
        let runner = tokio::runtime::Runtime::new().unwrap();

        b.to_async(runner).iter_batched(
            || {
                let buf = vec![1; *TEE_WRITER_MESSAGE_SIZE];
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
        .bench_function("buffered_into_vec", |b| {
            let runner = tokio::runtime::Runtime::new().unwrap();

            b.to_async(runner).iter_batched(
                || {
                    let buf = vec![1; *TEE_WRITER_MESSAGE_SIZE];
                    let vec_out_one = Vec::new();
                    let vec_out_two = Vec::new();

                    (vec_out_one, vec_out_two, buf)
                },
                async |(mut vec_out_one, mut vec_out_two, buf)| {
                    let mut tee_writer = TeeWriter::new(&mut vec_out_one, &mut vec_out_two);

                    for chunk in buf.chunks(*IMAGE_CHUNK_SIZE) {
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
