use {
    crate::{
        config::IMAGE_ROUTE_HANDLER_QUEUE_SIZE,
        services::image_service::{
            cache::ImageCacheServiceResult,
            client::{ImageClient, ImageClientError},
            r#gen::ImageGenerationParams,
        },
        util::channel_writer::non_blocking::ChannelWriter,
    },
    rocket::{futures::Stream, response::stream},
    tokio::task::JoinHandle,
};

pub struct ImageStreaming<S> {
    pub stream: S,
    pub finished: JoinHandle<Result<ImageCacheServiceResult, ImageClientError>>,
}

pub async fn stream_image(
    dimensions: ImageGenerationParams,
    image_client: ImageClient,
) -> ImageStreaming<impl Stream<Item = Vec<u8>>> {
    let buffer = *IMAGE_ROUTE_HANDLER_QUEUE_SIZE;
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<Vec<u8>>(buffer);

    let finished = tokio::task::spawn(async move {
        let mut writer = ChannelWriter::new(sender);

        image_client.image_into(dimensions, &mut writer).await
    });

    let stream = stream::stream! {
        while let Some(msg) = receiver.recv().await {
            yield msg
        }
    };

    ImageStreaming { stream, finished }
}
