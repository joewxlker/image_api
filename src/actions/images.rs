#[cfg(feature = "channeled")]
use {
    crate::{
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

#[cfg(feature = "channeled")]
pub struct ImageStreaming<S> {
    pub stream: S,
    pub finished: JoinHandle<Result<ImageCacheServiceResult, ImageClientError>>,
}

#[cfg(feature = "channeled")]
pub async fn stream_image_action(
    dimensions: ImageGenerationParams,
    image_client: ImageClient,
) -> ImageStreaming<impl Stream<Item = Vec<u8>>> {
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<Vec<u8>>(32);

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
