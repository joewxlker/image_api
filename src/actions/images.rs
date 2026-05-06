#[cfg(feature = "channeled")]
use {
    rocket::response::stream,
    crate::routes::images::ImageStream,
    rocket::futures::Stream,
    crate::util::channel_writer::non_blocking::ChannelWriter
};

#[cfg(not(feature = "channeled"))]
use crate::routes::images::{ImageBytes, ImageRouteError};

use crate::{services::image_service::{client::ImageClient, r#gen::ImageGenerationParams}};

#[cfg(feature = "channeled")]
pub async fn stream_image_action(
    dimensions: ImageGenerationParams,
    image_client: ImageClient,
) -> ImageStream<impl Stream<Item = Vec<u8>>> {
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<Vec<u8>>(32);

    tokio::task::spawn(async move {
        let mut writer = ChannelWriter::new(sender);

        if let Err(err) = image_client.image_into(dimensions, &mut writer).await {
            tracing::error!("Image streaming failed: {err}");
        }
    });

    let stream = stream::stream! {
        while let Some(msg) = receiver.recv().await {
            yield msg
        }
    };

    ImageStream(stream)
}

#[cfg(not(feature = "channeled"))]
pub async fn image_bytes_action(
    dimensions: ImageGenerationParams,
    mut image_client: ImageClient,
) -> Result<ImageBytes, ImageRouteError> {
    let result = image_client.image(dimensions).await?;

    Ok(ImageBytes(result.bytes_owned()))
}