use crate::{
    routes::images::ImageStream,
    services::image_service::{
        client::{ImageClient, ImageClientError},
        r#gen::ImageGenerationParams,
    },
};

pub enum ImageOutput<S: ImageStream> {
    Bytes(Vec<u8>),
    Stream(Box<S>),
}

impl<S: ImageStream> ImageOutput<S> {
    pub fn map_stream<T: ImageStream>(self, f: impl FnOnce(S) -> T) -> ImageOutput<T> {
        use ImageOutput::*;

        match self {
            Bytes(cache) => ImageOutput::<T>::Bytes(cache),
            Stream(stream) => Stream(Box::new(f(*stream))),
        }
    }
}

impl<S: ImageStream> ImageOutput<S> {
    pub fn is_cached(&self) -> bool {
        matches!(self, Self::Bytes(_))
    }
}

pub async fn stream_image(
    params: ImageGenerationParams,
    image_client: ImageClient,
) -> Result<ImageOutput<impl ImageStream + use<>>, ImageClientError> {
    image_client.image_into(params).await
}
