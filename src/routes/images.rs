use std::io::Cursor;

use rocket::{
    Response, Route, State,
    http::{ContentType, Header, Status, hyper::header::CACHE_CONTROL},
    response::{self, Responder},
    routes,
    serde::json::Json,
};

#[cfg(feature = "channeled")]
use rocket::{
    futures::{Stream, StreamExt},
    response::stream::ReaderStream,
};

use tracing::instrument;
use validator::ValidationErrors;

#[cfg(feature = "channeled")]
use crate::actions::images::stream_image_action;
use crate::services::image_service::{
    cache::{ImageMetadata, MmapImageCacheError},
    client::{ImageClient, ImageClientError},
    r#gen::ImageGenerationParams,
};

#[cfg(feature = "buffered")]
#[instrument(skip(image_client))]
#[rocket::get("/<index>?<width>&<height>&<bypass_cache_read>")]
pub async fn get_image(
    index: u32,
    width: u32,
    height: u32,
    bypass_cache_read: Option<bool>,
    image_client: &State<ImageClient>,
) -> Result<ImageBytes, ImageRouteError> {
    let dimensions = ImageGenerationParams::build(index, width, height, bypass_cache_read)?;
    let image_client = image_client.inner().clone();

    let result = image_client.image(dimensions).await?;

    Ok(ImageBytes(result.bytes_owned()))
}

#[cfg(feature = "channeled")]
#[instrument(skip(image_client))]
#[rocket::get("/<index>?<width>&<height>&<bypass_cache_read>")]
pub async fn get_image<'a>(
    index: u32,
    width: u32,
    height: u32,
    bypass_cache_read: Option<bool>,
    image_client: &State<ImageClient>,
) -> Result<ImageStream<impl Stream<Item = Vec<u8>>>, ImageRouteError> {
    let params = ImageGenerationParams::build(index, width, height, bypass_cache_read)?;
    let image_client = image_client.inner().clone();
    let image_streaming = stream_image_action(params, image_client).await;

    Ok(ImageStream(image_streaming.stream))
}

#[instrument(skip(image_client))]
#[rocket::get("/<index>/metadata?<width>&<height>")]
pub async fn get_metadata<'a>(
    index: u32,
    width: u32,
    height: u32,
    image_client: &State<ImageClient>,
) -> Result<Json<ImageMetadata>, ImageRouteError> {
    let metadata = image_client.metadata(index, width, height).await?;

    Ok(Json(metadata))
}

#[derive(thiserror::Error, Debug)]
pub enum ImageRouteError {
    #[error("Validation error: {0}")]
    ValidationErrors(#[from] ValidationErrors),
    #[error("ImageError: {0}")]
    ImageError(#[from] image::ImageError),
    #[error("ImageCacheError: {0}")]
    ImageCacheError(#[from] MmapImageCacheError),
    #[error("ImageClientError: {0}")]
    ImageClientError(#[from] ImageClientError),
}

#[cfg(feature = "channeled")]
pub struct ImageStream<S>(pub S);

#[cfg(feature = "channeled")]
impl<'r, S: Stream> Responder<'r, 'r> for ImageStream<S>
where
    S: Send + 'r,
    S::Item: AsRef<[u8]> + Send + Unpin + 'r,
{
    fn respond_to(self, _: &'r rocket::Request<'_>) -> response::Result<'r> {
        Response::build()
            .header(ContentType::JPEG)
            .header(Header::new(
                CACHE_CONTROL.as_str(),
                "public, max-age=31536000, immutable",
            ))
            .status(Status::Ok)
            .streamed_body(ReaderStream::from(self.0.map(std::io::Cursor::new)))
            .ok()
    }
}

#[cfg(feature = "buffered")]
pub struct ImageBytes(pub Vec<u8>);

#[cfg(feature = "buffered")]
impl<'a> Responder<'a, 'a> for ImageBytes {
    fn respond_to(self, _: &'a rocket::Request<'_>) -> response::Result<'a> {
        Response::build()
            .header(ContentType::JPEG)
            .header(Header::new(
                CACHE_CONTROL.as_str(),
                "public, max-age=31536000, immutable",
            ))
            .status(Status::Ok)
            .streamed_body(Cursor::new(self.0))
            .ok()
    }
}

impl<'a> Responder<'a, 'a> for ImageRouteError {
    fn respond_to(self, _: &'a rocket::Request<'_>) -> rocket::response::Result<'a> {
        let (message, status) = match self {
            Self::ImageClientError(error) => {
                tracing::error!("{}", error);

                (
                    // Technically not accurate
                    format!("Failed to generate image"),
                    Status::InternalServerError,
                )
            }
            Self::ImageError(error) => {
                tracing::error!("{}", error);

                (
                    format!("Failed to generate image"),
                    Status::InternalServerError,
                )
            }
            Self::ImageCacheError(error) => {
                tracing::error!("{}", error);

                (
                    format!("Failed to generate image"),
                    Status::InternalServerError,
                )
            }
            Self::ValidationErrors(error) => (
                format!("Received invalid params: {}", error),
                Status::BadRequest,
            ),
        };

        Response::build()
            .header(ContentType::Plain)
            .status(status)
            .sized_body(message.len(), Cursor::new(message))
            .ok()
    }
}

pub fn images_routes() -> Vec<Route> {
    routes![get_image, get_metadata]
}
