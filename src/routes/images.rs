use std::io::Cursor;

use rocket::{
    Response, Route, State,
    http::{ContentType, Header, Status, hyper::header::CACHE_CONTROL},
    response::{self, Responder},
    routes,
    serde::json::Json,
};

use rocket::{futures::StreamExt, response::stream::ReaderStream};

use tokio_stream::Stream;
use tracing::instrument;
use validator::ValidationErrors;

use crate::actions::images::{ImageOutput, stream_image};
use crate::services::image_service::{
    cache::{ImageMetadata, MmapImageCacheError},
    client::{ImageClient, ImageClientError},
    r#gen::ImageGenerationParams,
};

#[instrument(skip(image_client))]
#[rocket::get("/<index>?<width>&<height>&<bypass_cache_read>")]
pub async fn get_image<'a>(
    index: u32,
    width: u32,
    height: u32,
    bypass_cache_read: Option<bool>,
    image_client: &State<ImageClient>,
) -> Result<ImageOutput<impl ImageStream + use<>>, ImageRouteError> {
    let params = ImageGenerationParams::build(index, width, height, bypass_cache_read)?;
    let image_client = image_client.inner().clone();
    let image_streaming = stream_image(params, image_client).await?;

    Ok(image_streaming)
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

pub trait ImageStream: Stream<Item: AsRef<[u8]> + Unpin + Send> + Unpin + Send {}

impl<'r, S: ImageStream + 'r> Responder<'r, 'r> for ImageOutput<S> {
    fn respond_to(self, _request: &'r rocket::Request<'_>) -> response::Result<'r> {
        match self {
            Self::Bytes(bytes) => Response::build()
                .header(ContentType::JPEG)
                .header(Header::new(
                    CACHE_CONTROL.as_str(),
                    "public, max-age=31536000, immutable",
                ))
                .status(Status::Ok)
                .sized_body(bytes.len(), Cursor::new(bytes))
                .ok(),
            Self::Stream(stream) => Response::build()
                .header(ContentType::JPEG)
                .header(Header::new(
                    CACHE_CONTROL.as_str(),
                    "public, max-age=31536000, immutable",
                ))
                .status(Status::Ok)
                .streamed_body(ReaderStream::from(stream.map(std::io::Cursor::new)))
                .ok(),
        }
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
