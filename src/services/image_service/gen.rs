use image::{DynamicImage, ImageBuffer, Rgb, RgbImage};
use jpeg_encoder::{ColorType, Encoder, EncodingError};
use std::f32::consts::PI;
use validator::{Validate, ValidationErrors};

#[cfg(feature = "channeled")]
use rocket::futures::{AsyncWrite, AsyncWriteExt};

#[cfg(feature = "channeled")]
use crate::{
    config::{IMAGE_CHUNK_SIZE, IMAGE_ENCODER_QUEUE_SIZE, IMAGE_ENCODING_QUALITY},
    util::channel_writer::blocking::ChannelWriter,
};
use crate::{
    config::{MAX_IMAGE_HEIGHT, MAX_IMAGE_WIDTH},
    services::image_service::client::ImageClientError,
};

fn hash32(mut n: u32) -> u32 {
    n = (n ^ (n >> 15)).wrapping_mul(0x85eb_ca6b);
    n = (n ^ (n >> 13)).wrapping_mul(0xc2b2_ae35);
    n ^ (n >> 16)
}

struct SeededNoise {
    state: u32,
}

impl SeededNoise {
    fn new(seed: u32) -> Self {
        Self {
            state: hash32(seed),
        }
    }

    fn next_f32(&mut self) -> f32 {
        self.state = self
            .state
            .wrapping_mul(1_664_525)
            .wrapping_add(1_013_904_223);
        (self.state as f64 / 4_294_967_296.0) as f32
    }
}

fn hsl_to_rgb(h: f32, s: f32, l: f32) -> [u8; 3] {
    let s = s / 100.0;
    let l = l / 100.0;

    let k = |n: f32| (n + h / 30.0) % 12.0;
    let a = s * l.min(1.0 - l);

    let f = |n: f32| l - a * (-1.0f32).max((k(n) - 3.0).min((9.0 - k(n)).min(1.0)));

    [
        (255.0 * f(0.0)).round().clamp(0.0, 255.0) as u8,
        (255.0 * f(8.0)).round().clamp(0.0, 255.0) as u8,
        (255.0 * f(4.0)).round().clamp(0.0, 255.0) as u8,
    ]
}

fn palette_from_seed(seed: u32) -> [[u8; 3]; 3] {
    let mut rand = SeededNoise::new(seed);

    let base_hue = rand.next_f32() * 360.0;
    let hue_spread = 40.0 + rand.next_f32() * 120.0;

    let c1 = hsl_to_rgb(base_hue, 80.0, 55.0);
    let c2 = hsl_to_rgb((base_hue + hue_spread) % 360.0, 80.0, 55.0);
    let c3 = hsl_to_rgb((base_hue + hue_spread * 2.0) % 360.0, 70.0, 60.0);

    [c1, c2, c3]
}

fn handle_chunk(index: u32, width: u32, height: u32, chunk: Chunk) -> Vec<(u32, u32, Rgb<u8>)> {
    let seed = index;
    let mut rand = SeededNoise::new(seed);

    let palette = palette_from_seed(seed);

    let mode = seed % 6;
    let invert = ((seed >> 3) & 1) == 1;
    let mirror_x = ((seed >> 4) & 1) == 1;
    let mirror_y = ((seed >> 5) & 1) == 1;

    let center_x = width as f32 * (0.15 + rand.next_f32() * 0.7);
    let center_y = height as f32 * (0.15 + rand.next_f32() * 0.7);

    let global_rot = rand.next_f32() * PI * 2.0;
    let scale_x = 2.2 + rand.next_f32() * 2.8;
    let scale_y = 2.2 + rand.next_f32() * 2.8;

    let base_freq_a = 0.05 + rand.next_f32() * 0.18;
    let base_freq_b = 0.05 + rand.next_f32() * 0.18;
    let base_freq_c = 0.04 + rand.next_f32() * 0.14;
    let layer1_freq = 0.03 + rand.next_f32() * 0.16;
    let layer2_freq = 0.03 + rand.next_f32() * 0.16;
    let layer3_freq = 0.05 + rand.next_f32() * 0.22;

    let warp_amp1 = 10.0 + rand.next_f32() * 70.0;
    let warp_amp2 = 10.0 + rand.next_f32() * 70.0;
    let warp_amp3 = 8.0 + rand.next_f32() * 60.0;

    let radial_freq = 8.0 + rand.next_f32() * 42.0;
    let angular_freq = 2.0 + rand.next_f32() * 16.0;
    let stripe_tilt = (rand.next_f32() - 0.5) * 2.4;
    let fold_strength = 0.2 + rand.next_f32() * 2.2;
    let threshold = -0.25 + rand.next_f32() * 0.5;

    let cos_r = global_rot.cos();
    let sin_r = global_rot.sin();

    let mut pixels = vec![];

    for x in chunk.x_start..chunk.x_end {
        for y in chunk.y_start..chunk.y_end {
            let mut px = x as f32;
            let mut py = y as f32;

            if mirror_x && px > width as f32 * 0.5 {
                px = width as f32 - px;
            }
            if mirror_y && py > height as f32 * 0.5 {
                py = height as f32 - py;
            }

            let dx = px - center_x;
            let dy = py - center_y;

            let rx = dx * cos_r - dy * sin_r;
            let ry = dx * sin_r + dy * cos_r;

            let nx = (rx / width as f32) * scale_x;
            let ny = (ry / height as f32) * scale_y;

            let r = (nx * nx + ny * ny).sqrt() + 1e-6;
            let a = ny.atan2(nx);

            let warp1 = (nx * layer1_freq * 30.0 + a * angular_freq).sin() * warp_amp1
                + (ny * layer2_freq * 26.0 - r * radial_freq * 0.3).cos() * warp_amp2 * 0.7;

            let warp2 = ((nx + ny) * layer3_freq * 22.0).sin() * warp_amp3
                + ((nx - ny) * layer2_freq * 20.0 + a * 3.0).cos() * warp_amp2 * 0.45;

            let wx = x as f32 + warp1 + stripe_tilt * ry;
            let wy = y as f32 + warp2;

            let field = match mode {
                0 => {
                    let i1 = (wx * base_freq_a + (wy * layer1_freq * 3.0).sin() * 1.2).sin();
                    let i2 = (wy * base_freq_b + (wx * layer2_freq * 3.2).cos() * 1.1).cos();
                    let i3 = ((wx * 1.3 + wy * 0.8) * base_freq_c).sin();
                    i1 * 1.8 + i2 * 1.6 + i3 * 0.9
                }
                1 => {
                    let fan = (a * angular_freq + r * radial_freq).sin();
                    let shell = (r * radial_freq * 1.7 + (a * 5.0).sin() * 2.0).cos();
                    let cross = ((wx - wy) * base_freq_a * 0.8).sin();
                    fan * 1.7 + shell * 1.5 + cross * 0.8
                }
                2 => {
                    let fold = ((nx.abs() * fold_strength + ny) * radial_freq).sin()
                        + ((ny.abs() * fold_strength - nx) * angular_freq * 3.0).cos();
                    let detail = (wx * base_freq_a).sin() * (wy * base_freq_b).cos();
                    fold * 1.9 + detail * 1.2
                }
                3 => {
                    let spin = (a * (angular_freq * 2.4) + r * radial_freq * 2.1).sin();
                    let ring = (r * radial_freq * 2.8).cos();
                    let turbulence = ((wx + warp1) * base_freq_a * 1.2).sin()
                        + ((wy + warp2) * base_freq_b * 1.2).cos();
                    spin * 1.7 + ring * 1.2 + turbulence * 1.1
                }
                4 => {
                    let grid_a = (wx * base_freq_a * 1.4).sin();
                    let grid_b = (wy * base_freq_b * 1.4).sin();
                    let diag = ((wx + wy) * base_freq_c).cos();
                    let bend = (r * radial_freq + a * angular_freq).sin();
                    grid_a * grid_b * 2.3 + diag * 0.9 + bend * 1.0
                }
                _ => {
                    let h1 = (wx * base_freq_a + (r * 10.0).cos() * 2.0).sin();
                    let h2 = (wy * base_freq_b + (a * 7.0).sin() * 1.8).cos();
                    let h3 = (r * radial_freq * 1.5 + a * angular_freq * 2.0).sin();
                    let h4 = ((wx - wy) * base_freq_c * 1.6).cos();
                    h1 * 1.3 + h2 * 1.3 + h3 * 1.1 + h4 * 0.8
                }
            };

            let band = (field * 2.0).sin();

            let mut color = if band > 0.4 {
                palette[0]
            } else if band > -0.2 {
                palette[1]
            } else {
                palette[2]
            };

            if invert {
                color = if color == palette[0] {
                    palette[1]
                } else if color == palette[1] {
                    palette[2]
                } else {
                    palette[0]
                };
            }

            let final_color = if field > threshold {
                color
            } else {
                [
                    ((color[0] as f32) * 0.12) as u8,
                    ((color[1] as f32) * 0.12) as u8,
                    ((color[2] as f32) * 0.12) as u8,
                ]
            };

            pixels.push((x, y, Rgb(final_color)));
        }
    }

    pixels
}

#[derive(Debug)]
struct Chunk {
    x_start: u32,
    x_end: u32,
    y_start: u32,
    y_end: u32,
}

impl Chunk {
    pub fn new(x_start: u32, x_end: u32, y_start: u32, y_end: u32) -> Self {
        Self {
            x_end,
            x_start,
            y_end,
            y_start,
        }
    }
}

#[cfg(feature = "buffered")]
async fn encode_progressive(img: ImageBuffer<Rgb<u8>, Vec<u8>>) -> Result<Vec<u8>, ImageGeneratorError> {
    tokio::task::spawn_blocking(|| {
        let dyn_img = DynamicImage::ImageRgb8(img);
    
        let mut out = Vec::new();
        let mut encoder = Encoder::new(&mut out, 95);
    
        encoder.set_progressive(true);
    
        let rgb = dyn_img.to_rgb8();
        encoder.encode(
            rgb.as_raw(),
            rgb.width() as u16,
            rgb.height() as u16,
            ColorType::Rgb,
        )?;
    
        Ok(out)
    }).await.unwrap()
}

#[cfg(feature = "channeled")]
async fn encode_progressive_into<W>(
    img: ImageBuffer<Rgb<u8>, Vec<u8>>,
    out: &mut W,
) -> Result<(), ImageGeneratorError>
where
    W: AsyncWrite + Unpin,
{
    let buffer = *IMAGE_ENCODER_QUEUE_SIZE;
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<Vec<u8>>(buffer);
    let channel_writer = ChannelWriter::new(sender);
    let capacity = *IMAGE_CHUNK_SIZE;
    let mut writer = std::io::BufWriter::with_capacity(capacity, channel_writer);

    let handle = tokio::task::spawn_blocking(move || {
        let quality = *IMAGE_ENCODING_QUALITY;
        let mut encoder = Encoder::new(&mut writer, quality);

        encoder.set_progressive(true);

        let dyn_img = DynamicImage::ImageRgb8(img);
        let rgb = dyn_img.to_rgb8();
        encoder.encode(
            rgb.as_raw(),
            rgb.width() as u16,
            rgb.height() as u16,
            ColorType::Rgb,
        )?;

        Ok::<(), ImageGeneratorError>(())
    });

    while let Some(data) = receiver.recv().await {
        out.write_all(&data).await?;
    }

    handle.await.map_err(|_| ImageGeneratorError::JoinError)??;

    Ok(())
}

#[derive(Copy, Clone, Debug, Validate)]
pub struct ImageGenerationParams {
    #[validate(range(min = 1, max = *MAX_IMAGE_HEIGHT))]
    pub height: u32,
    #[validate(range(min = 1, max = *MAX_IMAGE_WIDTH))]
    pub width: u32,
    pub index: u32,
}

impl ImageGenerationParams {
    pub fn build(index: u32, width: u32, height: u32) -> Result<Self, ValidationErrors> {
        let this = Self {
            index,
            width,
            height,
        };

        this.validate()?;

        Ok(this)
    }
}

#[derive(Clone)]
pub struct ImageGenerator;

impl ImageGenerator {
    pub fn new() -> Self {
        Self
    }
}

impl ImageGenerator {
    async fn generate_rgb_image(
        &self,
        index: u32,
        width: u32,
        height: u32,
    ) -> Result<RgbImage, ImageGeneratorError> {
        let qtr = height / 4;

        let chunks = vec![
            Chunk::new(0, width, 0, qtr),
            Chunk::new(0, width, qtr, qtr * 2),
            Chunk::new(0, width, qtr * 2, qtr * 3),
            Chunk::new(0, width, qtr * 3, height),
        ];

        let mut handles = vec![];

        for chunk in chunks {
            handles.push(tokio::task::spawn_blocking(move || {
                handle_chunk(index, width, height, chunk)
            }));
        }

        let mut img: RgbImage = ImageBuffer::new(width, height);

        for handle in handles {
            let pixels = handle.await.map_err(|_| ImageGeneratorError::JoinError)?;

            for (x, y, pixel) in pixels {
                img.put_pixel(x, y, pixel);
            }
        }

        Ok(img)
    }
    #[cfg(feature = "buffered")]
    pub async fn jpeg_progressive(
        &self,
        params: ImageGenerationParams,
    ) -> Result<Vec<u8>, ImageGeneratorError> {
        let image = self
            .generate_rgb_image(params.index, params.width, params.height)
            .await?;

        encode_progressive(image).await
    }
    #[cfg(feature = "channeled")]
    pub async fn jpeg_progressive_into<W>(
        &self,
        params: ImageGenerationParams,
        writer: &mut W,
    ) -> Result<(), ImageGeneratorError>
    where
        W: AsyncWrite + Unpin,
    {
        let image = self
            .generate_rgb_image(params.index, params.width, params.height)
            .await?;

        encode_progressive_into(image, writer).await
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ImageGeneratorError {
    #[error("EncodingError: {0}")]
    EncodingError(#[from] EncodingError),
    #[error("ImageError: {0}")]
    ImageError(#[from] image::ImageError),
    #[error("Unhandled IoError: {0}")]
    IoError(#[from] std::io::Error),
    #[error("JoinError")]
    JoinError,
}

#[derive(Clone)]
pub struct ImageGeneratorService {
    generator: ImageGenerator,
}

impl ImageGeneratorService {
    pub fn new(generator: ImageGenerator) -> Self {
        Self { generator }
    }
}

impl ImageGeneratorService {
    #[cfg(feature = "buffered")]
    pub async fn handle(&self, params: ImageGenerationParams) -> Result<Vec<u8>, ImageClientError> {
        self.generator
            .jpeg_progressive(params)
            .await
            .map_err(ImageClientError::ImageGenError)
    }

    #[cfg(feature = "channeled")]
    pub async fn handle_into<W>(
        &self,
        params: ImageGenerationParams,
        writer: &mut W,
    ) -> Result<(), ImageClientError>
    where
        W: AsyncWrite + Unpin,
    {
        self.generator
            .jpeg_progressive_into(params, writer)
            .await
            .map_err(ImageClientError::ImageGenError)
    }
}
