use std::{cell::LazyCell, path::PathBuf};

use opentelemetry_sdk::logs::SdkLoggerProvider;
use tracing_subscriber::EnvFilter;

use crate::{
    config::{LOG_FILE_ALL, LOG_TO_FILE, OTLP_COLLECT_LOGS, OTLP_LOGS_ENDPOINT, OTLP_RESOURCE},
    services::log_service::{fs::file_logger, otel::otel_logger, stdout::set_stdout_logger},
};

mod fs;
pub mod otel;
pub mod stdout;

pub const ENV_FILTER: LazyCell<EnvFilter> = LazyCell::new(|| {
    EnvFilter::new(
        "info,\
         rocket=warn,\
         rocket_codegen=off,\
         rocket_http=off,\
         hyper=off,\
         h2=off,\
         mio=off,\
         tokio_util=off,\
         want=off,\
         tower=off,\
         tracing::span=off",
    )
});

/// Initializes logging based on configuration.
///
/// Selects between OTLP, file, or stdout logging. Returns an OTLP
/// [`SdkLoggerProvider`] if remote logging is enabled.
///
/// # Errors
///
/// - Returns an error if OTLP initialization or file logging setup fails.
pub async fn initialize_logging() -> Result<Option<SdkLoggerProvider>, LogServiceError> {
    if *OTLP_COLLECT_LOGS && OTLP_LOGS_ENDPOINT.is_some() {
        let logs_endpoint = OTLP_LOGS_ENDPOINT.clone().unwrap();

        tracing::info!(
            endpoint = %logs_endpoint,
            "OTLP logging enabled; initializing remote log exporter"
        );

        let provider = otel_logger(&logs_endpoint, &OTLP_RESOURCE).await?;

        tracing::info!(
            endpoint = %logs_endpoint,
            "OTLP logging initialized successfully"
        );

        return Ok(Some(provider));
    } else if *OTLP_COLLECT_LOGS && OTLP_LOGS_ENDPOINT.is_none() {
        tracing::warn!(
            "OTLP logging is enabled, but no collector endpoint is configured; falling back to file logging"
        );
    } else if *LOG_TO_FILE {
        tracing::info!("OTLP logging is disabled by configuration; using file logging");

        tracing::info!(
            path = %LOG_FILE_ALL.display(),
            "Initializing file logger"
        );

        file_logger(&LOG_FILE_ALL)?;

        tracing::info!(
            path = %LOG_FILE_ALL.display(),
            "File logging initialized successfully"
        );
    } else {
        tracing::warn!("No logging backend configured; falling back to stdout-only logging");

        set_stdout_logger();
    }

    Ok(None)
}

#[derive(thiserror::Error, Debug)]
pub enum LogServiceError {
    #[error("failed to open log file `{0}`: {1}")]
    OpenFileError(PathBuf, #[source] std::io::Error),

    #[error("failed to build OTLP exporter: {0}")]
    ExporterBuildError(#[from] opentelemetry_otlp::ExporterBuildError),
}
