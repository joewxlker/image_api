use std::{cell::LazyCell, sync::Arc};

use opentelemetry_sdk::{Resource, logs::SdkLoggerProvider};
use tracing_subscriber::{EnvFilter, Layer, Registry, filter::Filtered, fmt::{self, format::{Compact, DefaultFields}}, layer::{Layered, SubscriberExt}};

use crate::{config::Config, services::log_service::{fs::file_logger, otel::otel_logger}};

pub mod fs;
pub mod otel;

pub type SimpleLogger = Arc<
    Layered<
        Filtered<
            fmt::Layer<Registry, DefaultFields, fmt::format::Format<Compact>>,
            EnvFilter,
            Registry,
        >,
        Registry,
    >,
>;

pub const STDOUT_LOGGER: LazyCell<SimpleLogger> = LazyCell::new(|| {
    let filter = EnvFilter::new(
        "info,\
         rocket=off,\
         rocket_codegen=off,\
         rocket_http=off,\
         hyper=off,\
         h2=off,\
         mio=off,\
         tokio_util=off,\
         want=off,\
         tower=off,\
         tracing::span=off",
    );

    let fmt_layer = fmt::layer().compact().with_filter(filter);

    let registry = Registry::default().with(fmt_layer);

    Arc::new(registry)
});

pub const ENV_FILTER: LazyCell<EnvFilter> = LazyCell::new(|| {
    EnvFilter::new(
        "info,\
         rocket=off,\
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

pub async fn initialize_logging(
    config: &Config,
) -> Result<Option<SdkLoggerProvider>, Box<dyn std::error::Error>> {
    if config.otlp.collect_logs && config.otlp.collector_endpoint.is_some() {
        let collector_endpoint = config.otlp.collector_endpoint.clone().unwrap();

        let logs_endpoint = collector_endpoint.join("/v1/logs")?;
        let resource = Resource::from(config);

        tracing::info!(
            endpoint = %logs_endpoint,
            "OTLP logging enabled; initializing remote log exporter"
        );

        let provider = otel_logger(&logs_endpoint, resource).await?;

        tracing::info!(
            endpoint = %logs_endpoint,
            "OTLP logging initialized successfully"
        );

        return Ok(Some(provider));
    } else if config.otlp.collect_logs && config.otlp.collector_endpoint.is_none() {
        tracing::warn!(
            "OTLP logging is enabled, but no collector endpoint is configured; falling back to file logging"
        );
    } else {
        tracing::info!("OTLP logging is disabled by configuration; using file logging");
    }

    let log_file_path = config.log_directory.join("output.log");

    tracing::info!(
        path = %log_file_path.display(),
        "Initializing file logger"
    );

    file_logger(&log_file_path)?;

    tracing::info!(
        path = %log_file_path.display(),
        "File logging initialized successfully"
    );

    Ok(None)
}