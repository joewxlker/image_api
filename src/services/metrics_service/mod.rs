use opentelemetry_sdk::metrics::SdkMeterProvider;

use crate::{
    config::{OTLP_COLLECT_METRICS, OTLP_METRICS_ENDPOINT, OTLP_RESOURCE},
    services::metrics_service::otel::otel_metrics,
};

pub mod otel;

pub async fn initialize_metrics() -> Result<Option<SdkMeterProvider>, Box<dyn std::error::Error>> {
    if *OTLP_COLLECT_METRICS && OTLP_METRICS_ENDPOINT.is_some() {
        let metrics_endpoint = OTLP_METRICS_ENDPOINT.clone().unwrap();

        tracing::info!(
            endpoint = %metrics_endpoint,
            "OTLP metrics enabled; initializing exporter"
        );

        let provider = otel_metrics(&metrics_endpoint, &OTLP_RESOURCE).await?;

        tracing::info!(
            endpoint = %metrics_endpoint,
            "OTLP metrics initialized successfully"
        );

        return Ok(Some(provider));
    } else if *OTLP_COLLECT_METRICS && OTLP_METRICS_ENDPOINT.is_none() {
        tracing::warn!(
            "OTLP metrics export is enabled, but no collector endpoint is configured; metrics will not be recorded"
        );
    } else {
        tracing::info!("OTLP metrics export is disabled by configuration");
    }

    Ok(None)
}
