use std::{fs::OpenOptions, path::PathBuf};

use tracing_subscriber::{Layer, layer::SubscriberExt};

use crate::services::log_service::ENV_FILTER;

pub fn file_logger(log_file_path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let log_file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&log_file_path)
        .map_err(|e| format!("{e}: {:?}", log_file_path))?;

    let filter = ENV_FILTER.clone();

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(log_file)
        .compact()
        .with_filter(filter);

    let file_logger = tracing_subscriber::registry().with(fmt_layer);

    tracing::subscriber::set_global_default(file_logger)?;

    tracing::info!(
        path = %log_file_path.display(),
        "File logging initialized; writing logs to disk"
    );

    Ok(())
}