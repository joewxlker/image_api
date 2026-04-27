use std::{fs::OpenOptions, path::PathBuf};

use tracing_subscriber::{Layer, layer::SubscriberExt, util::SubscriberInitExt};

use crate::services::log_service::{ENV_FILTER, LogServiceError};

pub fn file_logger(log_file_path: &PathBuf) -> Result<(), LogServiceError> {
    let log_file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&log_file_path)
        .map_err(|e| LogServiceError::OpenFileError(log_file_path.clone(), e))?;

    let filter = ENV_FILTER.clone();

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(log_file)
        .compact()
        .with_filter(filter);

    tracing_subscriber::registry().with(fmt_layer).init();

    tracing::info!(
        path = %log_file_path.display(),
        "File logging initialized; writing logs to disk"
    );

    Ok(())
}
