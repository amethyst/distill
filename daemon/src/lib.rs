#![allow(unknown_lints)]
#![warn(clippy::all, rust_2018_idioms, rust_2018_compatibility)]

mod artifact_cache;
mod asset_hub;
mod asset_hub_service;
mod capnp_db;
mod daemon;
mod error;
mod file_asset_source;
mod file_tracker;
mod serialized_asset;
mod source_pair_import;
mod watcher;

pub use crate::{
    daemon::AssetDaemon,
    error::{Error, Result},
};

#[cfg(debug)]
const DEFAULT_LOGGING_LEVEL: log::LevelFilter = log::LevelFilter::Debug;
#[cfg(not(debug))]
const DEFAULT_LOGGING_LEVEL: log::LevelFilter = log::LevelFilter::Info;

pub fn init_logging() -> Result<()> {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{timestamp}][{level}][{target}] {message}",
                level = record.level(),
                timestamp = chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%.3f"),
                target = record.target(),
                message = message,
            ))
        })
        .chain(std::io::stdout())
        .level(DEFAULT_LOGGING_LEVEL)
        .level_for("mio", log::LevelFilter::Info)
        .level_for("tokio_core", log::LevelFilter::Info)
        // .chain(fern::log_file("output.log")?)
        .apply()?;
    Ok(())
}
