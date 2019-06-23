#![allow(unknown_lints)]
#![warn(clippy::all, rust_2018_idioms, rust_2018_compatibility)]

pub mod daemon;
mod asset_hub;
mod asset_hub_service;
pub mod capnp_db;
pub mod error;
mod file_asset_source;
pub mod file_tracker;
mod serialized_asset;
mod utils;
pub mod watcher;

pub use crate::{daemon::AssetDaemon, error::Result};

#[cfg(debug)]
const DEFAULT_LOGGING_LEVEL: log::LevelFilter = log::LevelFilter::Debug;
#[cfg(not(debug))]
const DEFAULT_LOGGING_LEVEL: log::LevelFilter = log::LevelFilter::Info;

pub fn init_logging() -> Result<()> {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{level}][{target}] {message}",
                level = record.level(),
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