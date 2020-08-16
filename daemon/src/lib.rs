#![recursion_limit = "1024"] // required for select!
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
mod scope;
mod serialized_asset;
mod source_pair_import;
mod watcher;

pub use crate::{
    daemon::AssetDaemon,
    error::{Error, Result},
};

#[cfg(debug_assertions)]
const DEFAULT_LOGGING_LEVEL: log::LevelFilter = log::LevelFilter::Debug;
#[cfg(not(debug_assertions))]
const DEFAULT_LOGGING_LEVEL: log::LevelFilter = log::LevelFilter::Info;

mod simple_logger {
    use log::{Level, Metadata, Record};

    pub struct SimpleLogger;

    impl log::Log for SimpleLogger {
        fn enabled(&self, metadata: &Metadata<'_>) -> bool {
            metadata.level() <= Level::Info
        }

        fn log(&self, record: &Record<'_>) {
            if self.enabled(record.metadata()) {
                println!("{} - {}", record.level(), record.args());
            }
        }

        fn flush(&self) {}
    }
}
#[cfg(not(feature = "pretty_log"))]
static LOGGER: simple_logger::SimpleLogger = simple_logger::SimpleLogger;

#[cfg(not(feature = "pretty_log"))]
pub fn init_logging() -> Result<()> {
    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(DEFAULT_LOGGING_LEVEL))
        .expect("failed to init logger");
    Ok(())
}
#[cfg(feature = "pretty_log")]
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
