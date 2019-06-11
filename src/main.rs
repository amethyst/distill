#![feature(try_trait)]
#![allow(unknown_lints)]
#![warn(clippy::all, rust_2018_idioms, rust_2018_compatibility)]
#![feature(mpsc_select)]
#![feature(vec_remove_item)]

pub mod asset_daemon;
mod asset_hub;
mod asset_hub_service;
pub mod capnp_db;
pub mod error;
mod file_asset_source;
pub mod file_tracker;
mod serialized_asset;
mod utils;
pub mod watcher;

use crate::{asset_daemon::AssetDaemon, error::Result};

#[cfg(debug)]
const DEFAULT_LOGGING_LEVEL: log::LevelFilter = log::LevelFilter::Debug;
#[cfg(not(debug))]
const DEFAULT_LOGGING_LEVEL: log::LevelFilter = log::LevelFilter::Info;

fn init_logging() -> Result<()> {
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

fn init_modules() {
    #[cfg(feature = "amethyst")]
    {
        use amethyst::assets::Asset;
        amethyst::renderer::types::Texture::name();
    }
}

fn main() {
    init_logging().expect("failed to init logging");
    init_modules();

    AssetDaemon::default()
        .with_importers(
            atelier_importer::get_source_importers().map(|i| { dbg!(&i); (i.extension, (i.instantiator)()) }),
        )
        .run();
}
