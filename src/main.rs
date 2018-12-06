#![feature(try_trait)]
#![feature(int_to_from_bytes)]
#![allow(unknown_lints)]
#![warn(clippy::all)]
#![feature(mpsc_select)]

mod asset_hub;
mod asset_hub_service;
mod asset_import;
pub mod capnp_db;
pub mod error;
mod file_asset_source;
pub mod file_tracker;
mod utils;
pub mod watcher;

use capnp_db::Environment;
use error::Result;
use file_tracker::FileTracker;
use std::{fs, path::Path, sync::Arc, thread};


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

fn main() {
    init_logging().expect("failed to init logging");
    let db_dir = Path::new(".amethyst");
    let _ = fs::create_dir(db_dir);
    let asset_db = Arc::new(Environment::new(db_dir).expect("failed to create asset db"));
    let tracker = Arc::new(FileTracker::new(asset_db.clone()).expect("failed to create tracker"));

    let hub =
        Arc::new(asset_hub::AssetHub::new(asset_db.clone()).expect("failed to create asset hub"));
    let asset_source = Arc::new(
        file_asset_source::FileAssetSource::new(&tracker, &hub, &asset_db)
            .expect("failed to create asset hub"),
    );
    let handle = {
        let run_tracker = tracker.clone();
        thread::spawn(move || run_tracker.clone().run(vec!["assets"]))
    };
    {
        let asset_source_handle = asset_source.clone();
        thread::spawn(move || {
            asset_source_handle
                .run()
                .expect("FileAssetSource.run() failed")
        })
    };
    let service = asset_hub_service::AssetHubService::new(asset_db.clone(), hub.clone());
    service.run();
    // loop {
    // tracker.clone().read_all_files().expect("failed to read all files");
    //     thread::sleep(Duration::from_millis(100));
    // }
    handle
        .join()
        .expect("file tracker thread panicked")
        .expect("file tracker returned error");
}
