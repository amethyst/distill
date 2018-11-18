#![feature(int_to_from_bytes)]
#![allow(unknown_lints)]
#![warn(clippy::all)]
extern crate amethyst;
extern crate capnp;
extern crate capnp_rpc;
extern crate futures;
extern crate lmdb;
extern crate owning_ref;
extern crate rayon;
extern crate tokio_core;
extern crate tokio_io;
#[macro_use]
extern crate crossbeam_channel;
extern crate bincode;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate erased_serde;
extern crate ron;
extern crate scoped_threadpool;
extern crate num_cpus;


#[cfg(test)]
extern crate tempfile;

mod asset_hub;
mod asset_hub_service;
mod asset_import;
pub mod capnp_db;
pub mod error;
mod file_asset_source;
pub mod file_tracker;
pub mod watcher;

use capnp_db::Environment;
use file_tracker::FileTracker;
use std::{fs, path::Path, sync::Arc, thread};
use tokio_core::reactor;

#[allow(clippy::all)]
#[allow(dead_code)]
pub mod data_capnp {
    include!(concat!(env!("OUT_DIR"), "/data_capnp.rs"));
}
#[allow(clippy::all)]
#[allow(dead_code)]
pub mod service_capnp {
    include!(concat!(env!("OUT_DIR"), "/service_capnp.rs"));
}

fn main() {
    let db_dir = Path::new(".amethyst");
    let _ = fs::create_dir(db_dir);
    let asset_db = Arc::new(Environment::new(db_dir).expect("failed to create asset db"));
    let tracker = Arc::new(FileTracker::new(asset_db.clone()).expect("failed to create tracker"));

    let hub = Arc::new(
        asset_hub::AssetHub::new(asset_db.clone())
            .expect("failed to create asset hub"),
    );
    let asset_source =
        file_asset_source::FileAssetSource::new(&tracker, &hub, &asset_db)
            .expect("failed to create asset hub");
    let handle = {
        let run_tracker = tracker.clone();
        thread::spawn(move || run_tracker.clone().run(vec!["assets"]))
    };
    asset_source.run().expect("FileAssetSource.run() failed");
    // let service = asset_hub_service::AssetHubService::new(tracker.clone());
    // service.run();
    // loop {
    // tracker.clone().read_all_files().expect("failed to read all files");
    //     thread::sleep(Duration::from_millis(100));
    // }
    handle
        .join()
        .expect("file tracker thread panicked")
        .expect("file tracker returned error");
}
