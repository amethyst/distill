extern crate capnp;
extern crate lmdb;
extern crate rayon;
extern crate capnp_rpc;
extern crate tokio_io;
extern crate tokio_core;
extern crate futures;
extern crate owning_ref;
extern crate amethyst;
#[macro_use]
extern crate crossbeam_channel;
extern crate serde;
extern crate serde_derive;
extern crate erased_serde;
#[macro_use]
extern crate downcast;

pub mod watcher;
pub mod file_error;
pub mod capnp_db;
pub mod file_tracker;
mod asset_hub_service;
mod asset_hub;
mod asset_import;


use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::path::{Path};
use file_tracker::FileTracker;

#[allow(dead_code)]
pub mod data_capnp {
    include!(concat!(env!("OUT_DIR"), "/data_capnp.rs"));
}
#[allow(dead_code)]
pub mod service_capnp {
    include!(concat!(env!("OUT_DIR"), "/service_capnp.rs"));
}


fn main() {
    let tracker =
        Arc::new(FileTracker::new(Path::new(".amethyst")).expect("failed to create tracker"));
    let handle = {
        let run_tracker = tracker.clone();
        thread::spawn(move || {
            println!(
                "result: {:?}",
                run_tracker
                    .clone()
                    .run(vec!["assets"])
            );
        })
    };

let hub = asset_hub::AssetHub::new(tracker);
hub.run();
    // let service = asset_hub_service::AssetHubService::new(tracker.clone());
    // service.run();
    loop {
        // tracker.clone().read_all_files().expect("failed to read all files");
        thread::sleep(Duration::from_millis(100));
    }
    handle.join();
}
