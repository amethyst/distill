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

#[cfg(test)]
extern crate tempfile;

mod asset_hub;
mod asset_hub_service;
mod asset_import;
pub mod capnp_db;
pub mod file_error;
pub mod file_tracker;
pub mod watcher;

use file_tracker::FileTracker;
use std::path::Path;
use std::sync::Arc;
use std::thread;

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
            run_tracker.clone().run(vec!["assets"])
        })
    };

    let hub = asset_hub::AssetHub::new(tracker).expect("failed to create asset hub");
    hub.run().expect("AssetHub.run() failed");
    // let service = asset_hub_service::AssetHubService::new(tracker.clone());
    // service.run();
    // loop {
        // tracker.clone().read_all_files().expect("failed to read all files");
    //     thread::sleep(Duration::from_millis(100));
    // }
    handle.join().expect("file tracker thread panicked").expect("file tracker returned error");
}
