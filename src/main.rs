extern crate capnp;
extern crate lmdb;
extern crate rayon;
extern crate capnp_rpc;
extern crate tokio_io;
extern crate tokio_core;
extern crate futures;

pub mod watcher;
pub mod file_error;
pub mod capnp_db;
pub mod file_tracker;
pub mod file_service;

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
        Arc::new(Box::new(FileTracker::new(Path::new(".amethyst")).expect("failed to create tracker")));
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

    let service = file_service::FileService::new(tracker.clone());
    service.run();
    loop {
        // tracker.clone().read_all_files().expect("failed to read all files");
        thread::sleep(Duration::from_millis(100));
    }
    handle.join();
}
