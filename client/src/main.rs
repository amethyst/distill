extern crate capnp;
extern crate capnp_rpc;
extern crate futures;
extern crate time;
extern crate tokio;
extern crate schema;

use schema::{data, service::asset_hub};
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};

use capnp::capability::Promise;

use futures::{executor::spawn, future::Executor, Future};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    sync::Arc,
    thread,
};
use time::PreciseTime;
use tokio::runtime::current_thread::Runtime;
use tokio::prelude::*;

pub fn main() {
    use std::net::ToSocketAddrs;
    use std::collections::HashMap;
    let mut wot = HashMap::new();
    wot.insert("fd", "fds");

    let addr = "localhost:9999".to_socket_addrs().unwrap().next().unwrap();

    // let thread = thread::spawn(move || {

    // let result = spawn(request.send().promise);
    // let snapshot = request.get();
    // let all_assets = snapshot.get_all_assets_request().get();
    // });

    let num_assets = Arc::new(AtomicUsize::new(0));
    let byte_size = Arc::new(AtomicUsize::new(0));
    let start_time = PreciseTime::now();
    let mut threads = Vec::new();
    for _ in 0..16 {
        let num_assets = num_assets.clone();
        let byte_size = byte_size.clone();
        threads.push(thread::spawn(move || {
        let mut runtime = Runtime::new().unwrap();
            let stream = runtime
                .block_on(::tokio::net::TcpStream::connect(&addr))
                .unwrap();
            stream.set_nodelay(true).unwrap();
            let (reader, writer) = stream.split();
            let rpc_network = Box::new(twoparty::VatNetwork::new(
                reader,
                writer,
                rpc_twoparty_capnp::Side::Client,
                Default::default(),
            ));

            let mut rpc_system = RpcSystem::new(rpc_network, None);
            let hub: asset_hub::Client = rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);
            runtime.spawn(rpc_system.map_err(|_| ()));
            let request = hub.get_snapshot_request();
            let snapshot = runtime
                .block_on(request.send().promise)
                .unwrap()
                .get()
                .unwrap()
                .get_snapshot()
                .unwrap();
            for i in 0..10000 {
                let request = snapshot.get_all_assets_request();
                let result = runtime.block_on(request.send().promise).unwrap();
                let result = result.get().unwrap();
                let len = result.get_assets().unwrap().len();
                num_assets.fetch_add(len as usize, Ordering::SeqCst);
                byte_size.fetch_add(
                    result.total_size().unwrap().word_count as usize,
                    Ordering::SeqCst,
                );
            }
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    let total_time = start_time.to(PreciseTime::now());
    println!(
        "got {} assets and {} bytes in {}",
        num_assets.load(Ordering::Acquire),
        byte_size.load(Ordering::Acquire),
        total_time
    );
    println!(
        "{} bytes per second and {} assets per second",
        byte_size.load(Ordering::Acquire) as f64 / total_time.num_seconds() as f64,
        num_assets.load(Ordering::Acquire) as f64 / total_time.num_seconds() as f64,
    );
}
