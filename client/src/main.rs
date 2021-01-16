// this is just a test crate at the moment
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::Instant,
};

use atelier_schema::service::asset_hub;
use capnp::message::ReaderOptions;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use tokio::runtime::Runtime;

pub fn main() {
    use std::net::ToSocketAddrs;

    let addr = "127.0.0.1:9999".to_socket_addrs().unwrap().next().unwrap();

    let num_assets = Arc::new(AtomicUsize::new(0));
    let byte_size = Arc::new(AtomicUsize::new(0));
    let start_time = Instant::now();
    let mut threads = Vec::new();
    for _ in 0..8 {
        let num_assets = num_assets.clone();
        let byte_size = byte_size.clone();
        threads.push(thread::spawn(move || {
            let runtime = Runtime::new().unwrap();
            let stream = runtime
                .block_on(::tokio::net::TcpStream::connect(&addr))
                .unwrap();
            stream.set_nodelay(true).unwrap();
            use futures_util::AsyncReadExt;
            use tokio_util::compat::*;
            let (reader, writer) = stream.compat().split();
            let rpc_network = Box::new(twoparty::VatNetwork::new(
                reader,
                writer,
                rpc_twoparty_capnp::Side::Client,
                *ReaderOptions::new()
                    .nesting_limit(64)
                    .traversal_limit_in_words(Some(256 * 1024 * 1024)),
            ));

            let mut rpc_system = RpcSystem::new(rpc_network, None);
            let hub: asset_hub::Client = rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);
            let disconnector = rpc_system.get_disconnector();

            // Doesn't work because RpcSystem is not send
            // use futures_util::future::TryFutureExt;
            // runtime.spawn(rpc_system.map_err(|_| ()));

            // This can be replaced by above if RpcSystem becomes Send
            tokio::task::spawn_local(rpc_system);

            let request = hub.get_snapshot_request();
            let snapshot = runtime
                .block_on(request.send().promise)
                .unwrap()
                .get()
                .unwrap()
                .get_snapshot()
                .unwrap();
            for _i in 0..1000 {
                let request = snapshot.get_all_asset_metadata_request();
                let result = runtime.block_on(request.send().promise).unwrap();
                let result = result.get().unwrap();
                let len = result.get_assets().unwrap().len();
                num_assets.fetch_add(len as usize, Ordering::SeqCst);
                byte_size.fetch_add(
                    result.total_size().unwrap().word_count as usize * 8,
                    Ordering::SeqCst,
                );
            }
            runtime
                .block_on(disconnector)
                .expect("Failed to block on RPC disconnector.");

            // Dropping the runtime blocks until it completes
            std::mem::forget(runtime);
        }));
        thread::sleep(std::time::Duration::new(0, 10));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    let total_time = Instant::now() - start_time;
    println!(
        "got {} assets and {} bytes in {}ms",
        num_assets.load(Ordering::Acquire),
        byte_size.load(Ordering::Acquire),
        total_time.as_millis()
    );
    println!(
        "{} bytes per second and {} assets per second",
        (byte_size.load(Ordering::Acquire) as f64 / total_time.as_secs_f64()),
        (num_assets.load(Ordering::Acquire) as f64 / total_time.as_secs_f64()),
    );
}
