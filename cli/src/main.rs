use atelier_schema::{
    data,
    service::asset_hub::{self, snapshot::Client as Snapshot},
};
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};

use capnp::message::ReaderOptions;

use futures::Future;
use shrust;
use std::{cell::RefCell, io, rc::Rc, time::Instant};
use tokio::prelude::*;
use tokio::runtime::current_thread::Runtime;

type Promise<T> = capnp::capability::Promise<T, capnp::Error>;

struct ListenerImpl {
    snapshot: Rc<RefCell<Snapshot>>,
}
impl asset_hub::listener::Server for ListenerImpl {
    fn update(
        &mut self,
        params: asset_hub::listener::UpdateParams,
        _results: asset_hub::listener::UpdateResults,
    ) -> Promise<()> {
        let snapshot = params.get()?.get_snapshot()?;
        self.snapshot.replace(snapshot);
        Promise::ok(())
    }
}

pub fn make_array<A, T>(slice: &[T]) -> A
where
    A: Sized + Default + AsMut<[T]>,
    T: Copy,
{
    let mut a = Default::default();
    <A as AsMut<[T]>>::as_mut(&mut a).copy_from_slice(slice);
    a
}

#[allow(dead_code)]
fn endpoint() -> String {
    if cfg!(windows) {
        r"\\.\pipe\atelier-assets".to_string()
    } else {
        r"/tmp/atelier-assets".to_string()
    }
}
struct Context {
    snapshot: Rc<RefCell<Snapshot>>,
}
fn print_asset_metadata(
    io: &mut shrust::ShellIO,
    asset: &data::asset_metadata::Reader,
) -> io::Result<()> {
    write!(
        io,
        "{{ id: {:?}",
        uuid::Uuid::from_bytes(make_array(asset.get_id().unwrap().get_id().unwrap()))
    )?;
    if let Ok(tags) = asset.get_search_tags() {
        let tags: Vec<String> = tags
            .iter()
            .map(|t| {
                format!(
                    "\"{}\": \"{}\"",
                    std::str::from_utf8(t.get_key().unwrap_or(b"")).unwrap(),
                    std::str::from_utf8(t.get_value().unwrap_or(b"")).unwrap()
                )
            })
            .collect();
        if !tags.is_empty() {
            write!(io, ", search_tags: [ {} ]", tags.join(", "))?;
        }
    }
    writeln!(io, "}}")
}
fn register_commands(shell: &mut shrust::Shell<Context>) -> io::Result<()> {
    shell.new_command("show_all", "Get all asset metadata", 0, |io, ctx, _| {
        let request = ctx.snapshot.borrow().get_all_asset_metadata_request();
        let mut io = io.clone();
        let start = Instant::now();
        Box::new(request.send().promise.then(move |result| {
            let total_time = Instant::now().duration_since(start);
            let response = result.unwrap();
            let response = response.get().unwrap();
            let assets = response.get_assets().unwrap();
            for asset in assets {
                let id = asset.get_id().unwrap().get_id().unwrap();
                writeln!(io, "{:?}", uuid::Uuid::from_bytes(make_array(id)))?;
            }
            writeln!(
                io,
                "got {} assets in {}",
                assets.len(),
                total_time.as_secs_f32()
            )?;
            Ok(())
        }))
    });
    shell.new_command("get", "Get asset metadata from uuid", 1, |io, ctx, args| {
        let id = uuid::Uuid::parse_str(args[0]).unwrap();
        let mut request = ctx.snapshot.borrow().get_asset_metadata_request();
        request.get().init_assets(1).get(0).set_id(id.as_bytes());
        let mut io = io.clone();
        let start = Instant::now();
        Box::new(request.send().promise.then(move |result| {
            let total_time = Instant::now().duration_since(start);
            let response = result.unwrap();
            let response = response.get().unwrap();
            let assets = response.get_assets().unwrap();
            for asset in assets {
                print_asset_metadata(&mut io, &asset)?;
            }
            writeln!(
                io,
                "got {} assets in {}",
                assets.len(),
                total_time.as_secs_f32()
            )?;
            Ok(())
        }))
    });
    shell.new_command(
        "build",
        "Get build artifact from uuid",
        1,
        |io, ctx, args| {
            let id = uuid::Uuid::parse_str(args[0]).unwrap();
            let mut request = ctx.snapshot.borrow().get_build_artifacts_request();
            request.get().init_assets(1).get(0).set_id(id.as_bytes());
            let mut io = io.clone();
            let start = Instant::now();
            Box::new(request.send().promise.then(move |result| {
                let total_time = Instant::now().duration_since(start);
                let response = result.unwrap();
                let response = response.get().unwrap();
                let artifacts = response.get_artifacts().unwrap();
                for artifact in artifacts {
                    let asset_uuid =
                        uuid::Uuid::from_slice(artifact.get_asset_id()?.get_id()?).unwrap();
                    writeln!(
                        io,
                        "{{ id: {}, hash: {:?}, length: {} }}",
                        asset_uuid,
                        artifact.get_key()?,
                        artifact.get_data()?.get_data()?.len()
                    )?;
                }
                writeln!(
                    io,
                    "got {} artifacts in {}",
                    artifacts.len(),
                    total_time.as_secs_f32()
                )?;
                Ok(())
            }))
        },
    );
    shell.new_command(
        "path_for_asset",
        "Get path from asset uuid",
        1,
        |io, ctx, args| {
            let id = uuid::Uuid::parse_str(args[0]).unwrap();
            let mut request = ctx.snapshot.borrow().get_path_for_assets_request();
            request.get().init_assets(1).get(0).set_id(id.as_bytes());
            let mut io = io.clone();
            Box::new(request.send().promise.then(move |result| {
                let response = result.unwrap();
                let response = response.get().unwrap();
                for asset in response.get_paths().unwrap() {
                    let asset_uuid = uuid::Uuid::from_slice(asset.get_id()?.get_id()?).unwrap();
                    write!(
                        io,
                        "{{ asset: {}, path: {} }}",
                        asset_uuid,
                        std::str::from_utf8(asset.get_path().unwrap()).unwrap()
                    )?;
                }
                Ok(())
            }))
        },
    );
    shell.new_command(
        "assets_for_path",
        "Get asset metadata from path",
        1,
        |io, ctx, args| {
            let mut request = ctx.snapshot.borrow().get_assets_for_paths_request();
            request.get().init_paths(1).set(0, args[0].as_bytes());
            let snapshot = ctx.snapshot.clone();
            let path_request = request.send().promise.and_then(move |response| {
                let response = response.get().unwrap();
                let asset_uuids_to_get: Vec<_> = response
                    .get_assets()
                    .unwrap()
                    .iter()
                    .flat_map(|a| a.get_assets().unwrap())
                    .collect();
                let mut request = snapshot.borrow().get_asset_metadata_request();
                let mut assets = request.get().init_assets(asset_uuids_to_get.len() as u32);
                for (idx, asset) in asset_uuids_to_get.iter().enumerate() {
                    assets
                        .reborrow()
                        .get(idx as u32)
                        .set_id(asset.get_id().unwrap());
                }
                Ok(request.send().promise)
            });
            let mut io = io.clone();
            Box::new(path_request.flatten().then(move |result| {
                let response = result.unwrap();
                let response = response.get().unwrap();
                for asset in response.get_assets().unwrap() {
                    print_asset_metadata(&mut io, &asset)?;
                }
                Ok(())
            }))
        },
    );
    Ok(())
}

fn start_runtime() {
    use std::net::ToSocketAddrs;
    let addr = "127.0.0.1:9999".to_socket_addrs().unwrap().next().unwrap();
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
        *ReaderOptions::new()
            .nesting_limit(64)
            .traversal_limit_in_words(64 * 1024 * 1024),
    ));

    let mut rpc_system = RpcSystem::new(rpc_network, None);
    let hub: asset_hub::Client = rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);
    let _disconnector = rpc_system.get_disconnector();
    runtime.spawn(rpc_system.map_err(|_| ()));
    let snapshot = Rc::new(RefCell::new({
        let request = hub.get_snapshot_request();
        runtime
            .block_on(request.send().promise)
            .unwrap()
            .get()
            .unwrap()
            .get_snapshot()
            .unwrap()
    }));
    let listener = asset_hub::listener::ToClient::new(ListenerImpl {
        snapshot: snapshot.clone(),
    })
    .into_client::<::capnp_rpc::Server>();
    let mut request = hub.register_listener_request();
    request.get().set_listener(listener);
    runtime.block_on(request.send().promise).unwrap();
    let ctx = Context { snapshot: snapshot };
    let mut shell = shrust::Shell::new(ctx);
    register_commands(&mut shell).expect("Failed to register commands.");

    runtime
        .block_on(
            shell
                .run_loop(
                    tokio_stdin_stdout::stdin(0),
                    tokio_stdin_stdout::stdout(64000),
                )
                .map_err(|e| {
                    if let shrust::ExecError::Quit = e {
                        ()
                    } else {
                        panic!("error in cmd loop {}", e)
                    }
                }),
        )
        .expect("Failed to register runtime block");
}

pub fn main() {
    // use parity_tokio_ipc::IpcConnection;
    start_runtime();
}
