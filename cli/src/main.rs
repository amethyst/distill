use atelier_schema::{
    data,
    service::asset_hub::{self, snapshot::Client as Snapshot},
};
use capnp_rpc::{pry, rpc_twoparty_capnp, twoparty, RpcSystem};

use capnp::message::ReaderOptions;

use futures::AsyncReadExt;
use std::{cell::RefCell, rc::Rc, time::Instant};
use tokio::prelude::*;
use tokio::runtime::Runtime;
use async_trait::async_trait;

#[macro_use]
mod macros;

mod shell;
use shell::{Shell, Command};

type Promise<T> = capnp::capability::Promise<T, capnp::Error>;
pub type DynResult = Result<(), Box<dyn std::error::Error>>;

struct ListenerImpl {
    snapshot: Rc<RefCell<Snapshot>>,
}
impl asset_hub::listener::Server for ListenerImpl {
    fn update(
        &mut self,
        params: asset_hub::listener::UpdateParams,
        _results: asset_hub::listener::UpdateResults,
    ) -> Promise<()> {
        let snapshot = pry!(pry!(params.get()).get_snapshot());
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
pub struct Context {
    snapshot: Rc<RefCell<Snapshot>>,
}

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut runtime = Runtime::new().unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async_main()))
}

async fn async_main() -> Result<(), Box<dyn std::error::Error>> {
    use std::net::ToSocketAddrs;
    let addr = "127.0.0.1:9999".to_socket_addrs()?.next().unwrap();
    let stream = tokio::net::TcpStream::connect(&addr).await?;
    stream.set_nodelay(true).unwrap();
    let (reader, writer) = futures_tokio_compat::Compat::new(stream).split();
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
    tokio::task::spawn_local(rpc_system);
    let snapshot = Rc::new(RefCell::new({
        let request = hub.get_snapshot_request();
        request.send().promise.await?.get()?.get_snapshot()?
    }));
    let listener = asset_hub::listener::ToClient::new(ListenerImpl {
        snapshot: snapshot.clone(),
    })
    .into_client::<::capnp_rpc::Server>();
    let mut request = hub.register_listener_request();
    request.get().set_listener(listener);

    request.send().promise.await?;
    let ctx = Context { snapshot: snapshot };

    let mut shell = Shell::new();

    shell.register_command("show_all", CmdShowAll);
    shell.register_command("get", CmdGet);
    shell.register_command("build", CmdBuild);
    shell.register_command("path_for_asset", CmdPathForAsset);
    shell.register_command("assets_for_path", CmdAssetsForPath);
    
    shell.run_repl(ctx).await
}

struct CmdShowAll;
#[async_trait(?Send)]
impl Command<Context> for CmdShowAll {
    fn desc(&self) -> &str { "- Get all asset metadata" }
    async fn run(&self, ctx: &mut Context, stdout: &mut io::Stdout, _args: Vec<&str>) -> DynResult {
        let start = Instant::now();
        let request = ctx.snapshot.borrow().get_all_asset_metadata_request();
        let response = request.send().promise.await?;
        let response = response.get()?;
        let total_time = Instant::now().duration_since(start);
        let assets = response.get_assets()?;
        for asset in assets {
            let id = asset.get_id().unwrap().get_id().unwrap();
            stdout.write_all(format!("{:?}\n", uuid::Uuid::from_bytes(make_array(id))).as_bytes()).await?;
        }
        async_write!(
            stdout,
            "got {} assets in {}\n",
            assets.len(),
            total_time.as_secs_f32(),
        ).await?;
        Ok(())
    }
}

struct CmdGet;
#[async_trait(?Send)]
impl Command<Context> for CmdGet {
    fn desc(&self) -> &str { "<uuid> - Get asset metadata from uuid" }
    fn nargs(&self) -> usize { 1 }
    async fn run(&self, ctx: &mut Context, stdout: &mut io::Stdout, args: Vec<&str>) -> DynResult {
        let id = uuid::Uuid::parse_str(args[0])?;
        let mut request = ctx.snapshot.borrow().get_asset_metadata_request();
        request.get().init_assets(1).get(0).set_id(id.as_bytes());
        let start = Instant::now();
        let response = request.send().promise.await?;
        let total_time = Instant::now().duration_since(start);
        let response = response.get()?;
        let assets = response.get_assets()?;
        for asset in assets {
            print_asset_metadata(stdout, &asset).await?;
        }
        async_write!(
            stdout,
            "got {} assets in {}\n",
            assets.len(),
            total_time.as_secs_f32(),
        ).await?;
        Ok(())
    }
}

async fn print_asset_metadata(
    stdout: &mut io::Stdout,
    asset: &data::asset_metadata::Reader<'_>,
) -> DynResult {
    async_write!(
        stdout,
        "{{ id: {:?}",
        uuid::Uuid::from_bytes(make_array(asset.get_id().unwrap().get_id().unwrap())),
    ).await?;

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
            async_write!(stdout, ", search_tags: [ {} ]", tags.join(", ")).await?;
        }
    }
    stdout.write_all(b" }}\n").await?;
    Ok(())
}

struct CmdBuild;
#[async_trait(?Send)]
impl Command<Context> for CmdBuild {
    fn desc(&self) -> &str { "<uuid> - Get build artifact from uuid" }
    fn nargs(&self) -> usize { 1 }
    async fn run(&self, ctx: &mut Context, stdout: &mut io::Stdout, args: Vec<&str>) -> DynResult {
        let id = uuid::Uuid::parse_str(args[0])?;
        let mut request = ctx.snapshot.borrow().get_import_artifacts_request();
        request.get().init_assets(1).get(0).set_id(id.as_bytes());
        let start = Instant::now();
        let response = request.send().promise.await?;
        let total_time = Instant::now().duration_since(start);
        let response = response.get()?;
        let artifacts = response.get_artifacts()?;
        for artifact in artifacts {
            let asset_uuid =
                uuid::Uuid::from_slice(artifact.get_metadata()?.get_asset_id()?.get_id()?)?;
            async_write!(
                stdout,
                "{{ id: {}, hash: {:?}, length: {} }}\n",
                asset_uuid,
                artifact.get_metadata()?.get_hash()?,
                artifact.get_data()?.len()
            ).await?;
        }
        async_write!(
            stdout,
            "got {} artifacts in {}\n",
            artifacts.len(),
            total_time.as_secs_f32(),
        ).await?;
        Ok(())
    }
}

struct CmdPathForAsset;
#[async_trait(?Send)]
impl Command<Context> for CmdPathForAsset {
    fn desc(&self) -> &str { "<uuid> - Get path from asset uuid" }
    fn nargs(&self) -> usize { 1 }
    async fn run(&self, ctx: &mut Context, stdout: &mut io::Stdout, args: Vec<&str>) -> DynResult {
        let id = uuid::Uuid::parse_str(args[0])?;
        let mut request = ctx.snapshot.borrow().get_path_for_assets_request();
        request.get().init_assets(1).get(0).set_id(id.as_bytes());
        let start = Instant::now();
        let response = request.send().promise.await?;
        let total_time = Instant::now().duration_since(start);
        let response = response.get()?;
        let asset_paths = response.get_paths()?;
        for asset_path in asset_paths {
            let asset_uuid = uuid::Uuid::from_slice(asset_path.get_id()?.get_id()?)?;
            async_write!(
                stdout,
                "{{ asset: {}, path: {} }}\n",
                asset_uuid,
                std::str::from_utf8(asset_path.get_path()?)?
            ).await?;
        }
        async_write!(
            stdout,
            "got {} asset paths in {}\n",
            asset_paths.len(),
            total_time.as_secs_f32(),
        ).await?;
        Ok(())
    }
}

struct CmdAssetsForPath;
#[async_trait(?Send)]
impl Command<Context> for CmdAssetsForPath {
    fn desc(&self) -> &str { "<path> - Get asset metadata from path" }
    fn nargs(&self) -> usize { 1 }
    async fn run(&self, ctx: &mut Context, stdout: &mut io::Stdout, args: Vec<&str>) -> DynResult {
        let mut request = ctx.snapshot.borrow().get_assets_for_paths_request();
        request.get().init_paths(1).set(0, args[0].as_bytes());
        let start = Instant::now();
        let response = request.send().promise.await?;
        let response = response.get()?;
        let asset_uuids_to_get: Vec<_> = response
            .get_assets()?
            .iter()
            .flat_map(|a| a.get_assets().unwrap())
            .collect();
        
        let mut request = ctx.snapshot.borrow().get_asset_metadata_request();
        let mut assets = request.get().init_assets(asset_uuids_to_get.len() as u32);
        for (idx, asset) in asset_uuids_to_get.iter().enumerate() {
            assets
                .reborrow()
                .get(idx as u32)
                .set_id(asset.get_id().unwrap());
        }
        let response = request.send().promise.await?;
        let response = response.get()?;
        let total_time = Instant::now().duration_since(start);

        let assets = response.get_assets()?;
        for asset in assets {
            print_asset_metadata(stdout, &asset).await?;
        }
        async_write!(
            stdout,
            "got {} assets in {}\n",
            assets.len(),
            total_time.as_secs_f32(),
        ).await?;
        Ok(())
    }
}
