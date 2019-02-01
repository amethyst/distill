use crate::{
    asset_hub::AssetHub,
    capnp_db::{CapnpCursor, Environment, RoTransaction},
    error::Error,
    utils,
};
use capnp::{self};
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{Future, Stream};
use importer::AssetUUID;
use owning_ref::OwningHandle;
use schema::{
    data::{imported_metadata, asset_change_log_entry},
    service::{self, asset_hub},
};
use std::{
    collections::{HashMap, HashSet},
    error, fmt,
    rc::Rc,
    sync::Arc,
    thread,
};
use tokio::prelude::*;
use tokio::runtime::current_thread::Runtime;
use uuid;

// crate::Error has `impl From<crate::Error> for capnp::Error`
type Promise<T> = capnp::capability::Promise<T, capnp::Error>;

struct ServiceContext {
    hub: Arc<AssetHub>,
    db: Arc<Environment>,
}

pub struct AssetHubService {
    ctx: Arc<ServiceContext>,
}

// RPC interface implementations

struct AssetHubSnapshotImpl<'a> {
    txn: Rc<OwningHandle<Arc<ServiceContext>, Rc<RoTransaction<'a>>>>,
}

struct AssetHubImpl {
    ctx: Arc<ServiceContext>,
}

impl<'a> asset_hub::snapshot::Server for AssetHubSnapshotImpl<'a> {
    fn get_asset_metadata(
        &mut self,
        params: asset_hub::snapshot::GetAssetMetadataParams,
        mut results: asset_hub::snapshot::GetAssetMetadataResults,
    ) -> Promise<()> {
        let params = params.get()?;
        let ctx = self.txn.as_owner();
        let txn = &**self.txn;
        let mut metadatas = Vec::new();
        for id in params.get_assets()? {
            let id = AssetUUID::from_slice(id.get_id()?).map_err(Error::UuidBytesError)?;
            let value = ctx.hub.get_metadata(txn, &id)?;
            if let Some(metadata) = value {
                metadatas.push(metadata);
            }
        }
        let mut results_builder = results.get();
        let assets = results_builder
            .reborrow()
            .init_assets(metadatas.len() as u32);
        for (idx, metadata) in metadatas.iter().enumerate() {
            let metadata = metadata.get()?;
            assets.set_with_caveats(idx as u32, metadata.get_metadata()?)?;
        }
        Promise::ok(())
    }
    fn get_asset_metadata_with_dependencies(
        &mut self,
        params: asset_hub::snapshot::GetAssetMetadataWithDependenciesParams,
        mut results: asset_hub::snapshot::GetAssetMetadataWithDependenciesResults,
    ) -> Promise<()> {
        let params = params.get()?;
        let ctx = self.txn.as_owner();
        let txn = &**self.txn;
        let mut metadatas = HashMap::new();
        for id in params.get_assets()? {
            let id = AssetUUID::from_slice(id.get_id()?).map_err(Error::UuidBytesError)?;
            let value = ctx.hub.get_metadata(txn, &id)?;
            if let Some(metadata) = value {
                metadatas.insert(id, metadata);
            }
        }
        let mut missing_metadata = HashSet::new();
        for metadata in metadatas.values() {
            for dep in metadata.get()?.get_metadata()?.get_load_deps()? {
                let dep = AssetUUID::from_slice(dep.get_id()?).map_err(Error::UuidBytesError)?;
                if !metadatas.contains_key(&dep) {
                    missing_metadata.insert(dep);
                }
            }
        }
        for id in missing_metadata {
            let value = ctx.hub.get_metadata(txn, &id)?;
            if let Some(metadata) = value {
                metadatas.insert(id, metadata);
            }
        }
        let mut results_builder = results.get();
        let assets = results_builder
            .reborrow()
            .init_assets(metadatas.len() as u32);
        for (idx, metadata) in metadatas.values().enumerate() {
            let metadata = metadata.get()?;
            assets.set_with_caveats(idx as u32, metadata.get_metadata()?)?;
        }
        Promise::ok(())
    }
    fn get_all_asset_metadata(
        &mut self,
        _params: asset_hub::snapshot::GetAllAssetMetadataParams,
        mut results: asset_hub::snapshot::GetAllAssetMetadataResults,
    ) -> Promise<()> {
        let ctx = self.txn.as_owner();
        let txn = &**self.txn;
        let mut metadatas = Vec::new();
        for (_, value) in ctx.hub.get_metadata_iter(txn)?.capnp_iter_start() {
            let value = value?;
            let metadata = value.into_typed::<imported_metadata::Owned>();
            metadatas.push(metadata);
        }
        let mut results_builder = results.get();
        let assets = results_builder
            .reborrow()
            .init_assets(metadatas.len() as u32);
        for (idx, metadata) in metadatas.iter().enumerate() {
            let metadata = metadata.get()?;
            assets.set_with_caveats(idx as u32, metadata.get_metadata()?)?;
        }
        Promise::ok(())
    }
    fn get_build_artifacts(
        &mut self,
        _params: asset_hub::snapshot::GetBuildArtifactsParams,
        mut results: asset_hub::snapshot::GetBuildArtifactsResults,
    ) -> Promise<()> {
        let ctx = self.txn.as_owner();
        let txn = &**self.txn;
        Promise::ok(())
    }
    fn get_latest_asset_change(
        &mut self,
        _params: asset_hub::snapshot::GetLatestAssetChangeParams,
        mut results: asset_hub::snapshot::GetLatestAssetChangeResults,
    ) -> Promise<()> {
        let ctx = self.txn.as_owner();
        let txn = &**self.txn;
        let change_num = ctx.hub.get_latest_asset_change(txn)?;
        results.get().set_num(change_num);
        Promise::ok(())
    }
    fn get_asset_changes(
        &mut self,
        params: asset_hub::snapshot::GetAssetChangesParams,
        mut results: asset_hub::snapshot::GetAssetChangesResults,
    ) -> Promise<()> {
        let params = params.get()?;
        let ctx = self.txn.as_owner();
        let txn = &**self.txn;
        let mut changes = Vec::new();
        let iter = ctx.hub.get_asset_changes_iter(txn)?.capnp_iter_from(&params.get_start().to_le_bytes());
        let mut count = params.get_count() as usize;
        if count == 0 {
            count = std::usize::MAX;
        }
        for (_, value) in iter.take(count) {
            let value = value?;
            let change = value.into_typed::<asset_change_log_entry::Owned>();
            changes.push(change);
        }
        let mut results_builder = results.get();
        let changes_results = results_builder
            .reborrow()
            .init_changes(changes.len() as u32);
        for (idx, change) in changes.iter().enumerate() {
            let change = change.get()?;
            changes_results.set_with_caveats(idx as u32, change)?;
        }
        Promise::ok(())
    }
}

impl asset_hub::Server for AssetHubImpl {
    fn register_listener(
        &mut self,
        _params: asset_hub::RegisterListenerParams,
        _results: asset_hub::RegisterListenerResults,
    ) -> Promise<()> {
        Promise::ok(())
    }
    fn get_snapshot(
        &mut self,
        _params: asset_hub::GetSnapshotParams,
        mut results: asset_hub::GetSnapshotResults,
    ) -> Promise<()> {
        let ctx = self.ctx.clone();
        let snapshot = AssetHubSnapshotImpl {
            txn: Rc::new(OwningHandle::new_with_fn(ctx, |t| unsafe {
                Rc::new((*t).db.ro_txn().unwrap())
            })),
        };
        results.get().set_snapshot(
            asset_hub::snapshot::ToClient::new(snapshot).into_client::<::capnp_rpc::Server>(),
        );
        Promise::ok(())
    }
}

fn endpoint() -> String {
    if cfg!(windows) {
        r"\\.\pipe\atelier-assets".to_string()
    } else {
        r"/tmp/atelier-assets".to_string()
    }
}
fn spawn_rpc<R: std::io::Read + Send + 'static, W: std::io::Write + Send + 'static>(
    reader: R,
    writer: W,
    ctx: Arc<ServiceContext>,
) {
    thread::spawn(move || {
        let service_impl = AssetHubImpl { ctx: ctx };
        let hub_impl = asset_hub::ToClient::new(service_impl).into_client::<::capnp_rpc::Server>();
        let mut runtime = Runtime::new().unwrap();

        let network = twoparty::VatNetwork::new(
            reader,
            writer,
            rpc_twoparty_capnp::Side::Server,
            Default::default(),
        );

        let rpc_system = RpcSystem::new(Box::new(network), Some(hub_impl.clone().client));
        runtime.block_on(rpc_system.map_err(|_| ())).unwrap();
    });
}
impl AssetHubService {
    pub fn new(db: Arc<Environment>, hub: Arc<AssetHub>) -> AssetHubService {
        AssetHubService {
            ctx: Arc::new(ServiceContext { hub, db }),
        }
    }
    pub fn run(&self, addr: std::net::SocketAddr) -> Result<(), Error> {
        use parity_tokio_ipc::Endpoint;

        let mut runtime = Runtime::new().unwrap();

        let tcp = ::tokio::net::TcpListener::bind(&addr)?;
        let tcp_future = tcp.incoming().for_each(move |stream| {
            stream.set_nodelay(true)?;
            stream.set_send_buffer_size(1 << 24)?;
            stream.set_recv_buffer_size(1 << 24)?;
            let (reader, writer) = stream.split();
            spawn_rpc(reader, writer, self.ctx.clone());
            Ok(())
        });

        let ipc = Endpoint::new(endpoint());

        let ipc_future = ipc
            .incoming(&tokio::reactor::Handle::default())
            .expect("failed to listen for incoming IPC connections")
            .for_each(move |(stream, _id)| {
                let (reader, writer) = stream.split();
                spawn_rpc(reader, writer, self.ctx.clone());
                Ok(())
            });

        runtime.block_on(tcp_future.join(ipc_future))?;
        Ok(())
    }
}
