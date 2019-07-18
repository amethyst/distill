use crate::{
    asset_hub::{AssetBatchEvent, AssetHub},
    capnp_db::{CapnpCursor, Environment, RoTransaction},
    error::Error,
    file_asset_source::FileAssetSource,
    file_tracker::FileTracker,
    serialized_asset::SerializedAsset,
    utils,
};
use atelier_schema::{
    data::{asset_change_log_entry, asset_metadata, serialized_asset, AssetSource},
    service::asset_hub,
};
use capnp;
use capnp_rpc::{pry, rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{sync::mpsc, Future, Stream};
use owning_ref::OwningHandle;
use std::{
    collections::{HashMap, HashSet},
    fs, path,
    rc::Rc,
    sync::Arc,
    thread,
};
use tokio::prelude::*;
use tokio::runtime::current_thread::Runtime;

// crate::Error has `impl From<crate::Error> for capnp::Error`
type Promise<T> = capnp::capability::Promise<T, capnp::Error>;
type Result<T> = std::result::Result<T, capnp::Error>;

struct ServiceContext {
    hub: Arc<AssetHub>,
    file_source: Arc<FileAssetSource>,
    file_tracker: Arc<FileTracker>,
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
fn build_serialized_asset_message<T: AsRef<[u8]>>(
    artifact: &SerializedAsset<T>,
) -> capnp::message::Builder<capnp::message::HeapAllocator> {
    let mut value_builder = capnp::message::Builder::new_default();
    {
        let mut m = value_builder.init_root::<serialized_asset::Builder<'_>>();
        m.reborrow().set_compression(artifact.compression);
        m.reborrow()
            .set_uncompressed_size(artifact.uncompressed_size as u64);
        m.reborrow().set_type_uuid(&artifact.type_uuid);
        let slice: &[u8] = artifact.data.as_ref();
        m.reborrow().set_data(slice);
    }
    value_builder
}

impl<'a> AssetHubSnapshotImpl<'a> {
    fn get_asset_metadata(
        &mut self,
        params: asset_hub::snapshot::GetAssetMetadataParams,
        mut results: asset_hub::snapshot::GetAssetMetadataResults,
    ) -> Result<()> {
        let params = params.get()?;
        let ctx = self.txn.as_owner();
        let txn = &**self.txn;
        let mut metadatas = Vec::new();
        for id in params.get_assets()? {
            let id = utils::uuid_from_slice(id.get_id()?)?;
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
            assets.set_with_caveats(idx as u32, metadata)?;
        }
        Ok(())
    }
    fn get_asset_metadata_with_dependencies(
        &mut self,
        params: asset_hub::snapshot::GetAssetMetadataWithDependenciesParams,
        mut results: asset_hub::snapshot::GetAssetMetadataWithDependenciesResults,
    ) -> Result<()> {
        let params = params.get()?;
        let ctx = self.txn.as_owner();
        let txn = &**self.txn;
        let mut metadatas = HashMap::new();
        for id in params.get_assets()? {
            let id = utils::uuid_from_slice(id.get_id()?)?;
            let value = ctx.hub.get_metadata(txn, &id)?;
            if let Some(metadata) = value {
                metadatas.insert(id, metadata);
            }
        }
        let mut missing_metadata = HashSet::new();
        for metadata in metadatas.values() {
            for dep in metadata.get()?.get_load_deps()? {
                let dep = utils::uuid_from_slice(dep.get_id()?)?;
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
            assets.set_with_caveats(idx as u32, metadata)?;
        }
        Ok(())
    }
    fn get_all_asset_metadata(
        &mut self,
        _params: asset_hub::snapshot::GetAllAssetMetadataParams,
        mut results: asset_hub::snapshot::GetAllAssetMetadataResults,
    ) -> Result<()> {
        let ctx = self.txn.as_owner();
        let txn = &**self.txn;
        let mut metadatas = Vec::new();
        for (_, value) in ctx.hub.get_metadata_iter(txn)?.capnp_iter_start() {
            let value = value?;
            let metadata = value.into_typed::<asset_metadata::Owned>();
            metadatas.push(metadata);
        }
        let mut results_builder = results.get();
        let assets = results_builder
            .reborrow()
            .init_assets(metadatas.len() as u32);
        for (idx, metadata) in metadatas.iter().enumerate() {
            let metadata = metadata.get()?;
            assets.set_with_caveats(idx as u32, metadata)?;
        }
        Ok(())
    }
    fn get_build_artifacts(
        &mut self,
        params: asset_hub::snapshot::GetBuildArtifactsParams,
        mut results: asset_hub::snapshot::GetBuildArtifactsResults,
    ) -> Result<()> {
        let params = params.get()?;
        let ctx = self.txn.as_owner();
        let txn = &**self.txn;
        let mut artifacts = Vec::new();
        let mut scratch_buf = Vec::new();
        for id in params.get_assets()? {
            let id = utils::uuid_from_slice(id.get_id()?)?;
            let value = ctx.hub.get_metadata(txn, &id)?;
            if let Some(metadata) = value {
                let metadata = metadata.get()?;
                match metadata.get_source()? {
                    AssetSource::File => {
                        // TODO run build pipeline
                        if let Some((hash, artifact)) = ctx.file_source.regenerate_import_artifact(
                            txn,
                            &id,
                            &mut scratch_buf,
                        )? {
                            let capnp_artifact = build_serialized_asset_message(&artifact);
                            artifacts.push((id, hash, capnp_artifact));
                        }
                    }
                }
            }
        }
        let mut results_builder = results.get();
        let mut artifact_results = results_builder
            .reborrow()
            .init_artifacts(artifacts.len() as u32);
        for (idx, (id, hash, artifact)) in artifacts.iter().enumerate() {
            let mut out = artifact_results.reborrow().get(idx as u32);
            out.reborrow().init_asset_id().set_id(id);
            out.reborrow().set_key(&hash.to_le_bytes());
            out.reborrow()
                .set_data(artifact.get_root_as_reader::<serialized_asset::Reader<'_>>()?)?;
        }
        Ok(())
    }
    fn get_latest_asset_change(
        &mut self,
        _params: asset_hub::snapshot::GetLatestAssetChangeParams,
        mut results: asset_hub::snapshot::GetLatestAssetChangeResults,
    ) -> Result<()> {
        let ctx = self.txn.as_owner();
        let txn = &**self.txn;
        let change_num = ctx.hub.get_latest_asset_change(txn)?;
        results.get().set_num(change_num);
        Ok(())
    }
    fn get_asset_changes(
        &mut self,
        params: asset_hub::snapshot::GetAssetChangesParams,
        mut results: asset_hub::snapshot::GetAssetChangesResults,
    ) -> Result<()> {
        let params = params.get()?;
        let ctx = self.txn.as_owner();
        let txn = &**self.txn;
        let mut changes = Vec::new();
        let iter = ctx
            .hub
            .get_asset_changes_iter(txn)?
            .capnp_iter_from(&params.get_start().to_le_bytes());
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
        Ok(())
    }
    fn get_path_for_assets(
        &mut self,
        params: asset_hub::snapshot::GetPathForAssetsParams,
        mut results: asset_hub::snapshot::GetPathForAssetsResults,
    ) -> Result<()> {
        let params = params.get()?;
        let ctx = self.txn.as_owner();
        let txn = &**self.txn;
        let mut asset_paths = Vec::new();
        for id in params.get_assets()? {
            let asset_uuid = utils::uuid_from_slice(id.get_id()?)?;
            let path = ctx.file_source.get_asset_path(txn, &asset_uuid)?;
            if let Some(path) = path {
                asset_paths.push((id, path));
            }
        }
        let mut results_builder = results.get();
        let mut assets = results_builder
            .reborrow()
            .init_paths(asset_paths.len() as u32);
        for (idx, (asset, path)) in asset_paths.iter().enumerate() {
            assets
                .reborrow()
                .get(idx as u32)
                .set_path(path.to_string_lossy().as_bytes());
            assets
                .reborrow()
                .get(idx as u32)
                .init_id()
                .set_id(asset.get_id()?);
        }
        Ok(())
    }
    fn get_assets_for_paths(
        &mut self,
        params: asset_hub::snapshot::GetAssetsForPathsParams,
        mut results: asset_hub::snapshot::GetAssetsForPathsResults,
    ) -> Result<()> {
        let params = params.get()?;
        let ctx = self.txn.as_owner();
        let txn = &**self.txn;
        let mut metadatas = Vec::new();
        for request_path in params.get_paths()? {
            let request_path = request_path?;
            let path_str = if cfg!(windows) {
                // fs::canonicalize does not handle forward slashes in paths on Windows
                std::str::from_utf8(request_path)?
                    .to_string()
                    .replace("/", "\\")
            } else {
                std::str::from_utf8(request_path)?.to_string()
            };
            let path = path::PathBuf::from(path_str);
            let mut metadata = None;
            if path.is_relative() {
                for dir in ctx.file_tracker.get_watch_dirs() {
                    if let Ok(canonicalized) = fs::canonicalize(dir.join(&path)) {
                        metadata = ctx.file_source.get_metadata(txn, &canonicalized)?;
                        if metadata.is_some() {
                            break;
                        }
                    }
                }
            } else if let Ok(canonicalized) = fs::canonicalize(&path) {
                metadata = ctx.file_source.get_metadata(txn, &canonicalized)?
            }
            if let Some(metadata) = metadata {
                metadatas.push((request_path, metadata));
            }
        }
        let mut results_builder = results.get();
        let mut results = results_builder
            .reborrow()
            .init_assets(metadatas.len() as u32);
        for (idx, (path, assets)) in metadatas.iter().enumerate() {
            let assets = assets.get()?.get_assets()?;
            let num_assets = assets.len();
            let mut asset_results = results.reborrow().get(idx as u32).init_assets(num_assets);
            for (idx, asset) in assets.iter().enumerate() {
                asset_results
                    .reborrow()
                    .get(idx as u32)
                    .set_id(asset.get_id()?);
            }
            results.reborrow().get(idx as u32).set_path(path);
        }
        Ok(())
    }
}

impl asset_hub::Server for AssetHubImpl {
    fn register_listener(
        &mut self,
        params: asset_hub::RegisterListenerParams,
        results: asset_hub::RegisterListenerResults,
    ) -> Promise<()> {
        Promise::ok(pry!(AssetHubImpl::register_listener(self, params, results)))
    }
    fn get_snapshot(
        &mut self,
        params: asset_hub::GetSnapshotParams,
        results: asset_hub::GetSnapshotResults,
    ) -> Promise<()> {
        Promise::ok(pry!(AssetHubImpl::get_snapshot(self, params, results)))
    }
}
impl AssetHubImpl {
    fn register_listener(
        &mut self,
        params: asset_hub::RegisterListenerParams,
        _results: asset_hub::RegisterListenerResults,
    ) -> Result<()> {
        let params = params.get()?;
        let listener = Rc::new(params.get_listener()?);
        let ctx = self.ctx.clone();
        let (mut tx, rx) = mpsc::channel(16);
        tx.try_send(AssetBatchEvent::Commit).unwrap();

        let tx = self.ctx.hub.register_listener(tx);
        tokio::runtime::current_thread::TaskExecutor::current()
            .spawn_local(Box::new(rx.for_each(move |_| {
                let mut request = listener.update_request();
                let snapshot = AssetHubSnapshotImpl {
                    txn: Rc::new(OwningHandle::new_with_fn(ctx.clone(), |t| unsafe {
                        Rc::new((*t).db.ro_txn().unwrap())
                    })),
                };
                let latest_change = ctx
                    .hub
                    .get_latest_asset_change(&**snapshot.txn)
                    .expect("failed to get latest change");
                request.get().set_latest_change(latest_change);
                request.get().set_snapshot(
                    asset_hub::snapshot::ToClient::new(snapshot)
                        .into_client::<::capnp_rpc::Server>(),
                );
                let ctx = ctx.clone();
                let _ = tokio::runtime::current_thread::TaskExecutor::current().spawn_local(
                    Box::new(request.send().promise.then(move |r| {
                        match r {
                            Ok(_) => {}
                            Err(_) => {
                                ctx.hub.drop_listener(tx);
                            }
                        }
                        Ok(())
                    })),
                );
                Ok(())
            })))
            .map_err(Error::TokioSpawnError)?;
        Ok(())
    }

    fn get_snapshot(
        &mut self,
        _params: asset_hub::GetSnapshotParams,
        mut results: asset_hub::GetSnapshotResults,
    ) -> Result<()> {
        let ctx = self.ctx.clone();
        let snapshot = AssetHubSnapshotImpl {
            txn: Rc::new(OwningHandle::new_with_fn(ctx, |t| unsafe {
                Rc::new((*t).db.ro_txn().unwrap())
            })),
        };
        results.get().set_snapshot(
            asset_hub::snapshot::ToClient::new(snapshot).into_client::<::capnp_rpc::Server>(),
        );
        Ok(())
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
        let mut runtime = Runtime::new().unwrap();
        let service_impl = AssetHubImpl { ctx: ctx };
        let hub_impl = asset_hub::ToClient::new(service_impl).into_client::<::capnp_rpc::Server>();

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
    pub fn new(
        db: Arc<Environment>,
        hub: Arc<AssetHub>,
        file_source: Arc<FileAssetSource>,
        file_tracker: Arc<FileTracker>,
    ) -> AssetHubService {
        AssetHubService {
            ctx: Arc::new(ServiceContext {
                hub,
                db,
                file_source,
                file_tracker,
            }),
        }
    }
    pub fn run(&self, addr: std::net::SocketAddr) -> Result<()> {
        use parity_tokio_ipc::Endpoint;

        let mut runtime = Runtime::new().unwrap();

        let tcp = ::tokio::net::TcpListener::bind(&addr)?;
        let tcp_future = tcp.incoming().for_each(move |stream| {
            stream.set_nodelay(true)?;
            stream.set_send_buffer_size(1 << 22)?;
            stream.set_recv_buffer_size(1 << 22)?;
            let (reader, writer) = stream.split();
            spawn_rpc(reader, writer, self.ctx.clone());
            Ok(())
        });
        let _ = std::fs::remove_file(endpoint());
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

impl<'a> asset_hub::snapshot::Server for AssetHubSnapshotImpl<'a> {
    fn get_asset_metadata(
        &mut self,
        params: asset_hub::snapshot::GetAssetMetadataParams,
        results: asset_hub::snapshot::GetAssetMetadataResults,
    ) -> Promise<()> {
        Promise::ok(pry!(AssetHubSnapshotImpl::get_asset_metadata(
            self, params, results
        )))
    }
    fn get_asset_metadata_with_dependencies(
        &mut self,
        params: asset_hub::snapshot::GetAssetMetadataWithDependenciesParams,
        results: asset_hub::snapshot::GetAssetMetadataWithDependenciesResults,
    ) -> Promise<()> {
        Promise::ok(pry!(
            AssetHubSnapshotImpl::get_asset_metadata_with_dependencies(self, params, results)
        ))
    }
    fn get_all_asset_metadata(
        &mut self,
        params: asset_hub::snapshot::GetAllAssetMetadataParams,
        results: asset_hub::snapshot::GetAllAssetMetadataResults,
    ) -> Promise<()> {
        Promise::ok(pry!(AssetHubSnapshotImpl::get_all_asset_metadata(
            self, params, results
        )))
    }
    fn get_build_artifacts(
        &mut self,
        params: asset_hub::snapshot::GetBuildArtifactsParams,
        results: asset_hub::snapshot::GetBuildArtifactsResults,
    ) -> Promise<()> {
        Promise::ok(pry!(AssetHubSnapshotImpl::get_build_artifacts(
            self, params, results
        )))
    }
    fn get_latest_asset_change(
        &mut self,
        params: asset_hub::snapshot::GetLatestAssetChangeParams,
        results: asset_hub::snapshot::GetLatestAssetChangeResults,
    ) -> Promise<()> {
        Promise::ok(pry!(AssetHubSnapshotImpl::get_latest_asset_change(
            self, params, results
        )))
    }
    fn get_asset_changes(
        &mut self,
        params: asset_hub::snapshot::GetAssetChangesParams,
        results: asset_hub::snapshot::GetAssetChangesResults,
    ) -> Promise<()> {
        Promise::ok(pry!(AssetHubSnapshotImpl::get_asset_changes(
            self, params, results
        )))
    }
    fn get_path_for_assets(
        &mut self,
        params: asset_hub::snapshot::GetPathForAssetsParams,
        results: asset_hub::snapshot::GetPathForAssetsResults,
    ) -> Promise<()> {
        Promise::ok(pry!(AssetHubSnapshotImpl::get_path_for_assets(
            self, params, results
        )))
    }
    fn get_assets_for_paths(
        &mut self,
        params: asset_hub::snapshot::GetAssetsForPathsParams,
        results: asset_hub::snapshot::GetAssetsForPathsResults,
    ) -> Promise<()> {
        Promise::ok(pry!(AssetHubSnapshotImpl::get_assets_for_paths(
            self, params, results
        )))
    }
}
