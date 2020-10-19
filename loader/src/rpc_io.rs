use atelier_core::{utils, ArtifactMetadata, AssetMetadata, AssetUuid};
use atelier_schema::{data::asset_change_event, parse_db_metadata, service::asset_hub};
use capnp::message::ReaderOptions;
use capnp_rpc::{pry, rpc_twoparty_capnp, twoparty, RpcSystem};
use crossbeam_channel::{unbounded, Receiver, Sender};
use futures_channel::oneshot;
use futures_util::AsyncReadExt;
use std::sync::Mutex;
use std::{error::Error, path::PathBuf};
use tokio::runtime::{Builder, Runtime};

use crate::io::{DataRequest, LoaderIO, MetadataRequest, ResolveRequest};
use crate::loader::LoaderState;

type Promise<T> = capnp::capability::Promise<T, capnp::Error>;

struct RpcConnection {
    _asset_hub: asset_hub::Client,
    snapshot: asset_hub::snapshot::Client,
    snapshot_rx: Receiver<SnapshotChange>,
}
enum InternalConnectionState {
    None,
    Connecting(oneshot::Receiver<Result<RpcConnection, Box<dyn Error>>>),
    Connected(RpcConnection),
    Error(Box<dyn Error>),
}

struct SnapshotChange {
    snapshot: asset_hub::snapshot::Client,
    changed_assets: Vec<AssetUuid>,
    deleted_assets: Vec<AssetUuid>,
}

// While capnp_rpc does not impl Send or Sync, in our usage of the API there can only be one thread
// accessing the internal state at any time due to Mutex. The !Send constraint in capnp_rpc is because
// of internal object lifetime management that is unsafe in the face of multiple threads accessing data
// from separate objects.
unsafe impl Send for RpcRuntime {}
struct RpcRuntime {
    runtime: Runtime,
    local: tokio::task::LocalSet,
    connection: InternalConnectionState,
}

#[derive(Default)]
struct QueuedRequests {
    data_requests: Vec<DataRequest>,
    metadata_requests: Vec<MetadataRequest>,
    resolve_requests: Vec<ResolveRequest>,
}

pub struct RpcIO {
    connect_string: String,
    runtime: Mutex<RpcRuntime>,
    requests: QueuedRequests,
}
pub fn default_connect_string() -> &'static str {
    "127.0.0.1:9999"
}

impl Default for RpcIO {
    fn default() -> RpcIO {
        RpcIO::new(default_connect_string().to_string()).unwrap()
    }
}

impl RpcIO {
    pub fn new(connect_string: String) -> std::io::Result<RpcIO> {
        Ok(RpcIO {
            connect_string,
            runtime: Mutex::new(RpcRuntime {
                runtime: Builder::new().basic_scheduler().enable_all().build()?,
                local: tokio::task::LocalSet::new(),
                connection: InternalConnectionState::None,
            }),
            requests: Default::default(),
        })
    }
}
impl RpcRuntime {
    fn check_asset_changes(&mut self, loader: &LoaderState) {
        self.connection =
            match std::mem::replace(&mut self.connection, InternalConnectionState::None) {
                InternalConnectionState::Connected(mut conn) => {
                    if let Ok(change) = conn.snapshot_rx.try_recv() {
                        conn.snapshot = change.snapshot;
                        let mut changed_assets = Vec::new();
                        for asset in change.changed_assets {
                            changed_assets.push(asset);
                        }
                        for asset in change.deleted_assets {
                            changed_assets.push(asset);
                        }
                        loader.invalidate_assets(&changed_assets);
                    }
                    InternalConnectionState::Connected(conn)
                }
                c => c,
            };
    }

    fn connect(&mut self, connect_string: &str) {
        match self.connection {
            InternalConnectionState::Connected(_) | InternalConnectionState::Connecting(_) => {
                panic!("Trying to connect while already connected or connecting")
            }
            _ => {}
        };
        use std::net::ToSocketAddrs;
        let addr = connect_string.to_socket_addrs().unwrap().next().unwrap();
        let (conn_tx, conn_rx) = oneshot::channel();
        self.local.spawn_local(async move {
            let result = async move {
                let stream = ::tokio::net::TcpStream::connect(&addr)
                    .await
                    .map_err(|e| -> Box<dyn Error> { Box::new(e) })?;
                stream.set_nodelay(true)?;
                use tokio_util::compat::*;
                let (reader, writer) = stream.compat().split();
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
                let request = hub.get_snapshot_request();
                let response = request
                    .send()
                    .promise
                    .await
                    .map_err(|e| -> Box<dyn Error> { Box::new(e) })?;

                let snapshot = response.get()?.get_snapshot()?;
                let (snapshot_tx, snapshot_rx) = unbounded();
                let listener: asset_hub::listener::Client = capnp_rpc::new_client(ListenerImpl {
                    snapshot_channel: snapshot_tx,
                    snapshot_change: None,
                });
                let mut request = hub.register_listener_request();
                request.get().set_listener(listener);
                let rpc_conn = request
                    .send()
                    .promise
                    .await
                    .map(|_| RpcConnection {
                        _asset_hub: hub,
                        snapshot,
                        snapshot_rx,
                    })
                    .map_err(|e| -> Box<dyn Error> { Box::new(e) })?;
                Ok(rpc_conn)
            }
            .await;
            let _ = conn_tx.send(result);
        });
        self.connection = InternalConnectionState::Connecting(conn_rx)
    }
}

async fn do_metadata_request(
    asset: &MetadataRequest,
    snapshot: &asset_hub::snapshot::Client,
) -> Result<Vec<ArtifactMetadata>, capnp::Error> {
    let mut request = snapshot.get_asset_metadata_with_dependencies_request();
    let mut assets = request
        .get()
        .init_assets(asset.requested_assets().count() as u32);
    for (idx, asset) in asset.requested_assets().enumerate() {
        assets.reborrow().get(idx as u32).set_id(&asset.0);
    }
    let response = request.send().promise.await?;
    let reader = response.get()?;
    let artifacts = reader
        .get_assets()?
        .into_iter()
        .map(|a| parse_db_metadata(&a))
        .filter_map(|a| a.artifact)
        .collect::<Vec<_>>();
    Ok(artifacts)
}

async fn do_import_artifact_request(
    asset: &DataRequest,
    snapshot: &asset_hub::snapshot::Client,
) -> Result<Vec<u8>, capnp::Error> {
    let mut request = snapshot.get_import_artifacts_request();
    let mut assets = request.get().init_assets(1);
    assets.reborrow().get(0).set_id(&asset.asset_id().0);
    let response = request.send().promise.await?;
    let reader = response.get()?;
    let artifact = reader.get_artifacts()?.get(0);
    Ok(Vec::from(artifact.get_data()?))
}

async fn do_resolve_request(
    resolve: &ResolveRequest,
    snapshot: &asset_hub::snapshot::Client,
) -> Result<Vec<(PathBuf, Vec<AssetMetadata>)>, capnp::Error> {
    let path = resolve.identifier().path();
    // get asset IDs at path
    let mut request = snapshot.get_assets_for_paths_request();
    let mut paths = request.get().init_paths(1);
    paths.reborrow().set(0, path.as_bytes());
    let response = request.send().promise.await?;
    let reader = response.get()?;
    let mut results = Vec::new();
    for reader in reader.get_assets()? {
        let path = PathBuf::from(std::str::from_utf8(reader.get_path()?)?);
        let asset_ids = reader.get_assets()?;
        // get metadata for the assetIDs
        let mut request = snapshot.get_asset_metadata_request();
        request.get().set_assets(asset_ids)?;
        let response = request.send().promise.await?;
        let reader = response.get()?;
        results.push((
            path,
            reader
                .get_assets()?
                .into_iter()
                .map(|a| parse_db_metadata(&a))
                .collect::<Vec<_>>(),
        ));
    }
    Ok(results)
}

fn process_requests(runtime: &mut RpcRuntime, requests: &mut QueuedRequests) {
    if let InternalConnectionState::Connected(connection) = &runtime.connection {
        let len = requests.data_requests.len();
        for asset in requests.data_requests.drain(0..len) {
            let snapshot = connection.snapshot.clone();
            runtime.local.spawn_local(async move {
                match do_import_artifact_request(&asset, &snapshot).await {
                    Ok(data) => {
                        asset.complete(data);
                    }
                    Err(e) => {
                        asset.error(e);
                    }
                }
            });
        }

        let len = requests.metadata_requests.len();
        for m in requests.metadata_requests.drain(0..len) {
            let snapshot = connection.snapshot.clone();
            runtime.local.spawn_local(async move {
                match do_metadata_request(&m, &snapshot).await {
                    Ok(data) => {
                        m.complete(data);
                    }
                    Err(e) => {
                        m.error(e);
                    }
                }
            });
        }

        let len = requests.resolve_requests.len();
        for m in requests.resolve_requests.drain(0..len) {
            let snapshot = connection.snapshot.clone();
            runtime.local.spawn_local(async move {
                match do_resolve_request(&m, &snapshot).await {
                    Ok(data) => {
                        m.complete(data);
                    }
                    Err(e) => {
                        m.error(e);
                    }
                }
            });
        }
    }
}

impl LoaderIO for RpcIO {
    fn get_asset_metadata_with_dependencies(&mut self, request: MetadataRequest) {
        self.requests.metadata_requests.push(request);
        let mut runtime = self.runtime.lock().unwrap();
        process_requests(&mut runtime, &mut self.requests);
    }

    fn get_asset_candidates(&mut self, requests: Vec<ResolveRequest>) {
        self.requests.resolve_requests.extend(requests);
        let mut runtime = self.runtime.lock().unwrap();
        process_requests(&mut runtime, &mut self.requests);
    }

    fn get_artifacts(&mut self, requests: Vec<DataRequest>) {
        self.requests.data_requests.extend(requests);
        let mut runtime = self.runtime.lock().unwrap();
        process_requests(&mut runtime, &mut self.requests);
    }

    fn tick(&mut self, loader: &mut LoaderState) {
        let mut runtime = self.runtime.lock().unwrap();
        match &runtime.connection {
            InternalConnectionState::Error(err) => {
                log::error!("Error connecting RpcIO: {}", err);
                runtime.connect(&self.connect_string);
            }
            InternalConnectionState::None => {
                runtime.connect(&self.connect_string);
            }
            _ => {}
        };
        process_requests(&mut runtime, &mut self.requests);
        runtime.connection =
            match std::mem::replace(&mut runtime.connection, InternalConnectionState::None) {
                // update connection state
                InternalConnectionState::Connecting(mut pending_connection) => {
                    match pending_connection.try_recv() {
                        Ok(connection_result) => match connection_result {
                            Some(value) => match value {
                                Ok(conn) => InternalConnectionState::Connected(conn),
                                Err(err) => InternalConnectionState::Error(err),
                            },
                            None => {
                                // still waiting
                                InternalConnectionState::Connecting(pending_connection)
                            }
                        },
                        // Sender was closed
                        Err(e) => InternalConnectionState::Error(Box::new(e)),
                    }
                }
                c => c,
            };
        {
            let RpcRuntime {
                ref mut runtime,
                ref mut local,
                ..
            } = &mut *runtime;
            // tick the tokio runtime
            local.block_on(runtime, async {
                tokio::task::yield_now().await;
            });
        }
        runtime.check_asset_changes(loader);
    }

    fn with_runtime(&self, f: &mut dyn FnMut(&mut Runtime)) {
        let mut runtime = self.runtime.lock().unwrap();
        f(&mut runtime.runtime)
    }
}

struct ListenerImpl {
    snapshot_channel: Sender<SnapshotChange>,
    snapshot_change: Option<u64>,
}
impl asset_hub::listener::Server for ListenerImpl {
    fn update(
        &mut self,
        params: asset_hub::listener::UpdateParams,
        _results: asset_hub::listener::UpdateResults,
    ) -> Promise<()> {
        let params = pry!(params.get());
        let snapshot = pry!(params.get_snapshot());
        if let Some(change_num) = self.snapshot_change {
            let channel = self.snapshot_channel.clone();
            let mut request = snapshot.get_asset_changes_request();
            request.get().set_start(change_num);
            request
                .get()
                .set_count(params.get_latest_change() - change_num);
            return Promise::from_future(async move {
                let response = request.send().promise.await?;
                let response = response.get()?;
                let mut changed_assets = Vec::new();
                let mut deleted_assets = Vec::new();
                for change in response.get_changes()? {
                    match change.get_event()?.which()? {
                        asset_change_event::ContentUpdateEvent(evt) => {
                            let evt = evt?;
                            let id = utils::make_array(evt.get_id()?.get_id()?);
                            changed_assets.push(id);
                        }
                        asset_change_event::RemoveEvent(evt) => {
                            let id = utils::make_array(evt?.get_id()?.get_id()?);
                            deleted_assets.push(id);
                        }
                    }
                }
                let _ = channel.send(SnapshotChange {
                    snapshot,
                    changed_assets,
                    deleted_assets,
                });
                Ok(())
            });
        } else {
            let _ = self.snapshot_channel.try_send(SnapshotChange {
                snapshot,
                changed_assets: Vec::new(),
                deleted_assets: Vec::new(),
            });
        }
        self.snapshot_change = Some(params.get_latest_change());
        Promise::ok(())
    }
}
