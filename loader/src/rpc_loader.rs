use crate::{
    handle::{RefOp, SerdeContext},
    loader::{
        AssetLoadOp, AssetStorage, AtomicHandleAllocator, HandleAllocator, HandleOp, LoadHandle,
        LoadInfo, LoadStatus, Loader, LoaderInfoProvider,
    },
    rpc_state::{ConnectionState, ResponsePromise, RpcState},
};
use atelier_core::{utils::make_array, AssetRef, AssetTypeId, AssetUuid};
use atelier_schema::{
    data::{
        artifact,
        asset_metadata::{self, latest_artifact},
        asset_ref,
    },
    service::asset_hub::{
        snapshot::get_asset_metadata_with_dependencies_results::Owned as GetAssetMetadataWithDependenciesResults,
        snapshot::get_import_artifacts_results::Owned as GetImportArtifactsResults,
    },
};
use crossbeam_channel::{unbounded, Receiver, Sender};
use dashmap::DashMap;
use log::{error, warn};
use std::{
    collections::HashMap,
    error::Error,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::Poll,
};

/// Describes the state of an asset load operation
#[derive(Copy, Clone, PartialEq, Debug)]
enum LoadState {
    /// Indeterminate state - may transition into a load, or result in removal if ref count is < 0
    None,
    /// The load operation needs metadata to progress
    WaitingForMetadata,
    /// Metadata is being fetched for the load operation
    RequestingMetadata,
    /// Dependencies are requested for loading
    RequestDependencies,
    /// Waiting for dependencies to complete loading
    WaitingForDependencies,
    /// Waiting for asset data to be fetched
    WaitingForData,
    /// Asset data is being fetched
    RequestingData,
    /// Engine systems are loading asset
    LoadingAsset,
    /// Engine systems have loaded asset, but the asset is not committed.
    /// This state is only reached when AssetVersionLoad.auto_commit == false.
    LoadedUncommitted,
    /// Asset is loaded and ready to use
    Loaded,
    /// Asset should be unloaded
    UnloadRequested,
    /// Asset is being unloaded by engine systems
    Unloading,
}

#[derive(Debug, Clone)]
struct AssetVersionLoad {
    state: LoadState,
    asset_type: Option<AssetTypeId>,
    auto_commit: bool,
    version: u32,
}
#[derive(Debug)]
struct AssetLoad {
    asset_id: AssetUuid,
    last_state_change_instant: std::time::Instant,
    refs: AtomicUsize,
    versions: Vec<AssetVersionLoad>,
    requested_version: Option<u32>,
    loaded_version: Option<u32>,
    version_counter: u32,
    pending_reload: bool,
}
struct AssetMetadata {
    load_deps: Vec<AssetUuid>,
}

/// Keeps track of a pending reload
struct PendingReload {
    /// ID of asset that should be reloaded
    asset_id: AssetUuid,
    /// The version of the asset before it was reloaded
    version_before: u32,
}

struct LoaderData {
    handle_allocator: Arc<dyn HandleAllocator>,
    load_states: DashMap<LoadHandle, AssetLoad>,
    uuid_to_load: DashMap<AssetUuid, LoadHandle>,
    metadata: HashMap<AssetUuid, AssetMetadata>,
    op_tx: Arc<Sender<HandleOp>>,
    op_rx: Receiver<HandleOp>,
    pending_reloads: Vec<PendingReload>,
}

struct PendingDataRequest {
    handle: LoadHandle,
    version: u32,
}
struct PendingMetadataDataRequest {
    handle: LoadHandle,
    version: u32,
}

struct RpcRequests {
    pending_data_requests: Vec<(
        ResponsePromise<GetImportArtifactsResults>,
        PendingDataRequest,
    )>,
    pending_metadata_requests: Vec<(
        ResponsePromise<GetAssetMetadataWithDependenciesResults>,
        HashMap<AssetUuid, PendingMetadataDataRequest>,
    )>,
}

unsafe impl Send for RpcRequests {}

impl LoaderData {
    fn add_ref(
        uuid_to_load: &DashMap<AssetUuid, LoadHandle>,
        handle_allocator: &dyn HandleAllocator,
        load_states: &DashMap<LoadHandle, AssetLoad>,
        id: AssetUuid,
    ) -> LoadHandle {
        let handle = *uuid_to_load.entry(id).or_insert_with(|| {
            let new_handle = handle_allocator.alloc();

            log::trace!(
                "Inserting load state for {:?} load handle {:?}",
                id,
                new_handle
            );

            load_states.insert(
                new_handle,
                AssetLoad {
                    asset_id: id,
                    versions: vec![AssetVersionLoad {
                        asset_type: None,
                        auto_commit: true,
                        state: LoadState::None,
                        version: 1,
                    }],
                    version_counter: 1,
                    last_state_change_instant: std::time::Instant::now(),
                    refs: AtomicUsize::new(0),
                    requested_version: None,
                    loaded_version: None,
                    pending_reload: false,
                },
            );
            new_handle
        });
        load_states
            .get(&handle)
            .map(|h| h.refs.fetch_add(1, Ordering::Relaxed));
        handle
    }
    fn get_asset(&self, load: LoadHandle) -> Option<(AssetTypeId, LoadHandle)> {
        self.load_states
            .get(&load)
            .map(|load| {
                load.versions
                    .iter()
                    .find(|version| matches!(version.state, LoadState::Loaded))
                    .map(|version| version.asset_type.map(|t| (t, *load.key())))
                    .unwrap_or(None)
            })
            .unwrap_or(None)
    }
    fn remove_ref(load_states: &DashMap<LoadHandle, AssetLoad>, load: LoadHandle) {
        load_states
            .get(&load)
            .map(|h| h.refs.fetch_sub(1, Ordering::Relaxed));
    }
}

/// [Loader] implementation which communicates with `atelier-daemon`.
/// `RpcLoader` is intended for use in development environments.
pub struct RpcLoader {
    connect_string: String,
    rpc: Arc<Mutex<RpcState>>,
    data: LoaderData,
    requests: Mutex<RpcRequests>,
}

impl Loader for RpcLoader {
    fn get_load(&self, id: AssetUuid) -> Option<LoadHandle> {
        self.data.uuid_to_load.get(&id).map(|l| *l)
    }
    fn get_load_info(&self, load: LoadHandle) -> Option<LoadInfo> {
        self.data.load_states.get(&load).map(|s| LoadInfo {
            asset_id: s.asset_id,
            refs: s.refs.load(Ordering::Relaxed) as u32,
        })
    }
    fn get_load_status(&self, load: LoadHandle) -> LoadStatus {
        if let Some(load) = self.data.load_states.get(&load) {
            let version = if let Some(loaded_version) = load.loaded_version {
                load.versions
                    .iter()
                    .find(|v| v.version == loaded_version)
                    .map(|v| v.state)
            } else {
                load.versions.first().map(|v| v.state)
            };
            version
                .map(|state| match state {
                    LoadState::None => LoadStatus::NotRequested,
                    LoadState::Loaded => LoadStatus::Loaded,
                    LoadState::UnloadRequested | LoadState::Unloading => LoadStatus::Unloading,
                    _ => LoadStatus::Loading,
                })
                .unwrap_or(LoadStatus::NotRequested)
        } else {
            LoadStatus::NotRequested
        }
    }
    fn add_ref(&self, id: AssetUuid) -> LoadHandle {
        LoaderData::add_ref(
            &self.data.uuid_to_load,
            &*self.data.handle_allocator,
            &self.data.load_states,
            id,
        )
    }
    fn get_asset(&self, load: LoadHandle) -> Option<(AssetTypeId, LoadHandle)> {
        self.data.get_asset(load)
    }
    fn remove_ref(&self, load: LoadHandle) {
        LoaderData::remove_ref(&self.data.load_states, load)
    }
    fn process(&mut self, asset_storage: &dyn AssetStorage) -> Result<(), Box<dyn Error>> {
        let mut rpc = self.rpc.lock().expect("rpc mutex poisoned");
        let mut requests = self.requests.lock().expect("rpc requests mutex poisoned");
        match rpc.connection_state() {
            ConnectionState::Error(err) => {
                error!("Error connecting RPC: {}", err);
                rpc.connect(&self.connect_string);
            }
            ConnectionState::None => rpc.connect(&self.connect_string),
            _ => {}
        };
        rpc.poll();
        process_asset_changes(&mut self.data, &mut rpc, asset_storage)?;
        {
            process_load_ops(asset_storage, &mut self.data.load_states, &self.data.op_rx);
            process_load_states(
                asset_storage,
                &*self.data.handle_allocator,
                &mut self.data.load_states,
                &self.data.uuid_to_load,
                &self.data.metadata,
            );
        }
        process_metadata_requests(&mut requests, &mut self.data, &mut rpc)?;
        process_data_requests(&mut requests, &mut self.data, asset_storage, &mut rpc)?;
        Ok(())
    }
}

impl LoaderInfoProvider
    for (
        &DashMap<AssetUuid, LoadHandle>,
        &DashMap<LoadHandle, AssetLoad>,
        &dyn HandleAllocator,
    )
{
    fn get_load_handle(&self, id: &AssetRef) -> Option<LoadHandle> {
        self.0.get(id.expect_uuid()).map(|l| *l)
    }
    fn get_asset_id(&self, load: LoadHandle) -> Option<AssetUuid> {
        self.1.get(&load).map(|l| l.asset_id)
    }
}

impl RpcLoader {
    pub fn new(connect_string: String) -> std::io::Result<RpcLoader> {
        Self::new_with_handle_allocator(connect_string, Arc::new(AtomicHandleAllocator::default()))
    }
    pub fn new_with_handle_allocator(
        connect_string: String,
        handle_allocator: Arc<dyn HandleAllocator>,
    ) -> std::io::Result<RpcLoader> {
        let (tx, rx) = unbounded();
        Ok(RpcLoader {
            connect_string,
            data: LoaderData {
                handle_allocator,
                load_states: DashMap::default(),
                uuid_to_load: DashMap::default(),
                metadata: HashMap::new(),
                op_rx: rx,
                op_tx: Arc::new(tx),
                pending_reloads: Vec::new(),
            },
            rpc: Arc::new(Mutex::new(RpcState::new()?)),
            requests: Mutex::new(RpcRequests {
                pending_metadata_requests: Vec::new(),
                pending_data_requests: Vec::new(),
            }),
        })
    }

    pub fn with_serde_context<R>(&self, tx: &Sender<RefOp>, f: impl FnOnce() -> R) -> R {
        let loader_info_provider = (
            &self.data.uuid_to_load,
            &self.data.load_states,
            &*self.data.handle_allocator,
        );
        let mut rpc = self.rpc.lock().expect("lock poisoned");
        rpc.runtime_mut().block_on(SerdeContext::with(
            &loader_info_provider,
            tx.clone(),
            async { f() },
        ))
    }
}

fn update_asset_metadata(
    metadata: &mut HashMap<AssetUuid, AssetMetadata>,
    uuid: &AssetUuid,
    reader: &asset_metadata::Reader<'_>,
) -> Result<(), capnp::Error> {
    let mut load_deps = Vec::new();
    if let latest_artifact::Artifact(Ok(artifact)) = reader.get_latest_artifact().which()? {
        for dep in artifact.get_load_deps()? {
            let uuid = match dep.which()? {
                asset_ref::Uuid(uuid) => make_array(uuid.and_then(|id| id.get_id())?),
                _ => {
                    return Err(capnp::Error::failed(
                        "capnp: unexpected type when reading load_dep in asset_metadata"
                            .to_string(),
                    ))
                }
            };

            load_deps.push(uuid);
        }
    }
    metadata.insert(*uuid, AssetMetadata { load_deps });
    Ok(())
}

struct AssetLoadResult {
    new_state: LoadState,
    new_version: Option<u32>,
    asset_type: Option<AssetTypeId>,
}

impl AssetLoadResult {
    pub fn from_state(new_state: LoadState) -> Self {
        Self {
            new_state,
            new_version: None,
            asset_type: None,
        }
    }
}

fn load_data(
    loader_info: &dyn LoaderInfoProvider,
    chan: &Arc<Sender<HandleOp>>,
    handle: LoadHandle,
    state: &AssetVersionLoad,
    reader: &artifact::Reader<'_>,
    storage: &dyn AssetStorage,
) -> Result<AssetLoadResult, Box<dyn Error>> {
    assert!(
        LoadState::RequestingData == state.state,
        "load_data expected AssetLoadState::RequestingData, was {:?}",
        state.state
    );
    let asset_type: AssetTypeId = make_array(reader.get_metadata()?.get_type_id()?);
    if let Some(prev_type) = state.asset_type {
        // TODO handle asset type changing?
        assert!(prev_type == asset_type);
    }
    let new_version = state.version;
    storage.update_asset(
        loader_info,
        &asset_type,
        &reader.get_data()?,
        handle,
        AssetLoadOp::new(chan.clone(), handle, state.version),
        new_version,
    )?;
    Ok(AssetLoadResult {
        new_state: LoadState::LoadingAsset,
        new_version: Some(new_version),
        asset_type: Some(asset_type),
    })
}

fn process_pending_requests<T, U, ProcessFunc>(
    requests: &mut Vec<(ResponsePromise<T>, U)>,
    mut process_request_func: ProcessFunc,
) where
    ProcessFunc: for<'a> FnMut(
        Result<
            capnp::message::TypedReader<capnp::message::Builder<capnp::message::HeapAllocator>, T>,
            Box<dyn Error>,
        >,
        &mut U,
    ) -> Result<(), Box<dyn Error>>,
    T: for<'a> capnp::traits::Owned<'a> + 'static,
{
    // reverse range so we can remove inside the loop without consequence
    for i in (0..requests.len()).rev() {
        let request = requests
            .get_mut(i)
            .expect("invalid iteration logic when processing RPC requests");
        let result: Result<Poll<()>, Box<dyn Error>> = match request.0.try_recv() {
            Ok(Some(Ok(response))) => {
                process_request_func(Ok(response), &mut request.1).map(Poll::Ready)
            }
            Ok(Some(Err(err))) => Err(Box::new(err)),
            Ok(None) => Ok(Poll::Pending),
            Err(err @ futures_channel::oneshot::Canceled) => Err(Box::new(err)),
        };
        match result {
            Err(err) => {
                let _ = process_request_func(Err(err), &mut request.1);

                // The request returned Err and does not need to be polled
                let _ = requests.swap_remove(i);
            }
            Ok(Poll::Ready(_)) => {
                // The request returned Ready and does not need to be polled
                let _ = requests.swap_remove(i);
            }
            Ok(Poll::Pending) => {}
        }
    }
}

fn process_data_requests(
    requests: &mut RpcRequests,
    data: &mut LoaderData,
    storage: &dyn AssetStorage,
    rpc: &mut RpcState,
) -> Result<(), Box<dyn Error>> {
    let op_channel = &data.op_tx;
    process_pending_requests(&mut requests.pending_data_requests, |result, request| {
        // We don't want to be holding a lock to the load while calling AssetStorage::update_asset in `load_data`,
        // so we save the state transition as a return value.
        let load_result = {
            let load = data
                .load_states
                .get(&request.handle)
                .expect("load did not exist when data request completed");
            match result {
                Ok(reader) => {
                    let version_load = load
                        .versions
                        .iter()
                        .find(|v| v.version == request.version)
                        .expect("load version did not exist when data request completed");

                    log::trace!("asset data request succeeded for asset {:?}", load.asset_id);
                    let reader = reader.get()?;
                    let artifacts = reader.get_artifacts()?;
                    if artifacts.len() == 0 {
                        warn!(
                            "asset data request did not return any data for asset {:?}",
                            load.asset_id
                        );
                        AssetLoadResult::from_state(LoadState::WaitingForData)
                    } else {
                        load_data(
                            &(
                                &data.uuid_to_load,
                                &data.load_states,
                                &*data.handle_allocator,
                            ),
                            op_channel,
                            request.handle,
                            &version_load,
                            &artifacts.get(0),
                            storage,
                        )?
                    }
                }
                Err(err) => {
                    error!(
                        "asset data request failed for asset {:?}: {}",
                        load.asset_id, err
                    );
                    AssetLoadResult::from_state(LoadState::WaitingForData)
                }
            }
        };

        let mut load = data
            .load_states
            .get_mut(&request.handle)
            .expect("load did not exist when data request completed");

        let version_load = load
            .versions
            .iter_mut()
            .find(|v| v.version == request.version)
            .expect("load version did not exist when data request completed");
        version_load.state = load_result.new_state;
        if let Some(asset_type) = load_result.asset_type {
            version_load.asset_type = Some(asset_type);
        }
        if let Some(version) = load_result.new_version {
            load.requested_version = Some(version);
        }
        Ok(())
    });
    if let ConnectionState::Connected = rpc.connection_state() {
        let mut assets_to_request = Vec::new();
        for mut load in data.load_states.iter_mut() {
            let handle = *load.key();
            let load = load.value_mut();

            if let Some(version_load) = load
                .versions
                .iter_mut()
                .find(|v| matches!(v.state, LoadState::WaitingForData))
            {
                version_load.state = LoadState::RequestingData;
                assets_to_request.push((load.asset_id, handle, version_load.version));
            }
        }
        if !assets_to_request.is_empty() {
            for (asset, handle, version) in assets_to_request {
                log::trace!(
                    "Queue request for asset import artifact {:?} {:?}",
                    asset,
                    handle
                );
                let response = rpc.request(move |_conn, snapshot| {
                    let mut request = snapshot.get_import_artifacts_request();
                    let mut assets = request.get().init_assets(1);
                    assets.reborrow().get(0).set_id(&asset.0);
                    log::trace!("Building get_import_artifacts_request for {:?}", asset);
                    (request, PendingDataRequest { handle, version })
                });
                requests.pending_data_requests.push(response);
            }
        }
    }
    Ok(())
}

fn process_metadata_requests(
    requests: &mut RpcRequests,
    data: &mut LoaderData,
    rpc: &mut RpcState,
) -> Result<(), capnp::Error> {
    let metadata = &mut data.metadata;
    let uuid_to_load = &data.uuid_to_load;
    let load_states = &data.load_states;
    process_pending_requests(
        &mut requests.pending_metadata_requests,
        |result, requested_assets| {
            match result {
                Ok(reader) => {
                    let reader = reader.get()?;
                    let assets = reader.get_assets()?;
                    for asset in assets {
                        let asset_uuid: AssetUuid = make_array(asset.get_id()?.get_id()?);
                        update_asset_metadata(metadata, &asset_uuid, &asset)?;
                        if let Some(load_handle) = uuid_to_load.get(&asset_uuid) {
                            let mut load = load_states
                                .get_mut(&*load_handle)
                                .expect("uuid in uuid_to_load but not in load_states");
                            log::trace!(
                                "received metadata for {:?} after {} secs",
                                asset_uuid,
                                std::time::Instant::now()
                                    .duration_since(load.last_state_change_instant)
                                    .as_secs_f32()
                            );
                            let load_version = requested_assets
                                .get(&asset_uuid)
                                .map(|request| request.version);

                            let version_load = load.versions.iter_mut().find(|v| {
                                load_version.is_none() || v.version == load_version.unwrap()
                            });
                            if let Some(version_load) = version_load {
                                if let LoadState::RequestingMetadata = version_load.state {
                                    version_load.state = LoadState::RequestDependencies
                                }
                            }
                        }
                    }
                    for request in requested_assets.values() {
                        let mut load = load_states
                            .get_mut(&request.handle)
                            .expect("uuid in uuid_to_load but not in load_states");
                        let version_load = load
                            .versions
                            .iter_mut()
                            .find(|v| v.version == request.version)
                            .expect("load in requested_assets but not in load.versions");
                        if let LoadState::RequestingMetadata = version_load.state {
                            version_load.state = LoadState::WaitingForMetadata
                        }
                    }
                }
                Err(err) => {
                    error!("metadata request failed: {}", err);
                    for request in requested_assets.values() {
                        let mut load = load_states
                            .get_mut(&request.handle)
                            .expect("uuid in uuid_to_load but not in load_states");
                        let version_load = load
                            .versions
                            .iter_mut()
                            .find(|v| v.version == request.version)
                            .expect("load in requested_assets but not in load.versions");
                        if let LoadState::RequestingMetadata = version_load.state {
                            version_load.state = LoadState::WaitingForMetadata
                        }
                    }
                }
            };
            Ok(())
        },
    );
    if let ConnectionState::Connected = rpc.connection_state() {
        let mut assets_to_request = HashMap::new();
        for mut entry in load_states.iter_mut() {
            let handle = *entry.key();
            let load = entry.value_mut();
            for version_load in &mut load.versions {
                if let LoadState::WaitingForMetadata = version_load.state {
                    version_load.state = LoadState::RequestingMetadata;
                    assets_to_request.insert(
                        load.asset_id,
                        PendingMetadataDataRequest {
                            handle,
                            version: version_load.version,
                        },
                    );
                }
            }
        }
        if !assets_to_request.is_empty() {
            for (asset, request) in &assets_to_request {
                log::trace!(
                    "Queue request for asset metadata {:?} {:?}",
                    asset,
                    request.handle
                );
            }
            let response = rpc.request(move |_conn, snapshot| {
                let mut request = snapshot.get_asset_metadata_with_dependencies_request();
                let mut assets = request.get().init_assets(assets_to_request.len() as u32);
                for (idx, (asset, _)) in assets_to_request.iter().enumerate() {
                    assets.reborrow().get(idx as u32).set_id(&asset.0);
                    log::trace!(
                        "Building get_asset_metadata_with_dependencies_request for {:?}",
                        asset
                    );
                }
                (request, assets_to_request)
            });
            requests.pending_metadata_requests.push(response);
        }
    }
    Ok(())
}

fn commit_asset(
    handle: LoadHandle,
    load: &mut AssetLoad,
    version: u32,
    asset_storage: &dyn AssetStorage,
) {
    let version_load = load
        .versions
        .iter_mut()
        .find(|v| v.version == version)
        .expect("expected version in load when committing asset");
    assert!(
        LoadState::LoadingAsset == version_load.state
            || LoadState::LoadedUncommitted == version_load.state
    );
    let asset_type = version_load
        .asset_type
        .as_ref()
        .expect("in LoadingAsset state but asset_type is None");
    asset_storage.commit_asset_version(asset_type, handle, version_load.version);
    version_load.state = LoadState::Loaded;
    for version_load in load.versions.iter_mut() {
        if version_load.version != version {
            assert!(LoadState::Loaded == version_load.state);
            version_load.state = LoadState::UnloadRequested;
        }
    }
}

fn process_load_ops(
    asset_storage: &dyn AssetStorage,
    load_states: &mut DashMap<LoadHandle, AssetLoad>,
    op_rx: &Receiver<HandleOp>,
) {
    while let Ok(op) = op_rx.try_recv() {
        match op {
            HandleOp::Error(_handle, _version, err) => {
                panic!("load error {}", err);
            }
            HandleOp::Complete(handle, version) => {
                let mut load = load_states
                    .get_mut(&handle)
                    .expect("load op completed but load state does not exist");
                let load_version = load
                    .versions
                    .iter_mut()
                    .find(|v| v.version == version)
                    .expect("loade op completed but version not found in load");
                if load_version.auto_commit {
                    commit_asset(handle, load.value_mut(), version, asset_storage);
                    load.loaded_version = Some(version);
                } else {
                    load_version.state = LoadState::LoadedUncommitted;
                }
            }
            HandleOp::Drop(handle, version) => panic!(
                "load op dropped without calling complete/error, handle {:?} version {}",
                handle, version
            ),
        }
    }
}

fn process_load_states(
    asset_storage: &dyn AssetStorage,
    handle_allocator: &dyn HandleAllocator,
    load_states: &mut DashMap<LoadHandle, AssetLoad>,
    uuid_to_load: &DashMap<AssetUuid, LoadHandle>,
    metadata: &HashMap<AssetUuid, AssetMetadata>,
) {
    let mut to_remove = Vec::new();
    let keys: Vec<_> = load_states.iter().map(|x| *x.key()).collect();

    for key in keys {
        let mut versions_to_remove = Vec::new();

        let mut entry = load_states.get_mut(&key).unwrap();
        let load = entry.value_mut();

        let has_refs = load.refs.load(Ordering::Relaxed) > 0;
        if !has_refs && load.versions.is_empty() {
            to_remove.push(key);
        } else if has_refs && load.pending_reload {
            // Make sure we are not already loading something before starting a load of a new version
            if load
                .versions
                .iter()
                .all(|v| matches!(v.state, LoadState::Loaded))
            {
                load.version_counter += 1;
                let new_version = load.version_counter;
                load.versions.push(AssetVersionLoad {
                    asset_type: None,
                    // The assets are not auto_commit for reloads to ensure all assets in a
                    // changeset are made visible together, atomically
                    auto_commit: false,
                    state: LoadState::None,
                    version: new_version,
                });
            }
        } else {
            let last_state_change_instant = load.last_state_change_instant;
            let mut versions = load.versions.clone();
            let asset_id = load.asset_id;
            // make sure we drop the lock before we start processing the state
            drop(entry);
            let mut state_change = false;
            let mut log_old_state = None;
            let mut log_new_state = None;
            for version_load in &mut versions {
                let new_state = match version_load.state {
                    LoadState::None if has_refs => {
                        if metadata.contains_key(&asset_id) {
                            LoadState::RequestDependencies
                        } else {
                            LoadState::WaitingForMetadata
                        }
                    }
                    LoadState::None => {
                        // no refs, inactive load
                        LoadState::UnloadRequested
                    }
                    LoadState::WaitingForMetadata => {
                        if metadata.contains_key(&asset_id) {
                            LoadState::RequestDependencies
                        } else {
                            LoadState::WaitingForMetadata
                        }
                    }
                    LoadState::RequestingMetadata => LoadState::RequestingMetadata,
                    LoadState::RequestDependencies => {
                        // Add ref to each of the dependent assets.
                        let asset_metadata = metadata.get(&asset_id).unwrap_or_else(|| {
                            panic!("Expected metadata for asset `{:?}` to exist.", asset_id)
                        });
                        asset_metadata
                            .load_deps
                            .iter()
                            .for_each(|dependency_asset_id| {
                                LoaderData::add_ref(
                                    uuid_to_load,
                                    handle_allocator,
                                    load_states,
                                    *dependency_asset_id,
                                );
                            });

                        LoadState::WaitingForDependencies
                    }
                    LoadState::WaitingForDependencies => {
                        let asset_id = asset_id;
                        let asset_metadata = metadata.get(&asset_id).unwrap_or_else(|| {
                            panic!("Expected metadata for asset `{:?}` to exist.", asset_id)
                        });

                        // Ensure dependencies are loaded by engine before continuing to load this asset.
                        let asset_dependencies_committed =
                            asset_metadata.load_deps.iter().all(|dependency_asset_id| {
                                uuid_to_load
                                    .get(dependency_asset_id)
                                    .as_ref()
                                    .and_then(|dep_load_handle| load_states.get(dep_load_handle))
                                    .map(|dep_load| {
                                        // Note that we accept assets to be uncommitted but loaded
                                        // This is to support atomically committing a set of changes when hot reloading

                                        // TODO: Properly check that all dependencies have loaded their *new* version
                                        dep_load.versions.iter().all(|v| {
                                            matches!(
                                                v.state,
                                                LoadState::Loaded | LoadState::LoadedUncommitted
                                            )
                                        })
                                    })
                                    .unwrap_or(false)
                            });

                        if asset_dependencies_committed {
                            LoadState::WaitingForData
                        } else {
                            LoadState::WaitingForDependencies
                        }
                    }
                    LoadState::WaitingForData => LoadState::WaitingForData,
                    LoadState::RequestingData => LoadState::RequestingData,
                    LoadState::LoadingAsset => LoadState::LoadingAsset,
                    LoadState::LoadedUncommitted => LoadState::LoadedUncommitted,
                    LoadState::Loaded => {
                        if !has_refs {
                            LoadState::UnloadRequested
                        } else {
                            LoadState::Loaded
                        }
                    }
                    LoadState::UnloadRequested => {
                        let asset_id = {
                            let mut entry = load_states.get_mut(&key).unwrap();
                            let (key, value) = entry.pair_mut();
                            if let Some(asset_type) = version_load.asset_type.take() {
                                asset_storage.free(&asset_type, *key);
                                let version = version_load.version;
                                if value.requested_version == Some(version) {
                                    value.requested_version = None;
                                }
                                if value.loaded_version == Some(version) {
                                    value.loaded_version = None;
                                }
                            }

                            // Remove reference from asset dependencies.
                            value.asset_id
                        };

                        let asset_metadata = metadata.get(&asset_id).unwrap_or_else(|| {
                            panic!("Expected metadata for asset `{:?}` to exist.", asset_id)
                        });
                        asset_metadata
                            .load_deps
                            .iter()
                            .for_each(|dependency_asset_id| {
                                if let Some(dependency_load_handle) =
                                    uuid_to_load.get(dependency_asset_id).as_ref()
                                {
                                    log::debug!("Removing ref from `{:?}`", *dependency_asset_id);
                                    LoaderData::remove_ref(load_states, **dependency_load_handle)
                                } else {
                                    panic!(
                                        "Expected load handle to exist for asset `{:?}`.",
                                        dependency_asset_id
                                    );
                                }
                            });

                        LoadState::Unloading
                    }
                    LoadState::Unloading => {
                        let entry = load_states.get(&key).unwrap();

                        if entry.value().refs.load(Ordering::Relaxed) == 0 {
                            versions_to_remove.push(version_load.version);
                        }
                        LoadState::None
                    }
                };
                if version_load.state != new_state {
                    state_change = true;
                    log_new_state = Some(new_state);
                    log_old_state = Some(version_load.state);
                    version_load.state = new_state;
                }
            }
            let mut entry = load_states.get_mut(&key).unwrap();

            for version in versions_to_remove {
                versions.retain(|v| v.version != version);
            }

            entry.value_mut().versions = versions;
            if state_change {
                let time_in_state = std::time::Instant::now()
                    .duration_since(last_state_change_instant)
                    .as_secs_f32();
                log::debug!("process_load_states asset load state changed, Key: {:?} Old state: {:?} New state: {:?} Time in state: {}", key, log_old_state.unwrap(), log_new_state.unwrap(), time_in_state);

                entry.value_mut().last_state_change_instant = std::time::Instant::now();
            } else {
                let time_in_state = std::time::Instant::now()
                    .duration_since(last_state_change_instant)
                    .as_secs_f32();
                log::trace!(
                    "process_load_states Key: {:?} Old state: {:?} Time in state: {}",
                    key,
                    entry
                        .value()
                        .versions
                        .iter()
                        .map(|v| format!("{:?}", v.state))
                        .collect::<Vec<_>>()
                        .join(", "),
                    time_in_state
                );
            }
        }

        // Uncomment for recursive logging of dependency's load states
        /*
        if log::log_enabled!(log::Level::Trace) {
            for entry in load_states.iter() {
                if entry.value().state == LoadState::WaitingForDependencies {
                    dump_dependencies(&value.asset_id, load_states, uuid_to_load, metadata, 0);
                }
            }
        }
        */
    }
    for _i in to_remove {
        // TODO: This will reset the version counter because it's stored in the AssetLoad.
        // Is this a problem? Should we guarantee that users never see the same version twice, ever?
        // Should we store version counters separately?
        //     let load_state = load_states.remove(&i);
        //     if let Some((_, load_state)) = load_state {
        //         uuid_to_load.remove(&load_state.asset_id);
        //     }
    }
}

// fn dump_dependencies(
//     asset_id: &AssetUuid,
//     load_states: &DashMap<LoadHandle, AssetLoad>,
//     uuid_to_load: &DashMap<AssetUuid, LoadHandle>,
//     metadata: &HashMap<AssetUuid, AssetMetadata>,
//     indent: usize,
// ) {
//     if let Some(load_handle) = uuid_to_load.get(asset_id) {
//         if let Some(load_state) = load_states.get(&load_handle) {
//             log::trace!(
//                 "{} Load Handle: {:?}  Load State: {:?}",
//                 String::from_utf8(vec![b' '; indent]).unwrap(),
//                 *load_handle,
//                 *load_state
//             );

//             if let Some(asset_metadata) = metadata.get(&asset_id) {
//                 asset_metadata
//                     .load_deps
//                     .iter()
//                     .for_each(|dependency_asset_id| {
//                         dump_dependencies(
//                             dependency_asset_id,
//                             load_states,
//                             uuid_to_load,
//                             metadata,
//                             indent + 2,
//                         );
//                     });
//             } else {
//                 log::trace!(
//                     "{} Metadata not found for asset `{:?}`",
//                     String::from_utf8(vec![b' '; indent]).unwrap(),
//                     asset_id
//                 );
//             }
//         } else {
//             log::trace!(
//                 "{} Load state not found for asset `{:?}`",
//                 String::from_utf8(vec![b' '; indent]).unwrap(),
//                 asset_id
//             );
//         }
//     } else {
//         log::trace!(
//             "{} Load handle not found for asset `{:?}`",
//             String::from_utf8(vec![b' '; indent]).unwrap(),
//             asset_id
//         );
//     }
// }

/// Checks for changed assets that need to be reloaded or unloaded
fn process_asset_changes(
    data: &mut LoaderData,
    rpc: &mut RpcState,
    asset_storage: &dyn AssetStorage,
) -> Result<(), Box<dyn Error>> {
    if data.pending_reloads.is_empty() {
        // if we have no pending hot reloads, poll for new changes
        let changes = rpc.check_asset_changes();
        if let Some(changes) = changes {
            // TODO handle deleted assets
            for asset_id in changes.changed.iter() {
                let current_version = data
                    .uuid_to_load
                    .get(asset_id)
                    .map(|l| *l)
                    .and_then(|load_handle| {
                        data.load_states
                            .get(&load_handle)
                            .map(|load| (load_handle, load))
                    })
                    .map(|(load_handle, load)| {
                        load.requested_version.map(|version| (load_handle, version))
                    })
                    .unwrap_or(None);
                if let Some((handle, current_version)) = current_version {
                    let mut load = data
                        .load_states
                        .get_mut(&handle)
                        .expect("load state should exist for pending reload");
                    load.pending_reload = true;
                    data.pending_reloads.push(PendingReload {
                        asset_id: *asset_id,
                        version_before: current_version,
                    });
                }
            }
        }
    } else {
        let is_finished = data.pending_reloads.iter().all(|reload| {
            data.uuid_to_load
                .get(&reload.asset_id)
                .as_ref()
                .and_then(|load_handle| data.load_states.get(load_handle))
                .map(|load| {
                    // The reload is considered finished if we have a loaded asset with a version
                    // that is higher than the version observed when the reload was requested
                    load.versions.iter().any(|v| {
                        matches!(v.state, LoadState::Loaded)
                            && load.requested_version.unwrap() > reload.version_before
                    })
                })
                // A pending reload for something that is not supposed to be loaded is considered finished.
                // The asset could have been unloaded by being unreferenced.
                .unwrap_or(true)
        });
        if is_finished {
            data.pending_reloads.iter().for_each(|reload| {
                if let Some((load_handle, mut load)) = data
                    .uuid_to_load
                    .get(&reload.asset_id)
                    .as_ref()
                    .and_then(|load_handle| {
                        data.load_states
                            .get_mut(load_handle)
                            .map(|load| (load_handle, load))
                    })
                {
                    if let Some(version_to_commit) = load
                        .versions
                        .iter()
                        .find(|v| {
                            matches!(v.state, LoadState::Loaded | LoadState::LoadedUncommitted)
                        })
                        .map(|v| v.version)
                    {
                        // Commit reloaded asset
                        commit_asset(
                            **load_handle,
                            load.value_mut(),
                            version_to_commit,
                            asset_storage,
                        );
                    }
                }
            });
            data.pending_reloads.clear();
        }
    }
    Ok(())
}

pub fn default_connect_string() -> &'static str {
    "127.0.0.1:9999"
}

impl Default for RpcLoader {
    fn default() -> RpcLoader {
        RpcLoader::new(default_connect_string().to_string()).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TypeUuid;
    use atelier_core::AssetUuid;
    use atelier_daemon::{init_logging, AssetDaemon};
    use atelier_importer::{AsyncImporter, ImportedAsset, ImporterValue, Result as ImportResult};
    use futures_core::future::BoxFuture;
    use futures_io::AsyncRead;
    use futures_util::io::AsyncReadExt;
    use serde::{Deserialize, Serialize};
    use std::{
        iter::FromIterator,
        path::PathBuf,
        str::FromStr,
        string::FromUtf8Error,
        sync::RwLock,
        thread::{self, JoinHandle},
    };
    use uuid::Uuid;

    #[derive(Debug)]
    struct LoadState {
        size: Option<usize>,
        commit_version: Option<u32>,
        load_version: Option<u32>,
    }
    struct Storage {
        map: RwLock<HashMap<LoadHandle, LoadState>>,
    }
    impl AssetStorage for Storage {
        fn update_asset(
            &self,
            _loader_info: &dyn LoaderInfoProvider,
            _asset_type: &AssetTypeId,
            data: &[u8],
            loader_handle: LoadHandle,
            load_op: AssetLoadOp,
            version: u32,
        ) -> Result<(), Box<dyn Error>> {
            println!(
                "update asset {:?} data size {}",
                loader_handle,
                data.as_ref().len()
            );
            let mut map = self.map.write().unwrap();
            let state = map.entry(loader_handle).or_insert(LoadState {
                size: None,
                commit_version: None,
                load_version: None,
            });

            state.size = Some(data.as_ref().len());
            state.load_version = Some(version);
            load_op.complete();
            Ok(())
        }
        fn commit_asset_version(
            &self,
            _asset_type: &AssetTypeId,
            loader_handle: LoadHandle,
            version: u32,
        ) {
            println!("commit asset {:?}", loader_handle,);
            let mut map = self.map.write().unwrap();
            let state = map.get_mut(&loader_handle).unwrap();

            assert!(state.load_version.unwrap() == version);
            state.commit_version = Some(version);
            state.load_version = None;
        }
        fn free(&self, _asset_type: &AssetTypeId, loader_handle: LoadHandle) {
            println!("free asset {:?}", loader_handle);
            self.map.write().unwrap().remove(&loader_handle);
        }
    }

    /// Removes file comments (begin with `#`) and empty lines.
    #[derive(Clone, Debug, Default, Deserialize, Serialize, TypeUuid)]
    #[uuid = "346e6a3e-3278-4c53-b21c-99b4350662db"]
    pub struct TxtFormat;
    impl TxtFormat {
        fn from_utf8(&self, vec: Vec<u8>) -> Result<String, FromUtf8Error> {
            String::from_utf8(vec).map(|data| {
                let processed = data
                    .lines()
                    .map(|line| {
                        line.find('#')
                            .map(|index| line.split_at(index).0)
                            .unwrap_or(line)
                            .trim()
                    })
                    .filter(|line| !line.is_empty())
                    .flat_map(|line| line.chars().chain(std::iter::once('\n')));
                String::from_iter(processed)
            })
        }
    }
    /// A simple state for Importer to retain the same UUID between imports
    /// for all single-asset source files
    #[derive(Default, Deserialize, Serialize, TypeUuid)]
    #[uuid = "c50c36fe-8df0-48fe-b1d7-3e69ab00a997"]
    pub struct TxtImporterState {
        id: Option<AssetUuid>,
    }
    #[derive(TypeUuid)]
    #[uuid = "fa50e08c-af6c-4ada-aed1-447c116d63bc"]
    struct TxtImporter;
    impl AsyncImporter for TxtImporter {
        type State = TxtImporterState;
        type Options = TxtFormat;

        fn version_static() -> u32
        where
            Self: Sized,
        {
            1
        }
        fn version(&self) -> u32 {
            Self::version_static()
        }

        fn import<'a>(
            &'a self,
            source: &'a mut (dyn AsyncRead + Unpin + Send + Sync),
            txt_format: &'a Self::Options,
            state: &'a mut Self::State,
        ) -> BoxFuture<'a, ImportResult<ImporterValue>> {
            Box::pin(async move {
                if state.id.is_none() {
                    state.id = Some(AssetUuid(*uuid::Uuid::new_v4().as_bytes()));
                }
                let mut bytes = Vec::new();
                source.read_to_end(&mut bytes).await?;
                let parsed_asset_data = txt_format
                    .from_utf8(bytes)
                    .expect("Failed to construct string asset.");

                let load_deps = parsed_asset_data
                    .lines()
                    .filter_map(|line| Uuid::from_str(line).ok())
                    .map(|uuid| AssetRef::Uuid(AssetUuid(*uuid.as_bytes())))
                    .collect::<Vec<AssetRef>>();

                Ok(ImporterValue {
                    assets: vec![ImportedAsset {
                        id: state.id.expect("AssetUuid not generated"),
                        search_tags: Vec::new(),
                        build_deps: Vec::new(),
                        load_deps,
                        asset_data: Box::new(parsed_asset_data),
                        build_pipeline: None,
                    }],
                })
            })
        }
    }

    fn wait_for_status(
        status: LoadStatus,
        handle: LoadHandle,
        loader: &mut RpcLoader,
        storage: &Storage,
    ) {
        loop {
            println!(
                "state {:?} expecting {:?}",
                loader.get_load_status(handle),
                status
            );
            if std::mem::discriminant(&status)
                == std::mem::discriminant(&loader.get_load_status(handle))
            {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
            if let Err(e) = loader.process(storage) {
                println!("err {:?}", e);
            }
        }
    }

    #[test]
    fn test_connect() {
        let _ = init_logging(); // Another test may have initialized logging, so we ignore errors.

        // Start daemon in a separate thread
        let daemon_port = 2500;
        let daemon_address = format!("127.0.0.1:{}", daemon_port);
        let _atelier_daemon = spawn_daemon(&daemon_address);

        let mut loader = RpcLoader::new(daemon_address).expect("Failed to construct `RpcLoader`.");
        let handle = loader.add_ref(
            // asset uuid of "tests/assets/asset.txt"
            AssetUuid(
                *uuid::Uuid::parse_str("60352042-616f-460e-abd2-546195c060fe")
                    .unwrap()
                    .as_bytes(),
            ),
        );
        let storage = &mut Storage {
            map: RwLock::new(HashMap::new()),
        };
        wait_for_status(LoadStatus::Loaded, handle, &mut loader, &storage);
        loader.remove_ref(handle);
        wait_for_status(LoadStatus::NotRequested, handle, &mut loader, &storage);
    }

    #[test]
    fn test_load_with_dependencies() {
        let _ = init_logging(); // Another test may have initialized logging, so we ignore errors.

        // Start daemon in a separate thread
        let daemon_port = 2505;
        let daemon_address = format!("127.0.0.1:{}", daemon_port);
        let _atelier_daemon = spawn_daemon(&daemon_address);

        let mut loader = RpcLoader::new(daemon_address).expect("Failed to construct `RpcLoader`.");
        let handle = loader.add_ref(
            // asset uuid of "tests/assets/asset_a.txt"
            AssetUuid(
                *uuid::Uuid::parse_str("a5ce4da0-675e-4460-be02-c8b145c2ee49")
                    .unwrap()
                    .as_bytes(),
            ),
        );
        let storage = &mut Storage {
            map: RwLock::new(HashMap::new()),
        };
        wait_for_status(LoadStatus::Loaded, handle, &mut loader, &storage);

        // Check that dependent assets are loaded
        let asset_handles = asset_tree()
            .iter()
            .map(|(asset_uuid, file_name)| {
                let asset_load_handle = loader
                    .get_load(*asset_uuid)
                    .unwrap_or_else(|| panic!("Expected `{}` to be loaded.", file_name));

                (asset_load_handle, *file_name)
            })
            .collect::<Vec<(LoadHandle, &'static str)>>();

        asset_handles
            .iter()
            .for_each(|(asset_load_handle, file_name)| {
                assert_eq!(
                    std::mem::discriminant(&LoadStatus::Loaded),
                    std::mem::discriminant(&loader.get_load_status(*asset_load_handle)),
                    "Expected `{}` to be loaded.",
                    file_name
                );
            });

        // Remove reference to top level asset.
        loader.remove_ref(handle);
        wait_for_status(LoadStatus::NotRequested, handle, &mut loader, &storage);

        // Remove ref when unloading top level asset.
        asset_handles
            .iter()
            .for_each(|(asset_load_handle, file_name)| {
                println!("Waiting for {} to be `NotRequested`.", file_name);
                wait_for_status(
                    LoadStatus::NotRequested,
                    *asset_load_handle,
                    &mut loader,
                    &storage,
                );
            });
    }

    fn asset_tree() -> Vec<(AssetUuid, &'static str)> {
        [
            ("a5ce4da0-675e-4460-be02-c8b145c2ee49", "asset_a.txt"),
            ("039dc5f8-ee1c-4949-a7df-72383f12c7a2", "asset_b.txt"),
            ("c071f3ff-c9ea-4bf5-b3b9-bf5fc29f9b59", "asset_c.txt"),
            ("55adb689-b91c-42a0-941b-de4a9f7f4f03", "asset_d.txt"),
        ]
        .iter()
        .map(|(id, file_name)| {
            let asset_uuid = *uuid::Uuid::parse_str(id)
                .unwrap_or_else(|_| panic!("Failed to parse `{}` as `Uuid`.", id))
                .as_bytes();

            (AssetUuid(asset_uuid), *file_name)
        })
        .collect::<Vec<(AssetUuid, &'static str)>>()
    }

    fn spawn_daemon(daemon_address: &str) -> JoinHandle<()> {
        let daemon_address = daemon_address
            .parse()
            .expect("Failed to parse string as `SocketAddr`.");
        thread::Builder::new()
            .name("atelier-daemon".to_string())
            .spawn(move || {
                let tests_path = PathBuf::from_iter(&[env!("CARGO_MANIFEST_DIR"), "tests"]);

                AssetDaemon::default()
                    .with_db_path(tests_path.join("assets_db"))
                    .with_address(daemon_address)
                    .with_importer("txt", TxtImporter)
                    .with_asset_dirs(vec![tests_path.join("assets")])
                    .run();
            })
            .expect("Failed to spawn `atelier-daemon` thread.")
    }
}
