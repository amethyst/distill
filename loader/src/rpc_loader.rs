use crate::{
    handle::{RefOp, SerdeContext},
    loader::{
        AssetLoadOp, AssetStorage, AtomicHandleAllocator, HandleAllocator, HandleOp,
        IndirectionTable, LoadHandle, LoadInfo, LoadStatus, LoaderInfoProvider,
    },
};
use atelier_core::{ArtifactId, ArtifactMetadata, AssetMetadata, AssetRef, AssetTypeId, AssetUuid};
use crossbeam_channel::{unbounded, Receiver, Sender};
use dashmap::DashMap;
use log::error;
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

pub type CapnpMessage<T> =
    capnp::message::TypedReader<capnp::message::Builder<capnp::message::HeapAllocator>, T>;

pub trait LoaderIO {
    fn get_asset_metadata_with_dependencies(&mut self, request: MetadataRequest);
    fn get_asset_candidates(&mut self, requests: Vec<ResolveRequest>);
    fn get_artifacts(&mut self, requests: Vec<DataRequest>);
    fn tick(&mut self, loader: &mut LoaderState);
    fn with_runtime(&self, f: &mut dyn FnMut(&mut tokio::runtime::Runtime));
}

/// Describes the state of an asset load operation
#[derive(Copy, Clone, PartialEq, Debug)]
enum LoadState {
    /// Indeterminate state - may transition into a load, or result in removal if ref count is == 0
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

/// Describes the state of an indirect Handle
#[derive(Copy, Clone, PartialEq, Debug)]
enum IndirectHandleState {
    None,
    WaitingForMetadata,
    RequestingMetadata,
    Resolved,
}

#[derive(Clone, PartialEq, Eq, Debug, Hash)]
pub enum IndirectIdentifier {
    Path(String),
}

struct IndirectLoad {
    id: IndirectIdentifier,
    state: IndirectHandleState,
    resolved_handle: Option<LoadHandle>,
    refs: AtomicUsize,
    pending_reresolve: bool,
}

#[derive(Debug, Clone)]
struct AssetVersionLoad {
    state: LoadState,
    metadata: Option<ArtifactMetadata>,
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

/// Keeps track of a pending reload
struct PendingReload {
    /// ID of asset that should be reloaded
    asset_id: AssetUuid,
    /// The version of the asset before it was reloaded
    version_before: u32,
}

pub struct LoaderState {
    handle_allocator: Arc<dyn HandleAllocator>,
    load_states: DashMap<LoadHandle, AssetLoad>,
    uuid_to_load: DashMap<AssetUuid, LoadHandle>,
    op_tx: Sender<HandleOp>,
    op_rx: Receiver<HandleOp>,
    invalidate_tx: Sender<AssetUuid>,
    invalidate_rx: Receiver<AssetUuid>,
    pending_reloads: Vec<PendingReload>,
    indirect_loads: DashMap<IndirectIdentifier, IndirectLoad>,
    indirect_table: IndirectionTable,
    responses: IORequestChannels,
}

struct IORequestChannels {
    data_rx: Receiver<(Result<Vec<u8>, Box<dyn Error + Send>>, LoadHandle, u32)>,
    data_tx: Sender<(Result<Vec<u8>, Box<dyn Error + Send>>, LoadHandle, u32)>,
    metadata_rx: Receiver<(
        Result<Vec<ArtifactMetadata>, Box<dyn Error + Send>>,
        HashMap<AssetUuid, (LoadHandle, u32)>,
    )>,
    metadata_tx: Sender<(
        Result<Vec<ArtifactMetadata>, Box<dyn Error + Send>>,
        HashMap<AssetUuid, (LoadHandle, u32)>,
    )>,
    resolve_rx: Receiver<(
        Result<Vec<AssetMetadata>, Box<dyn Error + Send>>,
        IndirectIdentifier,
    )>,
    resolve_tx: Sender<(
        Result<Vec<AssetMetadata>, Box<dyn Error + Send>>,
        IndirectIdentifier,
    )>,
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

pub struct MetadataRequest {
    metadata_tx: Sender<(
        Result<Vec<ArtifactMetadata>, Box<dyn Error + Send>>,
        HashMap<AssetUuid, (LoadHandle, u32)>,
    )>,
    requests: HashMap<AssetUuid, (LoadHandle, u32)>,
    consumed: bool,
}
impl MetadataRequest {
    pub fn requested_assets(&self) -> impl Iterator<Item = &AssetUuid> {
        self.requests.keys()
    }
    pub fn error<T: Error + Send + 'static>(mut self, err: T) {
        self.consumed = true;
        let _ = self.metadata_tx.send((Err(Box::new(err)), self.requests));
    }
    pub fn complete(mut self, metadata: Vec<ArtifactMetadata>) {
        self.consumed = true;
        let _ = self.metadata_tx.send((Ok(metadata), self.requests));
    }
}

pub struct DataRequest {
    data_tx: Sender<(Result<Vec<u8>, Box<dyn Error + Send>>, LoadHandle, u32)>,
    asset_id: AssetUuid,
    artifact_id: ArtifactId,
    handle: LoadHandle,
    version: u32,
    consumed: bool,
}
impl DataRequest {
    pub fn asset_id(&self) -> AssetUuid {
        self.asset_id
    }
    pub fn artifact_id(&self) -> ArtifactId {
        self.artifact_id
    }
    pub fn error<T: Error + Send + 'static>(mut self, err: T) {
        self.consumed = true;
        let _ = self
            .data_tx
            .send((Err(Box::new(err)), self.handle, self.version));
    }
    pub fn complete(mut self, data: Vec<u8>) {
        self.consumed = true;
        let _ = self.data_tx.send((Ok(data), self.handle, self.version));
    }
}

pub struct ResolveRequest {
    resolve_tx: Sender<(
        Result<Vec<AssetMetadata>, Box<dyn Error + Send>>,
        IndirectIdentifier,
    )>,
    id: IndirectIdentifier,
}
impl ResolveRequest {
    pub fn identifier(&self) -> &IndirectIdentifier {
        &self.id
    }
}

impl LoaderState {
    fn get_or_insert(&self, id: AssetUuid) -> LoadHandle {
        let handle = *self.uuid_to_load.entry(id).or_insert_with(|| {
            let new_handle = self.handle_allocator.alloc();

            log::trace!(
                "Inserting load state for {:?} load handle {:?}",
                id,
                new_handle
            );

            self.load_states.insert(
                new_handle,
                AssetLoad {
                    asset_id: id,
                    versions: vec![AssetVersionLoad {
                        asset_type: None,
                        auto_commit: true,
                        metadata: None,
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
        handle
    }
    fn add_ref(&self, id: AssetUuid) -> LoadHandle {
        let handle = self.get_or_insert(id);
        self.load_states
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
    fn remove_ref(&self, load: LoadHandle) {
        self.load_states
            .get(&load)
            .map(|h| h.refs.fetch_sub(1, Ordering::Relaxed));
    }
    fn process_load_states(&self, asset_storage: &dyn AssetStorage) {
        let mut to_remove = Vec::new();
        let keys: Vec<_> = self.load_states.iter().map(|x| *x.key()).collect();

        for key in keys {
            let mut versions_to_remove = Vec::new();

            let mut entry = self.load_states.get_mut(&key).unwrap();
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
                        metadata: None,
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
                // make sure we drop the lock before we start processing the state
                drop(entry);
                let mut state_change = false;
                let mut log_old_state = None;
                let mut log_new_state = None;
                let num_versions = versions.len();
                for version_load in &mut versions {
                    let new_state = match version_load.state {
                        LoadState::None if has_refs => {
                            if version_load.metadata.is_some() {
                                LoadState::RequestDependencies
                            } else {
                                LoadState::WaitingForMetadata
                            }
                        }
                        LoadState::None => {
                            // Remove the version only if it's not the only one.
                            // TODO: reason about the lifetime of metadata in version loads for dependencies, weak handles
                            if num_versions > 1 {
                                versions_to_remove.push(version_load.version);
                            }
                            LoadState::None
                        }
                        LoadState::WaitingForMetadata => {
                            if version_load.metadata.is_some() {
                                LoadState::RequestDependencies
                            } else {
                                LoadState::WaitingForMetadata
                            }
                        }
                        LoadState::RequestingMetadata => LoadState::RequestingMetadata,
                        LoadState::RequestDependencies => {
                            // Add ref to each of the dependent assets.
                            if let Some(artifact) = version_load.metadata.as_ref() {
                                for dependency_asset_id in &artifact.load_deps {
                                    if let AssetRef::Uuid(uuid) = dependency_asset_id {
                                        self.add_ref(*uuid);
                                    }
                                }
                            }

                            LoadState::WaitingForDependencies
                        }
                        LoadState::WaitingForDependencies => {
                            let asset_metadata = version_load.metadata.as_ref().unwrap();

                            // Ensure dependencies are loaded by engine before continuing to load this asset.
                            let asset_dependencies_committed =
                                asset_metadata.load_deps.iter().all(|dependency_asset_id| {
                                    self.uuid_to_load
                                        .get(dependency_asset_id.expect_uuid())
                                        .as_ref()
                                        .and_then(|dep_load_handle| {
                                            self.load_states.get(dep_load_handle)
                                        })
                                        .map(|dep_load| {
                                            // Note that we accept assets to be uncommitted but loaded
                                            // This is to support atomically committing a set of changes when hot reloading

                                            // TODO: Properly check that all dependencies have loaded their *new* version
                                            dep_load.versions.iter().all(|v| {
                                                matches!(
                                                    v.state,
                                                    LoadState::Loaded
                                                        | LoadState::LoadedUncommitted
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
                            {
                                let mut entry = self.load_states.get_mut(&key).unwrap();
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
                            }

                            if let Some(asset_metadata) = version_load.metadata.as_ref() {
                                asset_metadata
                                    .load_deps
                                    .iter()
                                    .for_each(|dependency_asset_id| {
                                        let uuid = dependency_asset_id.expect_uuid();
                                        // look up handle for uuid
                                        let dependency_load_handle =
                                            self.uuid_to_load.get(uuid).unwrap_or_else(|| {
                                                panic!(
                                                "Expected load handle to exist for asset `{:?}`.",
                                                uuid
                                            )
                                            });
                                        log::debug!("Removing ref from `{:?}`", uuid);
                                        // Remove reference from asset dependency.
                                        self.remove_ref(*dependency_load_handle)
                                    });
                            }

                            LoadState::Unloading
                        }
                        LoadState::Unloading => {
                            // Should we have confirmation from engine here?
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
                let mut entry = self.load_states.get_mut(&key).unwrap();

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
    fn process_metadata_requests(&self, io: &mut dyn LoaderIO) -> Result<(), capnp::Error> {
        while let Ok(mut response) = self.responses.metadata_rx.try_recv() {
            let request_data = &mut response.1;
            match response.0 {
                Ok(metadata_list) => {
                    for metadata in metadata_list {
                        let request_data = request_data.remove(&metadata.asset_id);
                        let load_handle = if let Some((handle, _)) = request_data {
                            handle
                        } else {
                            self.get_or_insert(metadata.asset_id)
                        };
                        let mut load = self
                            .load_states
                            .get_mut(&load_handle)
                            .expect("uuid in uuid_to_load but not in load_states");
                        log::trace!(
                            "received metadata for {:?} after {} secs",
                            load.asset_id,
                            std::time::Instant::now()
                                .duration_since(load.last_state_change_instant)
                                .as_secs_f32()
                        );
                        let version_load = load.versions.iter_mut().find(|v| {
                            if let Some((_, requesting_version)) = request_data {
                                v.version == requesting_version
                            } else {
                                v.metadata.is_none()
                            }
                        });
                        if let Some(version_load) = version_load {
                            version_load.metadata = Some(metadata);
                            if let LoadState::RequestingMetadata = version_load.state {
                                version_load.state = LoadState::RequestDependencies
                            }
                        } else {
                            load.version_counter += 1;
                            let new_version = load.version_counter;
                            load.versions.push(AssetVersionLoad {
                                asset_type: None,
                                auto_commit: true,
                                metadata: Some(metadata),
                                state: LoadState::None,
                                version: new_version,
                            });
                        }
                    }
                }
                Err(err) => {
                    error!("metadata request failed: {}", err);
                }
            }
            for (_, (handle, version)) in request_data {
                let mut load = self
                    .load_states
                    .get_mut(&handle)
                    .expect("load in metadata request but not in load_states");
                let version_load = load
                    .versions
                    .iter_mut()
                    .find(|v| v.version == *version)
                    .expect("load in metadata request but not in load.versions");
                if let LoadState::RequestingMetadata = version_load.state {
                    version_load.state = LoadState::WaitingForMetadata
                }
            }
        }
        let mut assets_to_request = HashMap::new();
        for mut entry in self.load_states.iter_mut() {
            let handle = *entry.key();
            let load = entry.value_mut();
            for version_load in &mut load.versions {
                if let LoadState::WaitingForMetadata = version_load.state {
                    version_load.state = LoadState::RequestingMetadata;
                    assets_to_request.insert(load.asset_id, (handle, version_load.version));
                }
            }
        }
        if !assets_to_request.is_empty() {
            io.get_asset_metadata_with_dependencies(MetadataRequest {
                metadata_tx: self.responses.metadata_tx.clone(),
                requests: assets_to_request,
                consumed: false,
            })
        }
        Ok(())
    }

    fn process_data_requests(
        &self,
        storage: &dyn AssetStorage,
        io: &mut dyn LoaderIO,
    ) -> Result<(), Box<dyn Error>> {
        while let Ok(response) = self.responses.data_rx.try_recv() {
            let result = response.0;
            let handle = response.1;
            let version = response.2;
            let load = self
                .load_states
                .get(&handle)
                .expect("load did not exist when data request completed");
            let load_result = match result {
                Ok(ref artifact_data) => {
                    let version_load = load
                        .versions
                        .iter()
                        .find(|v| v.version == version)
                        .expect("load version did not exist when data request completed");

                    let artifact_type = version_load.metadata.as_ref().unwrap().type_id;
                    let asset_id = load.asset_id;
                    log::trace!("asset data request succeeded for asset {:?}", load.asset_id);
                    // We don't want to be holding a lock to the load while calling AssetStorage::update_asset in `load_data`,
                    // so we drop the load ref, and save the state transition as a return value.
                    drop(load);
                    let update_result = storage.update_asset(
                        self,
                        &artifact_type,
                        artifact_data,
                        response.1,
                        AssetLoadOp::new(self.op_tx.clone(), handle, version),
                        response.2,
                    );
                    if let Err(storage_error) = update_result {
                        error!(
                            "AssetStorage implementor error when updating asset {:?}: {}",
                            asset_id, storage_error
                        );
                        AssetLoadResult::from_state(LoadState::WaitingForData)
                    } else {
                        AssetLoadResult {
                            asset_type: Some(artifact_type),
                            new_state: LoadState::LoadingAsset,
                            new_version: Some(response.2),
                        }
                    }
                }
                Err(err) => {
                    error!(
                        "asset data request failed for asset {:?}: {}",
                        load.asset_id, err
                    );
                    AssetLoadResult::from_state(LoadState::WaitingForMetadata)
                }
            };
            let mut load = self
                .load_states
                .get_mut(&response.1)
                .expect("load did not exist when data request completed");
            let version_load = load
                .versions
                .iter_mut()
                .find(|v| v.version == version)
                .expect("load version did not exist when data request completed");
            version_load.state = load_result.new_state;
            if let Some(asset_type) = load_result.asset_type {
                version_load.asset_type = Some(asset_type);
            }
            if let Some(version) = load_result.new_version {
                load.requested_version = Some(version);
            }
        }
        let mut assets_to_request = Vec::new();
        for mut load in self.load_states.iter_mut() {
            let handle = *load.key();
            let load = load.value_mut();

            if let Some(version_load) = load
                .versions
                .iter_mut()
                .find(|v| matches!(v.state, LoadState::WaitingForData))
            {
                version_load.state = LoadState::RequestingData;
                let artifact_id = version_load.metadata.as_ref().unwrap().id;
                assets_to_request.push(DataRequest {
                    data_tx: self.responses.data_tx.clone(),
                    asset_id: load.asset_id,
                    artifact_id,
                    handle,
                    version: version_load.version,
                    consumed: false,
                });
            }
        }
        if !assets_to_request.is_empty() {
            io.get_artifacts(assets_to_request);
        }
        Ok(())
    }
    fn process_load_ops(&self, asset_storage: &dyn AssetStorage) {
        while let Ok(op) = self.op_rx.try_recv() {
            match op {
                HandleOp::Error(_handle, _version, err) => {
                    panic!("load error {}", err);
                }
                HandleOp::Complete(handle, version) => {
                    let mut load = self
                        .load_states
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
    /// Checks for changed assets that need to be reloaded or unloaded
    fn process_asset_changes(
        &mut self,
        asset_storage: &dyn AssetStorage,
    ) -> Result<(), Box<dyn Error>> {
        if self.pending_reloads.is_empty() {
            // if we have no pending hot reloads, poll for new changes
            let mut changes = HashSet::new();
            while let Ok(asset) = self.invalidate_rx.try_recv() {
                changes.insert(asset);
            }
            if !changes.is_empty() {
                // TODO handle deleted assets
                for asset_id in &changes {
                    let current_version = self
                        .uuid_to_load
                        .get(asset_id)
                        .map(|l| *l)
                        .and_then(|load_handle| {
                            self.load_states
                                .get(&load_handle)
                                .map(|load| (load_handle, load))
                        })
                        .map(|(load_handle, load)| {
                            load.requested_version.map(|version| (load_handle, version))
                        })
                        .unwrap_or(None);
                    if let Some((handle, current_version)) = current_version {
                        let mut load = self
                            .load_states
                            .get_mut(&handle)
                            .expect("load state should exist for pending reload");
                        load.pending_reload = true;
                        self.pending_reloads.push(PendingReload {
                            asset_id: *asset_id,
                            version_before: current_version,
                        });
                    }
                }
            }
        } else {
            let is_finished = self.pending_reloads.iter().all(|reload| {
                self.uuid_to_load
                    .get(&reload.asset_id)
                    .as_ref()
                    .and_then(|load_handle| self.load_states.get(load_handle))
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
                self.pending_reloads.iter().for_each(|reload| {
                    if let Some((load_handle, mut load)) = self
                        .uuid_to_load
                        .get(&reload.asset_id)
                        .as_ref()
                        .and_then(|load_handle| {
                            self.load_states
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
                self.pending_reloads.clear();
            }
        }
        Ok(())
    }

    fn process_resolve_requests(
        requests: &mut IORequestChannels,
        data: &mut LoaderState,
        rpc: &mut dyn LoaderIO,
    ) -> Result<(), capnp::Error> {
        while let Ok(response) = requests.resolve_rx.try_recv() {
            match response.0 {
                Ok(candidates) => {}
                Err(e) => {}
            }
        }
        Ok(())
    }

    pub fn invalidate_assets(&self, assets: &[AssetUuid]) {
        for asset in assets {
            let _ = self.invalidate_tx.send(*asset);
        }
    }
}

pub struct Loader {
    io: Box<dyn LoaderIO>,
    data: LoaderState,
}

impl LoaderInfoProvider for LoaderState {
    fn get_load_handle(&self, id: &AssetRef) -> Option<LoadHandle> {
        self.uuid_to_load.get(id.expect_uuid()).map(|l| *l)
    }
    fn get_asset_id(&self, load: LoadHandle) -> Option<AssetUuid> {
        self.load_states.get(&load).map(|l| l.asset_id)
    }
}

impl Loader {
    pub fn new(io: Box<dyn LoaderIO>) -> Loader {
        Self::new_with_handle_allocator(io, Arc::new(AtomicHandleAllocator::default()))
    }
    pub fn new_with_handle_allocator(
        io: Box<dyn LoaderIO>,
        handle_allocator: Arc<dyn HandleAllocator>,
    ) -> Loader {
        let (op_tx, op_rx) = unbounded();
        let (invalidate_tx, invalidate_rx) = unbounded();
        let (metadata_tx, metadata_rx) = unbounded();
        let (data_tx, data_rx) = unbounded();
        let (resolve_tx, resolve_rx) = unbounded();
        Loader {
            data: LoaderState {
                handle_allocator,
                load_states: DashMap::default(),
                uuid_to_load: DashMap::default(),
                op_rx,
                op_tx,
                invalidate_rx,
                invalidate_tx,
                pending_reloads: Vec::new(),
                indirect_loads: DashMap::new(),
                indirect_table: IndirectionTable(Arc::new(DashMap::new())),
                responses: IORequestChannels {
                    metadata_rx,
                    metadata_tx,
                    data_tx,
                    data_rx,
                    resolve_tx,
                    resolve_rx,
                },
            },
            io,
        }
    }

    pub fn with_serde_context<R>(&self, tx: &Sender<RefOp>, mut f: impl FnMut() -> R) -> R {
        let mut result = None;
        self.io.with_runtime(&mut |runtime| {
            result =
                Some(runtime.block_on(SerdeContext::with(&self.data, tx.clone(), async { f() })));
        });
        result.unwrap()
    }

    /// Returns the load handle for the asset with the given UUID, if present.
    ///
    /// This will only return `Some(..)` if there has been a previous call to [`Loader::add_ref`].
    ///
    /// # Parameters
    ///
    /// * `id`: UUID of the asset.
    pub fn get_load(&self, id: AssetUuid) -> Option<LoadHandle> {
        self.data.uuid_to_load.get(&id).map(|l| *l)
    }
    /// Returns the number of references to an asset.
    ///
    /// **Note:** The information is true at the time the `LoadInfo` is retrieved. The actual number
    /// of references may change.
    ///
    /// # Parameters
    ///
    /// * `load_handle`: ID allocated by `atelier-assets` to track loading of the asset.
    pub fn get_load_info(&self, load: LoadHandle) -> Option<LoadInfo> {
        self.data.load_states.get(&load).map(|s| LoadInfo {
            asset_id: s.asset_id,
            refs: s.refs.load(Ordering::Relaxed) as u32,
        })
    }

    /// Returns the asset load status.
    ///
    /// # Parameters
    ///
    /// * `load_handle`: ID allocated by `atelier-assets` to track loading of the asset.
    pub fn get_load_status(&self, load: LoadHandle) -> LoadStatus {
        if let Some(load) = self.data.load_states.get(&load) {
            let version = if let Some(loaded_version) = load.loaded_version {
                load.versions.iter().find(|v| v.version == loaded_version)
            } else {
                load.versions.first().map(|v| v)
            };
            version
                .map(|v| match v.state {
                    LoadState::None => {
                        if load.refs.load(Ordering::Relaxed) > 0 {
                            LoadStatus::Loading
                        } else {
                            LoadStatus::NotRequested
                        }
                    }
                    LoadState::Loaded => LoadStatus::Loaded,
                    LoadState::UnloadRequested | LoadState::Unloading => LoadStatus::Unloading,
                    _ => LoadStatus::Loading,
                })
                .unwrap_or(LoadStatus::NotRequested)
        } else {
            LoadStatus::NotRequested
        }
    }
    /// Adds a reference to an asset and returns its [`LoadHandle`].
    ///
    /// If the asset is already loaded, this returns the existing [`LoadHandle`]. If it is not
    /// loaded, this allocates a new [`LoadHandle`] and returns that.
    ///
    /// # Parameters
    ///
    /// * `id`: UUID of the asset.
    pub fn add_ref(&self, id: AssetUuid) -> LoadHandle {
        self.data.add_ref(id)
    }
    /// Returns the `AssetType` UUID and load handle if the asset is loaded.
    ///
    /// # Parameters
    ///
    /// * `load_handle`: ID allocated by `atelier-assets` to track loading of the asset.
    pub fn get_asset(&self, load: LoadHandle) -> Option<(AssetTypeId, LoadHandle)> {
        self.data.get_asset(load)
    }
    /// Removes a reference to an asset.
    ///
    /// # Parameters
    ///
    /// * `load_handle`: ID allocated by `atelier-assets` to track loading of the asset.
    pub fn remove_ref(&self, load: LoadHandle) {
        self.data.remove_ref(load);
    }

    /// Processes pending load operations.
    ///
    /// Load operations include:
    ///
    /// * Requesting asset metadata.
    /// * Requesting asset data.
    /// * Committing completed [`AssetLoadOp`]s.
    /// * Updating the [`LoadStatus`]es of assets.
    ///
    /// # Parameters
    ///
    /// * `asset_storage`: Storage for all assets of all asset types.
    pub fn process(&mut self, asset_storage: &dyn AssetStorage) -> Result<(), Box<dyn Error>> {
        self.io.tick(&mut self.data);
        self.data.process_asset_changes(asset_storage)?;
        self.data.process_load_ops(asset_storage);
        self.data.process_load_states(asset_storage);
        self.data.process_metadata_requests(self.io.as_mut())?;
        self.data
            .process_data_requests(asset_storage, self.io.as_mut())?;
        Ok(())
    }

    /// Returns a reference to the loader's [`IndirectionTable`].
    ///
    /// When a user fetches an asset by LoadHandle, implementors of [`AssetStorage`]
    /// should resolve LoadHandles where [`LoadHandle::is_indirect`] returns true by using [`IndirectionTable::resolve`].
    /// IndirectionTable is Send + Sync + Clone so that it can be retrieved once at startup,
    /// then stored in implementors of [`AssetStorage`].
    pub fn indirection_table(&self) -> IndirectionTable {
        self.data.indirect_table.clone()
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{rpc_state::RpcIO, TypeUuid};
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
        loader: &mut Loader,
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

        let mut loader = Loader::new(Box::new(RpcIO::new(daemon_address).unwrap()));
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

        let mut loader = Loader::new(Box::new(RpcIO::new(daemon_address).unwrap()));
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
