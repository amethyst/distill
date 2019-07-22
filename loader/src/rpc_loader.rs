use crate::{
    loader::{AssetLoadOp, AssetStorage, HandleOp, LoadHandle, LoadInfo, LoadStatus, Loader},
    rpc_state::{ConnectionState, ResponsePromise, RpcState},
    AssetTypeId, AssetUuid,
};
use atelier_schema::{
    data::{artifact, asset_metadata},
    service::asset_hub::{
        snapshot::get_asset_metadata_with_dependencies_results::Owned as GetAssetMetadataWithDependenciesResults,
        snapshot::get_build_artifacts_results::Owned as GetBuildArtifactsResults,
    },
};
use ccl::dhashmap::DHashMap;
use crossbeam_channel::{unbounded, Receiver, Sender};
use futures::Future;
use log::{error, warn};
use std::{
    collections::HashMap,
    error::Error,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tokio::prelude::*;

#[derive(Copy, Clone, PartialEq, Debug)]
enum LoadState {
    None,
    WaitingForMetadata,
    RequestingMetadata,
    WaitingForData,
    RequestingData,
    LoadingAsset,
    LoadedPrecommit,
    Loaded,
    UnloadRequested,
    Unloading,
}

struct AssetLoad {
    asset_id: AssetUuid,
    state: LoadState,
    refs: AtomicUsize,
    asset_type: Option<AssetTypeId>,
    requested_version: Option<u32>,
    loaded_version: Option<u32>,
}
struct AssetMetadata {
    load_deps: Vec<AssetUuid>,
}

struct HandleAllocator(AtomicU64);
impl HandleAllocator {
    fn alloc(&self) -> LoadHandle {
        LoadHandle(self.0.fetch_add(1, Ordering::Relaxed))
    }
}

struct LoaderData {
    handle_allocator: HandleAllocator,
    load_states: DHashMap<LoadHandle, AssetLoad>,
    uuid_to_load: DHashMap<AssetUuid, LoadHandle>,
    metadata: HashMap<AssetUuid, AssetMetadata>,
    op_tx: Arc<Sender<HandleOp>>,
    op_rx: Receiver<HandleOp>,
}
struct RpcRequests {
    pending_data_requests: Vec<ResponsePromise<GetBuildArtifactsResults, LoadHandle>>,
    pending_metadata_requests:
        Vec<ResponsePromise<GetAssetMetadataWithDependenciesResults, Vec<(AssetUuid, LoadHandle)>>>,
}

unsafe impl Send for RpcRequests {}

impl LoaderData {
    fn add_ref(&self, id: AssetUuid) -> LoadHandle {
        let handle = self.uuid_to_load.get(&id).map(|h| *h);
        let handle = if let Some(handle) = handle {
            handle
        } else {
            *self.uuid_to_load.get_or_insert_with(&id, || {
                let new_handle = self.handle_allocator.alloc();
                self.load_states.insert(
                    new_handle,
                    AssetLoad {
                        asset_id: id,
                        state: LoadState::None,
                        refs: AtomicUsize::new(0),
                        asset_type: None,
                        requested_version: None,
                        loaded_version: None,
                    },
                );
                new_handle
            })
        };
        self.load_states
            .get(&handle)
            .map(|h| h.refs.fetch_add(1, Ordering::Relaxed));
        handle
    }
    fn get_asset(&self, load: &LoadHandle) -> Option<(AssetTypeId, LoadHandle)> {
        self.load_states
            .get(load)
            .filter(|a| a.state == LoadState::Loaded)
            .and_then(|a| a.asset_type.map(|t| (t, *load)))
    }
    fn remove_ref(&self, load: &LoadHandle) {
        self.load_states
            .get(load)
            .map(|h| h.refs.fetch_sub(1, Ordering::Relaxed));
    }
}

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
    fn get_load_info(&self, load: &LoadHandle) -> Option<LoadInfo> {
        self.data.load_states.get(load).map(|s| LoadInfo {
            asset_id: s.asset_id,
            refs: s.refs.load(Ordering::Relaxed) as u32,
        })
    }
    fn get_load_status(&self, load: &LoadHandle) -> LoadStatus {
        use LoadState::*;
        self.data
            .load_states
            .get(load)
            .map(|s| match s.state {
                None => LoadStatus::NotRequested,
                WaitingForMetadata | RequestingMetadata | WaitingForData | RequestingData
                | LoadingAsset | LoadedPrecommit => LoadStatus::Loading,
                Loaded => {
                    if let Some(_) = s.loaded_version {
                        LoadStatus::Loaded
                    } else {
                        LoadStatus::Loading
                    }
                }
                UnloadRequested | Unloading => LoadStatus::Unloading,
            })
            .unwrap_or(LoadStatus::NotRequested)
    }
    fn add_ref(&self, id: AssetUuid) -> LoadHandle {
        self.data.add_ref(id)
    }
    fn get_asset(&self, load: &LoadHandle) -> Option<(AssetTypeId, LoadHandle)> {
        self.data.get_asset(load)
    }
    fn remove_ref(&self, load: &LoadHandle) {
        self.data.remove_ref(load)
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
        {
            process_load_ops(&mut self.data.load_states, &self.data.op_rx);
            process_load_states(
                asset_storage,
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

impl RpcLoader {
    pub fn new(connect_string: String) -> std::io::Result<RpcLoader> {
        let (tx, rx) = unbounded();
        Ok(RpcLoader {
            connect_string: connect_string,
            data: LoaderData {
                handle_allocator: HandleAllocator(AtomicU64::new(1)),
                load_states: DHashMap::default(),
                uuid_to_load: DHashMap::default(),
                metadata: HashMap::new(),
                op_rx: rx,
                op_tx: Arc::new(tx),
            },
            rpc: Arc::new(Mutex::new(RpcState::new()?)),
            requests: Mutex::new(RpcRequests {
                pending_metadata_requests: Vec::new(),
                pending_data_requests: Vec::new(),
            }),
        })
    }
}

fn update_asset_metadata(
    metadata: &mut HashMap<AssetUuid, AssetMetadata>,
    uuid: &AssetUuid,
    reader: &asset_metadata::Reader<'_>,
) -> Result<(), capnp::Error> {
    let mut load_deps = Vec::new();
    for dep in reader.get_load_deps()? {
        load_deps.push(make_array(dep.get_id()?));
    }
    metadata.insert(*uuid, AssetMetadata { load_deps });
    Ok(())
}

fn load_data(
    chan: &Arc<Sender<HandleOp>>,
    handle: &LoadHandle,
    state: &mut AssetLoad,
    reader: &artifact::Reader<'_>,
    storage: &dyn AssetStorage,
) -> Result<LoadState, Box<dyn Error>> {
    assert!(
        LoadState::RequestingData == state.state || LoadState::Loaded == state.state,
        "LoadState::RequestingData == {:?} || LoadState::Loaded == {:?}",
        state.state,
        state.state
    );
    let serialized_asset = reader.get_data()?;
    let asset_type: AssetTypeId = make_array(serialized_asset.get_type_uuid()?);
    if let Some(prev_type) = state.asset_type {
        // TODO handle asset type changing?
        assert!(prev_type == asset_type);
    } else {
        state.asset_type.replace(asset_type);
    }
    let new_version = state.requested_version.unwrap_or(0) + 1;
    state.requested_version = Some(new_version);
    storage.update_asset(
        &asset_type,
        &serialized_asset.get_data()?,
        handle,
        AssetLoadOp::new(chan.clone(), *handle),
        new_version,
    )?;
    if state.loaded_version.is_none() {
        Ok(LoadState::LoadingAsset)
    } else {
        Ok(LoadState::Loaded)
    }
}

fn process_pending_requests<T, U, ProcessFunc>(
    requests: &mut Vec<ResponsePromise<T, U>>,
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
        let result: Result<Async<()>, Box<dyn Error>> = match request.poll() {
            Ok(Async::Ready(response)) => {
                process_request_func(Ok(response), request.get_user_data()).map(|r| Async::Ready(r))
            }
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(err) => Err(err),
        };
        match result {
            Err(err) => {
                let _ = process_request_func(Err(err), request.get_user_data());
                requests.swap_remove(i);
            }
            Ok(Async::Ready(_)) => {
                requests.swap_remove(i);
            }
            Ok(Async::NotReady) => {}
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
    process_pending_requests(&mut requests.pending_data_requests, |result, handle| {
        let mut load = data
            .load_states
            .get_mut(handle)
            .expect("load did not exist when data request completed");
        load.state = match result {
            Ok(reader) => {
                let reader = reader.get()?;
                let artifacts = reader.get_artifacts()?;
                if artifacts.len() == 0 {
                    warn!(
                        "asset data request did not return any data for asset {:?}",
                        load.asset_id
                    );
                    LoadState::WaitingForData
                } else {
                    load_data(op_channel, &handle, &mut load, &artifacts.get(0), storage)?
                }
            }
            Err(err) => {
                error!(
                    "asset data request failed for asset {:?}: {}",
                    load.asset_id, err
                );
                LoadState::WaitingForData
            }
        };
        Ok(())
    });
    if let ConnectionState::Connected = rpc.connection_state() {
        let mut assets_to_request = Vec::new();
        for mut chunk in data.load_states.chunks_write() {
            assets_to_request.extend(
                chunk
                    .iter_mut()
                    .filter(|(_, v)| {
                        if let LoadState::WaitingForData = v.state {
                            true
                        } else {
                            false
                        }
                    })
                    .map(|(k, v)| {
                        v.state = LoadState::RequestingData;
                        (v.asset_id, *k)
                    }),
            );
        }
        if assets_to_request.len() > 0 {
            for (asset, handle) in assets_to_request {
                let response = rpc.request(move |_conn, snapshot| {
                    let mut request = snapshot.get_build_artifacts_request();
                    let mut assets = request.get().init_assets(1);
                    assets.reborrow().get(0).set_id(&asset);
                    (request, handle)
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
                            let mut state = load_states
                                .get_mut(&*load_handle)
                                .expect("uuid in uuid_to_load but not in load_states");
                            if let LoadState::RequestingMetadata = state.state {
                                state.state = LoadState::WaitingForData
                            }
                        }
                    }
                    for (_, load_handle) in requested_assets {
                        let mut state = load_states
                            .get_mut(load_handle)
                            .expect("uuid in uuid_to_load but not in load_states");
                        if let LoadState::RequestingMetadata = state.state {
                            state.state = LoadState::WaitingForMetadata
                        }
                    }
                }
                Err(err) => {
                    error!("metadata request failed: {}", err);
                    for (_, load_handle) in requested_assets {
                        let mut state = load_states
                            .get_mut(load_handle)
                            .expect("uuid in uuid_to_load but not in load_states");
                        if let LoadState::RequestingMetadata = state.state {
                            state.state = LoadState::WaitingForMetadata
                        }
                    }
                }
            };
            Ok(())
        },
    );
    if let ConnectionState::Connected = rpc.connection_state() {
        let mut assets_to_request = Vec::new();
        for mut chunk in load_states.chunks_write() {
            assets_to_request.extend(
                chunk
                    .iter_mut()
                    .filter(|(_, v)| {
                        if let LoadState::WaitingForMetadata = v.state {
                            true
                        } else {
                            false
                        }
                    })
                    .map(|(k, v)| {
                        v.state = LoadState::RequestingMetadata;
                        (v.asset_id, *k)
                    }),
            );
        }
        if assets_to_request.len() > 0 {
            let response = rpc.request(move |_conn, snapshot| {
                let mut request = snapshot.get_asset_metadata_with_dependencies_request();
                let mut assets = request.get().init_assets(assets_to_request.len() as u32);
                for (idx, (asset, _)) in assets_to_request.iter().enumerate() {
                    assets.reborrow().get(idx as u32).set_id(asset);
                }
                (request, assets_to_request)
            });
            requests.pending_metadata_requests.push(response);
        }
    }
    Ok(())
}

fn process_load_ops(load_states: &mut DHashMap<LoadHandle, AssetLoad>, op_rx: &Receiver<HandleOp>) {
    while let Ok(op) = op_rx.try_recv() {
        match op {
            HandleOp::LoadError(_handle, err) => {
                panic!("load error {}", err);
            }
            HandleOp::LoadComplete(handle) => {
                let mut load = load_states
                    .get_mut(&handle)
                    .expect("load op completed but load state does not exist");
                if let LoadState::LoadingAsset = load.state {
                    load.state = LoadState::LoadedPrecommit;
                } else {
                    panic!("load op completed but load state is {:?}", load.state);
                }
            }
            HandleOp::LoadDrop(_handle) => panic!("load op dropped without calling complete/error"),
        }
    }
}

fn process_load_states(
    asset_storage: &dyn AssetStorage,
    load_states: &mut DHashMap<LoadHandle, AssetLoad>,
    uuid_to_load: &DHashMap<AssetUuid, LoadHandle>,
    metadata: &HashMap<AssetUuid, AssetMetadata>,
) {
    let mut to_remove = Vec::new();
    for mut chunk in load_states.chunks_write() {
        for (key, mut value) in chunk.iter_mut() {
            let new_state = match value.state {
                LoadState::None if value.refs.load(Ordering::Relaxed) > 0 => {
                    if metadata.contains_key(&value.asset_id) {
                        LoadState::WaitingForData
                    } else {
                        LoadState::WaitingForMetadata
                    }
                }
                LoadState::None => {
                    // no refs, inactive load
                    LoadState::UnloadRequested
                }
                LoadState::WaitingForMetadata => {
                    if metadata.contains_key(&value.asset_id) {
                        LoadState::WaitingForData
                    } else {
                        LoadState::WaitingForMetadata
                    }
                }
                LoadState::RequestingMetadata => LoadState::RequestingMetadata,
                LoadState::WaitingForData => {
                    log::info!("waiting for data");
                    if value.asset_type.is_some() {
                        LoadState::LoadingAsset
                    } else {
                        LoadState::WaitingForData
                    }
                }
                LoadState::RequestingData => LoadState::RequestingData,
                LoadState::LoadingAsset => LoadState::LoadingAsset,
                LoadState::LoadedPrecommit => {
                    let asset_type = value
                        .asset_type
                        .as_ref()
                        .expect("in `LoadedPrecommit` state but asset_type is None");

                    // Ensure dependencies are committed before committing this asset.
                    // load is an AssetLoad.
                    let asset_dependencies_committed = {
                        // need to map from  load: AssetLoad to its AssetMetadata, to get load_deps
                        let asset_id = value.asset_id;
                        let asset_metadata = metadata.get(&asset_id).unwrap_or_else(|| {
                            panic!("Expected metadata for asset `{:?}` to exist.", asset_id)
                        });
                        asset_metadata.load_deps.iter().all(|dependency_asset_id| {
                            uuid_to_load
                                .get(dependency_asset_id)
                                .as_ref()
                                .and_then(|dep_load_handle| load_states.get(dep_load_handle))
                                .map(|dep_load| dep_load.state == LoadState::Loaded)
                                .unwrap_or(false)
                        })
                    };

                    if asset_dependencies_committed {
                        asset_storage.commit_asset_version(
                            asset_type,
                            &key,
                            value.requested_version.unwrap(),
                        );
                        value.loaded_version = value.requested_version;
                        LoadState::Loaded
                    } else {
                        LoadState::LoadedPrecommit
                    }
                }
                LoadState::Loaded => {
                    if value.refs.load(Ordering::Relaxed) <= 0 {
                        LoadState::UnloadRequested
                    } else {
                        LoadState::Loaded
                    }
                }
                LoadState::UnloadRequested => {
                    if let Some(asset_type) = value.asset_type.take() {
                        asset_storage.free(&asset_type, *key);
                        value.requested_version = None;
                        value.loaded_version = None;
                    }
                    LoadState::Unloading
                }
                LoadState::Unloading => {
                    if value.refs.load(Ordering::Relaxed) <= 0 {
                        to_remove.push(*key);
                    }
                    LoadState::None
                }
            };
            value.state = new_state;
        }
    }
    for i in to_remove {
        load_states.remove(&i);
    }
}

impl Default for RpcLoader {
    fn default() -> RpcLoader {
        RpcLoader::new("127.0.0.1:9999".to_string()).unwrap()
    }
}

fn make_array<A, T>(slice: &[T]) -> A
where
    A: Sized + Default + AsMut<[T]>,
    T: Copy,
{
    let mut a = Default::default();
    <A as AsMut<[T]>>::as_mut(&mut a).copy_from_slice(slice);
    a
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TypeUuid;
    use atelier_daemon::{init_logging, AssetDaemon};
    use atelier_importer::{AssetUUID, BoxedImporter, ImportedAsset, Importer, ImporterValue};
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
            _asset_type: &AssetTypeId,
            data: &[u8],
            loader_handle: &LoadHandle,
            load_op: AssetLoadOp,
            version: u32,
        ) -> Result<(), Box<dyn Error>> {
            println!(
                "update asset {:?} data size {}",
                loader_handle,
                data.as_ref().len()
            );
            let mut map = self.map.write().unwrap();
            let state = map.entry(*loader_handle).or_insert(LoadState {
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
            loader_handle: &LoadHandle,
            version: u32,
        ) {
            println!("commit asset {:?}", loader_handle,);
            let mut map = self.map.write().unwrap();
            let state = map.get_mut(loader_handle).unwrap();

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
    impl Importer for TxtImporter {
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

        fn import(
            &self,
            source: &mut dyn Read,
            txt_format: Self::Options,
            state: &mut Self::State,
        ) -> atelier_importer::Result<ImporterValue> {
            if state.id.is_none() {
                state.id = Some(*uuid::Uuid::new_v4().as_bytes());
            }
            let mut bytes = Vec::new();
            source.read_to_end(&mut bytes)?;
            let parsed_asset_data = txt_format
                .from_utf8(bytes)
                .expect("Failed to construct string asset.");

            let load_deps = parsed_asset_data
                .lines()
                .filter_map(|line| Uuid::from_str(line).ok())
                .map(|uuid| *uuid.as_bytes())
                .collect::<Vec<AssetUUID>>();

            Ok(ImporterValue {
                assets: vec![ImportedAsset {
                    id: state.id.expect("AssetUUID not generated"),
                    search_tags: Vec::new(),
                    build_deps: Vec::new(),
                    load_deps,
                    instantiate_deps: Vec::new(),
                    asset_data: Box::new(parsed_asset_data),
                    build_pipeline: None,
                }],
            })
        }
    }

    fn wait_for_status(
        status: LoadStatus,
        handle: &LoadHandle,
        loader: &mut RpcLoader,
        storage: &Storage,
    ) {
        loop {
            println!("state {:?}", loader.get_load_status(&handle));
            if std::mem::discriminant(&status)
                == std::mem::discriminant(&loader.get_load_status(&handle))
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
            *uuid::Uuid::parse_str("60352042-616f-460e-abd2-546195c060fe")
                .unwrap()
                .as_bytes(),
        );
        let storage = &mut Storage {
            map: RwLock::new(HashMap::new()),
        };
        wait_for_status(LoadStatus::Loaded, &handle, &mut loader, &storage);
        loader.remove_ref(&handle);
        wait_for_status(LoadStatus::NotRequested, &handle, &mut loader, &storage);
    }

    #[test]
    fn test_load_with_dependencies() {
        let _ = init_logging(); // Another test may have initialized logging, so we ignore errors.

        // Start daemon in a separate thread
        let daemon_port = 9999;
        let daemon_address = format!("127.0.0.1:{}", daemon_port);
        let _atelier_daemon = spawn_daemon(&daemon_address);

        let mut loader = RpcLoader::new(daemon_address).expect("Failed to construct `RpcLoader`.");
        let handle = loader.add_ref(
            // asset uuid of "tests/assets/asset_a.txt"
            *uuid::Uuid::parse_str("a5ce4da0-675e-4460-be02-c8b145c2ee49")
                .unwrap()
                .as_bytes(),
        );
        let storage = &mut Storage {
            map: RwLock::new(HashMap::new()),
        };
        wait_for_status(LoadStatus::Loaded, &handle, &mut loader, &storage);

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
                    std::mem::discriminant(&loader.get_load_status(&asset_load_handle)),
                    "Expected `{}` to be loaded.",
                    file_name
                );
            });

        // Remove reference to top level asset.
        loader.remove_ref(&handle);
        wait_for_status(LoadStatus::NotRequested, &handle, &mut loader, &storage);

        asset_handles
            .iter()
            .for_each(|(asset_load_handle, file_name)| {
                assert_eq!(
                    std::mem::discriminant(&LoadStatus::NotRequested),
                    std::mem::discriminant(&loader.get_load_status(&asset_load_handle)),
                    "Expected `{}` to be loaded.",
                    file_name
                );
            });
    }

    fn asset_tree() -> Vec<(AssetUUID, &'static str)> {
        [
            ("a5ce4da0-675e-4460-be02-c8b145c2ee49", "asset_a.txt"),
            ("039dc5f8-ee1c-4949-a7df-72383f12c7a2", "asset_b.txt"),
            ("c071f3ff-c9ea-4bf5-b3b9-bf5fc29f9b59", "asset_c.txt"),
            ("55adb689-b91c-42a0-941b-de4a9f7f4f03", "asset_d.txt"),
        ]
        .into_iter()
        .map(|(id, file_name)| {
            let asset_uuid = *uuid::Uuid::parse_str(id)
                .unwrap_or_else(|_| panic!("Failed to parse `{}` as `Uuid`.", id))
                .as_bytes();

            (asset_uuid, *file_name)
        })
        .collect::<Vec<(AssetUUID, &'static str)>>()
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
                    .with_db_path(tests_path.join("db"))
                    .with_address(daemon_address)
                    .with_importers(std::iter::once((
                        "txt",
                        Box::new(TxtImporter) as Box<dyn BoxedImporter>,
                    )))
                    .with_asset_dirs(vec![tests_path.join("assets")])
                    .run();
            })
            .expect("Failed to spawn `atelier-daemon` thread.")
    }

}
