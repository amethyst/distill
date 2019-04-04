use crate::{
    loader::{AssetLoadOp, AssetStorage, HandleOp, LoadHandle, LoadInfo, LoadStatus, Loader},
    rpc_state::{ConnectionState, ResponsePromise, RpcState},
    AssetTypeId, AssetUuid,
};
use capnp::{capability::Response, message::ReaderOptions, Result as CapnpResult};
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use crossbeam_channel::{unbounded, Receiver, Sender};
use futures::Future;
use log::{error, warn};
use schema::{
    data::{artifact, asset_metadata},
    service::asset_hub::{
        self,
        snapshot::get_asset_metadata_with_dependencies_results::Owned as GetAssetMetadataWithDependenciesResults,
        snapshot::get_build_artifacts_results::Owned as GetBuildArtifactsResults,
    },
};
use slotmap::{Key, KeyData, SlotMap};
use std::{
    cell::RefCell,
    collections::HashMap,
    error::Error,
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, RwLock,
    },
};
use tokio::prelude::*;
use tokio_current_thread::CurrentThread;

impl From<KeyData> for LoadHandle {
    fn from(key: KeyData) -> Self {
        Self(key.as_ffi())
    }
}
impl Into<KeyData> for LoadHandle {
    fn into(self) -> KeyData {
        KeyData::from_ffi(self.0)
    }
}
impl Key for LoadHandle {}
struct AssetUuidKey(AssetUuid);

#[derive(Copy, Clone, PartialEq)]
enum AssetState {
    None,
    WaitingForMetadata,
    RequestingMetadata,
    WaitingForData,
    RequestingData,
    LoadingData,
    LoadingAsset,
    Loaded,
    UnloadRequested,
    Unloading,
}

struct AssetStatus<Handle> {
    asset_id: AssetUuid,
    state: AssetState,
    refs: AtomicUsize,
    asset_handle: Option<(AssetTypeId, Handle)>,
}
struct AssetMetadata {
    load_deps: Vec<AssetUuid>,
}

struct LoaderData<HandleType> {
    load_states: RwLock<SlotMap<LoadHandle, AssetStatus<HandleType>>>,
    uuid_to_load: RwLock<HashMap<AssetUuid, LoadHandle>>,
    metadata: HashMap<AssetUuid, AssetMetadata>,
    pending_data_requests: Vec<ResponsePromise<GetBuildArtifactsResults, LoadHandle>>,
    pending_metadata_requests:
        Vec<ResponsePromise<GetAssetMetadataWithDependenciesResults, Vec<(AssetUuid, LoadHandle)>>>,
    op_tx: Sender<HandleOp>,
    op_rx: Receiver<HandleOp>,
}

impl<HandleType: Clone> LoaderData<HandleType> {
    fn add_ref(&self, id: AssetUuid) -> LoadHandle {
        let handle = self.uuid_to_load.read().unwrap().get(&id).map(|h| *h);
        let handle = if let Some(handle) = handle {
            handle
        } else {
            let mut load_states = self.load_states.write().unwrap();
            *self
                .uuid_to_load
                .write()
                .unwrap()
                .entry(id)
                .or_insert_with(|| {
                    load_states.insert(AssetStatus {
                        asset_id: id,
                        state: AssetState::None,
                        refs: AtomicUsize::new(0),
                        asset_handle: None,
                    })
                })
        };
        let load_states = self.load_states.read().unwrap();
        load_states
            .get(handle)
            .map(|h| h.refs.fetch_add(1, Ordering::Relaxed));
        handle
    }
    fn get_asset(&self, load: &LoadHandle) -> Option<(AssetTypeId, HandleType, LoadHandle)> {
        self.load_states
            .read()
            .unwrap()
            .get(*load)
            .filter(|a| {
                if let AssetState::Loaded = a.state {
                    true
                } else {
                    false
                }
            })
            .map(|a| a.asset_handle.clone().map(|(t, a)| (t, a, *load)))
            .unwrap_or(None)
    }
    fn remove_ref(&self, load: &LoadHandle) {
        self.load_states
            .write()
            .unwrap()
            .get_mut(*load)
            .map(|h| h.refs.fetch_sub(1, Ordering::Relaxed));
    }
}

pub struct RpcLoader<HandleType> {
    connect_string: String,
    rpc: Arc<Mutex<RpcState>>,
    data: LoaderData<HandleType>,
}

impl<HandleType: Clone> Loader for RpcLoader<HandleType> {
    type HandleType = HandleType;
    fn get_load(&self, id: AssetUuid) -> Option<LoadHandle> {
        self.data.uuid_to_load.read().unwrap().get(&id).map(|l| *l)
    }
    fn get_load_info(&self, load: &LoadHandle) -> Option<LoadInfo> {
        self.data
            .load_states
            .read()
            .unwrap()
            .get(*load)
            .map(|s| LoadInfo {
                asset_id: s.asset_id,
                refs: s.refs.load(Ordering::Relaxed) as u32,
            })
    }
    fn get_load_status(&self, load: &LoadHandle) -> LoadStatus {
        use AssetState::*;
        self.data
            .load_states
            .read()
            .unwrap()
            .get(*load)
            .map(|s| match s.state {
                None => LoadStatus::NotRequested,
                WaitingForMetadata | RequestingMetadata | WaitingForData | RequestingData
                | LoadingData | LoadingAsset => LoadStatus::Loading,
                Loaded => LoadStatus::Loaded,
                UnloadRequested | Unloading => LoadStatus::Unloading,
            })
            .unwrap_or(LoadStatus::NotRequested)
    }
    fn add_ref(&self, id: AssetUuid) -> LoadHandle {
        self.data.add_ref(id)
    }
    fn get_asset(&self, load: &LoadHandle) -> Option<(AssetTypeId, Self::HandleType, LoadHandle)> {
        self.data.get_asset(load)
    }
    fn remove_ref(&self, load: &LoadHandle) {
        self.data.remove_ref(load)
    }
    fn process(
        &mut self,
        asset_storage: &dyn AssetStorage<HandleType = Self::HandleType>,
    ) -> Result<(), Box<dyn Error>> {
        let mut rpc = self.rpc.lock().expect("rpc mutex poisoned");
        match rpc.connection_state() {
            ConnectionState::Error(err) => {
                error!("Error connecting RPC: {}", err);
                rpc.connect(&self.connect_string);
            }
            ConnectionState::None => rpc.connect(&self.connect_string),
            _ => {}
        };
        rpc.poll();
        for (key, mut value) in self.data.load_states.write().unwrap().iter_mut() {
            let new_state = match value.state {
                AssetState::None if value.refs.load(Ordering::Relaxed) > 0 => {
                    if self.data.metadata.contains_key(&value.asset_id) {
                        AssetState::WaitingForData
                    } else {
                        AssetState::WaitingForMetadata
                    }
                }
                AssetState::None => {
                    // no refs, inactive load
                    AssetState::UnloadRequested
                }
                AssetState::WaitingForMetadata => {
                    log::info!("waiting for metadata");
                    if self.data.metadata.contains_key(&value.asset_id) {
                        AssetState::WaitingForData
                    } else {
                        AssetState::WaitingForMetadata
                    }
                }
                AssetState::RequestingMetadata => AssetState::RequestingMetadata,
                AssetState::WaitingForData => {
                    log::info!("waiting for data");
                    if value.asset_handle.is_some() {
                        AssetState::LoadingAsset
                    } else {
                        AssetState::WaitingForData
                    }
                }
                AssetState::RequestingData => AssetState::RequestingData,
                AssetState::LoadingData => AssetState::LoadingData,
                AssetState::LoadingAsset => {
                    log::info!("loading asset");
                    if let Some((ref asset_type, ref asset_handle)) = value.asset_handle {
                        if asset_storage.is_loaded(asset_type, asset_handle, &key) {
                            log::info!("loaded asset {:?}", value.asset_id);
                            AssetState::Loaded
                        } else {
                            AssetState::LoadingAsset
                        }
                    } else {
                        panic!("in LoadingAsset state without an asset handle");
                    }
                }
                AssetState::Loaded => AssetState::Loaded,
                AssetState::UnloadRequested => AssetState::UnloadRequested,
                AssetState::Unloading => AssetState::Unloading,
            };
            value.state = new_state;
        }
        // process_metadata_requests(&mut self.data, &mut rpc)?;
        process_data_requests(&mut self.data, asset_storage, &mut rpc)?;
        Ok(())
    }
}
impl<HandleType> RpcLoader<HandleType> {
    pub fn new() -> std::io::Result<RpcLoader<HandleType>> {
        let (tx, rx) = unbounded();
        Ok(RpcLoader {
            connect_string: "".to_string(),
            data: LoaderData {
                load_states: RwLock::new(SlotMap::with_key()),
                uuid_to_load: RwLock::new(HashMap::new()),
                metadata: HashMap::new(),
                pending_data_requests: Vec::new(),
                pending_metadata_requests: Vec::new(),
                op_rx: rx,
                op_tx: tx,
            },
            rpc: Arc::new(Mutex::new(RpcState::new()?)),
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
    println!("updated metadata for {:?}", uuid);
    metadata.insert(*uuid, AssetMetadata { load_deps });
    Ok(())
}

fn load_data<HandleType>(
    handle: &LoadHandle,
    state: &mut AssetStatus<HandleType>,
    reader: &artifact::Reader<'_>,
    storage: &dyn AssetStorage<HandleType = HandleType>,
) -> Result<AssetState, Box<dyn Error>> {
    assert!(AssetState::RequestingData != state.state && AssetState::Loaded != state.state);
    let serialized_asset = reader.get_data()?;
    let asset_type: AssetTypeId = make_array(serialized_asset.get_type_uuid()?);
    println!("loaded data of size {}", serialized_asset.get_data()?.len());
    if state.asset_handle.is_none() {
        state.asset_handle.replace((
            asset_type,
            storage.allocate(&asset_type, &state.asset_id, handle),
        ));
    }
    storage.update_asset(
        &asset_type,
        &state.asset_handle.as_ref().unwrap().1,
        &serialized_asset.get_data()?,
        handle,
    )?;
    Ok(AssetState::LoadingData)
}

fn process_pending_requests<T, U, ProcessFunc>(
    requests: &mut Vec<ResponsePromise<T, U>>,
    mut process_request_func: ProcessFunc,
) where
    ProcessFunc: for<'a> FnMut(
        Result<<T as capnp::traits::Owned<'a>>::Reader, Box<dyn Error>>,
        &mut U,
    ) -> Result<(), Box<dyn Error>>,
    T: capnp::traits::Pipelined + for<'a> capnp::traits::Owned<'a> + 'static,
{
    // reverse range so we can remove inside the loop without consequence
    for i in (0..requests.len()).rev() {
        let mut request = requests
            .get_mut(i)
            .expect("invalid iteration logic when processing RPC requests");
        let result: Result<Async<()>, Box<dyn Error>> = match request.poll() {
            Ok(Async::Ready(response)) => match response.get() {
                Ok(response) => process_request_func(Ok(response), request.get_user_data())
                    .map(|r| Async::Ready(r)),
                Err(err) => Err(Box::new(err)),
            },
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

fn process_data_requests<HandleType>(
    data: &mut LoaderData<HandleType>,
    storage: &dyn AssetStorage<HandleType = HandleType>,
    rpc: &mut RpcState,
) -> Result<(), Box<dyn Error>> {
    let mut load_states = data.load_states.get_mut().unwrap();
    process_pending_requests(&mut data.pending_data_requests, |result, handle| {
        let load = load_states
            .get_mut(*handle)
            .expect("load did not exist when data request completed");
        load.state = match result {
            Ok(reader) => {
                let artifacts = reader.get_artifacts()?;
                if artifacts.len() == 0 {
                    warn!(
                        "asset data request did not return any data for asset {:?}",
                        load.asset_id
                    );
                    AssetState::WaitingForData
                } else {
                    load_data(&handle, load, &artifacts.get(0), storage)?
                }
            }
            Err(err) => {
                error!(
                    "asset data request failed for asset {:?}: {}",
                    load.asset_id, err
                );
                AssetState::WaitingForData
            }
        };
        Ok(())
    });
    let assets_to_request: Vec<_> = load_states
        .iter_mut()
        .filter(|(_, v)| {
            if let AssetState::WaitingForData = v.state {
                true
            } else {
                false
            }
        })
        .map(|(k, v)| {
            v.state = AssetState::RequestingData;
            (v.asset_id, k)
        })
        .collect();
    if assets_to_request.len() > 0 {
        for (asset, handle) in assets_to_request {
            let response = rpc.request(move |conn, snapshot| {
                let mut request = snapshot.get_build_artifacts_request();
                let mut assets = request.get().init_assets(1);
                assets.reborrow().get(0).set_id(&asset);
                (request, handle)
            });
            data.pending_data_requests.push(response);
        }
    }
    Ok(())
}

fn process_metadata_requests<HandleType>(
    data: &mut LoaderData<HandleType>,
    rpc: &mut RpcState,
) -> Result<(), capnp::Error> {
    let mut load_states = data.load_states.get_mut().unwrap();
    let mut uuid_to_load = data.uuid_to_load.get_mut().unwrap();
    let metadata = &mut data.metadata;
    process_pending_requests(&mut data.pending_metadata_requests, |result, requested_assets| {
        match result {
            Ok(reader) => {
                let assets = reader.get_assets()?;
                for asset in assets {
                    let asset_uuid: AssetUuid = make_array(asset.get_id()?.get_id()?);
                    update_asset_metadata(metadata, &asset_uuid, &asset)?;
                    if let Some(load_handle) = uuid_to_load.get(&asset_uuid) {
                        let state = load_states.get_mut(*load_handle).expect("uuid in uuid_to_load but not in load_states");
                        if let AssetState::RequestingMetadata = state.state {
                            state.state = AssetState::WaitingForData
                        }
                    }
                }
                for (_, load_handle) in requested_assets {
                    let state = load_states.get_mut(*load_handle).expect("uuid in uuid_to_load but not in load_states");
                    if let AssetState::RequestingMetadata = state.state {
                        state.state = AssetState::WaitingForMetadata
                    }
                }
            }
            Err(err) => {
                error!(
                    "metadata request failed: {}",
                    err
                );
                for (_, load_handle) in requested_assets {
                    let state = load_states.get_mut(*load_handle).expect("uuid in uuid_to_load but not in load_states");
                    if let AssetState::RequestingMetadata = state.state {
                        state.state = AssetState::WaitingForMetadata
                    }
                }
            }
        };
        Ok(())
    });
    let mut load_states = data.load_states.get_mut().unwrap();
    let assets_to_request: Vec<_> = load_states
        .iter_mut()
        .filter(|(_, v)| {
            if let AssetState::WaitingForMetadata = v.state {
                true
            } else {
                false
            }
        })
        .map(|(k, v)| {
            v.state = AssetState::RequestingMetadata;
            (v.asset_id, k)
        })
        .collect();
    if assets_to_request.len() > 0 {
        let response = rpc.request(move |conn, snapshot| {
            let mut request = snapshot.get_asset_metadata_with_dependencies_request();
            let mut assets = request.get().init_assets(assets_to_request.len() as u32);
            for (idx, (asset, _)) in assets_to_request.iter().enumerate() {
                assets.reborrow().get(idx as u32).set_id(asset);
            }
            (request, assets_to_request)
        });
        data.pending_metadata_requests.push(response);
    }
    Ok(())
}

impl<S> Default for RpcLoader<S> {
    fn default() -> RpcLoader<S> {
        RpcLoader::new().unwrap()
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
