use crate::{
    loader::{Loader, AssetStorage},
    AssetUuid, AssetTypeId,
};
use capnp::{capability::Response, message::ReaderOptions, Result as CapnpResult};
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{
    executor::spawn,
    sync::oneshot::{channel, Receiver},
    Future,
};
use log::error;
use schema::{
    data::{artifact, asset_metadata},
    service::asset_hub::{
        self,
        snapshot::get_asset_metadata_with_dependencies_results::Owned as GetAssetMetadataWithDependenciesResults,
        snapshot::get_build_artifacts_results::Owned as GetBuildArtifactsResults,
    },
};
use serde_dyn::TypeUuid;
use std::{cell::RefCell, collections::HashMap, error::Error, rc::Rc};
use tokio::prelude::*;
use tokio_current_thread::CurrentThread;

type Promise<T> = capnp::capability::Promise<T, capnp::Error>;

enum AssetState {
    None,
    WaitingForMetadata,
    RequestedMetadata,
    WaitingForData,
    LoadingData,
    LoadingAsset,
    Loaded,
    UnloadRequested,
    Unloading,
}

struct AssetStatus<Handle> {
    state: AssetState,
    refs: u32,
    asset_handle: Option<Handle>,
}
struct AssetMetadata {
    load_deps: Vec<AssetUuid>,
}

struct RpcConnection {
    asset_hub: asset_hub::Client,
    snapshot: Rc<RefCell<asset_hub::snapshot::Client>>,
}

struct Runtime {
    reactor_handle: tokio_reactor::Handle,
    timer_handle: tokio_timer::timer::Handle,
    clock: tokio_timer::clock::Clock,
    executor:
        tokio_current_thread::CurrentThread<tokio_timer::timer::Timer<tokio_reactor::Reactor>>,
}

impl Runtime {
    /// Create the configured `Runtime`.
    pub fn new() -> std::io::Result<Runtime> {
        use tokio_current_thread::CurrentThread;
        use tokio_reactor::Reactor;
        use tokio_timer::{clock::Clock, timer::Timer};
        // We need a reactor to receive events about IO objects from kernel
        let reactor = Reactor::new()?;
        let reactor_handle = reactor.handle();

        let clock = Clock::new();
        // Place a timer wheel on top of the reactor. If there are no timeouts to fire, it'll let the
        // reactor pick up some new external events.
        let timer = Timer::new_with_now(reactor, clock.clone());
        let timer_handle = timer.handle();

        // And now put a single-threaded executor on top of the timer. When there are no futures ready
        // to do something, it'll let the timer or the reactor to generate some new stimuli for the
        // futures to continue in their life.
        let executor = CurrentThread::new_with_park(timer);

        Ok(Runtime {
            reactor_handle,
            timer_handle,
            clock,
            executor,
        })
    }
    pub fn executor(
        &mut self,
    ) -> &mut CurrentThread<tokio_timer::timer::Timer<tokio_reactor::Reactor>> {
        &mut self.executor
    }
    pub fn poll(&mut self) {
        let Runtime {
            ref reactor_handle,
            ref timer_handle,
            ref clock,
            ref mut executor,
            ..
        } = *self;

        let mut enter = tokio_executor::enter().expect("Multiple executors at once");
        tokio_reactor::with_default(&reactor_handle, &mut enter, |enter| {
            tokio_timer::clock::with_default(&clock, enter, |enter| {
                tokio_timer::with_default(&timer_handle, enter, |enter| {
                    let mut default_executor = tokio_current_thread::TaskExecutor::current();
                    tokio_executor::with_default(&mut default_executor, enter, |enter| {
                        let mut executor = executor.enter(enter);
                        executor.run_timeout(std::time::Duration::from_secs(0));
                    })
                });
            });
        });
    }
}

pub struct RpcLoader<HandleType> {
    assets: HashMap<AssetUuid, AssetStatus<HandleType>>,
    metadata: HashMap<AssetUuid, AssetMetadata>,
    runtime: Runtime,
    connect_string: String,
    connection: Option<RpcConnection>,
    pending_connection: Option<Receiver<Result<RpcConnection, Box<dyn Error>>>>,
    pending_metadata_request:
        Option<Receiver<CapnpResult<Response<GetAssetMetadataWithDependenciesResults>>>>,
    pending_data_request: Option<Receiver<CapnpResult<Response<GetBuildArtifactsResults>>>>,
}

pub struct RpcAssetLoad {
    id: AssetUuid,
}
impl<HandleType> Loader for RpcLoader<HandleType> {
    type LoadOp = RpcAssetLoad;
    type HandleType = HandleType;
    fn add_asset_ref(&mut self, id: AssetUuid) -> Self::LoadOp {
        self.assets
            .entry(id)
            .or_insert(AssetStatus {
                state: AssetState::None,
                refs: 0,
                asset_handle: None,
            })
            .refs += 1;
        RpcAssetLoad { id }
    }
    fn get_asset_load(&self, id: &AssetUuid) -> Option<Self::LoadOp> {
        if self.assets.contains_key(id) {
            Some(RpcAssetLoad { id: *id })
        } else {
            None
        }
    }
    fn get_asset(&self, load: &Self::LoadOp) -> Option<(AssetTypeId, Self::HandleType)> {
        None
    }
    fn decrease_asset_ref(&mut self, id: AssetUuid) {
        self.assets.entry(id).and_modify(|a| a.refs -= 1);
    }
    fn process(&mut self, asset_storage: &dyn AssetStorage<HandleType = Self::HandleType>) -> Result<(), Box<dyn Error>> {
        if self.pending_connection.is_none() && self.connection.is_none() {
            println!("connect");
            self.pending_connection = Some(self.rpc_connect());
        }
        if let Some(ref mut pending_connection) = self.pending_connection {
            println!("waiting");
            match pending_connection.try_recv() {
                Ok(Some(connection_result)) => {
                    println!("{:?}", connection_result.is_ok());
                    match connection_result {
                        Ok(conn) => self.connection = Some(conn),
                        Err(err) => error!("error connecting RpcLoader {}", err),
                    }
                    self.pending_connection = None;
                }
                Err(e) => panic!("failed to receive result for rpc connection: {:?}", e),
                _ => {}
            }
        }
        self.runtime.poll();
        for (key, mut value) in self.assets.iter_mut() {
            let new_state = match value.state {
                AssetState::None if value.refs > 0 => {
                    if self.metadata.contains_key(key) {
                        AssetState::LoadingData
                    } else {
                        AssetState::WaitingForMetadata
                    }
                }
                AssetState::None => {
                    // no refs, inactive load
                    AssetState::UnloadRequested
                }
                AssetState::WaitingForMetadata => {
                    if self.metadata.contains_key(key) {
                        AssetState::WaitingForData
                    } else {
                        AssetState::WaitingForMetadata
                    }
                }
                AssetState::RequestedMetadata => AssetState::RequestedMetadata,
                AssetState::WaitingForData => {
                    if value.asset_handle.is_some() {
                        AssetState::LoadingAsset
                    } else {
                        AssetState::WaitingForData
                    }
                },
                AssetState::LoadingData => AssetState::LoadingData,
                AssetState::LoadingAsset => AssetState::LoadingAsset,
                AssetState::Loaded => AssetState::Loaded,
                AssetState::UnloadRequested => AssetState::UnloadRequested,
                AssetState::Unloading => AssetState::Unloading,
            };
            value.state = new_state;
        }
        self.process_metadata_requests();
        self.process_data_requests(asset_storage);
        Ok(())
    }
}
impl<HandleType> RpcLoader<HandleType> {
    pub fn new() -> std::io::Result<RpcLoader<HandleType>> {
        Ok(RpcLoader {
            runtime: Runtime::new()?,
            connect_string: "".to_string(),
            assets: HashMap::new(),
            metadata: HashMap::new(),
            connection: None,
            pending_connection: None,
            pending_metadata_request: None,
            pending_data_request: None,
        })
    }

    fn update_asset_metadata(
        &mut self,
        reader: &asset_metadata::Reader,
    ) -> Result<(), capnp::Error> {
        let mut load_deps = Vec::new();
        for dep in reader.get_load_deps()? {
            load_deps.push(make_array(dep.get_id()?));
        }
        let uuid: AssetUuid = make_array(reader.get_id()?.get_id()?);
        println!("updated metadata for {:?}", uuid);
        self.metadata.insert(uuid, AssetMetadata { load_deps });
        Ok(())
    }

    fn load_data(&mut self, reader: &artifact::Reader, storage: &dyn AssetStorage<HandleType = HandleType>) -> Result<(), capnp::Error> {
        let uuid: AssetUuid = make_array(reader.get_asset_id()?.get_id()?);
        let serialized_asset = reader.get_data()?;
        let asset_type: AssetTypeId = make_array(serialized_asset.get_type_uuid()?);
        println!("loaded data of size {}", serialized_asset.get_data()?.len());
        let mut asset = self.assets.get_mut(&uuid).unwrap();
        if asset.asset_handle.is_none() {
            asset.asset_handle.replace(storage.allocate(&asset_type, &uuid));
        }
        storage.update_asset(&asset_type, &asset.asset_handle.as_ref().unwrap(), &serialized_asset.get_data()?);
        Ok(())
    }

    fn process_data_requests(&mut self, storage: &dyn AssetStorage<HandleType = HandleType>) -> Result<(), capnp::Error> {
        if let Some(ref mut request) = self.pending_data_request {
            match request.try_recv() {
                Ok(Some(request_result)) => {
                    match request_result {
                        Ok(response) => {
                            for artifact in response.get()?.get_artifacts()? {
                                self.load_data(&artifact, storage)?;
                            }
                        }
                        Err(err) => error!("error requesting build artifacts {}", err),
                    }
                    self.pending_data_request = None;
                }
                Err(e) => panic!("failed to receive result for build artifacts: {:?}", e),
                _ => {}
            }
        } else if let Some(ref conn) = self.connection {
            let assets_to_request: Vec<_> = self
                .assets
                .iter()
                .filter(|(_, v)| {
                    if let AssetState::WaitingForData = v.state {
                        true
                    } else {
                        false
                    }
                })
                .map(|(k, _)| k)
                .collect();
            let mut request = conn.snapshot.borrow().get_build_artifacts_request();
            let mut assets = request.get().init_assets(assets_to_request.len() as u32);
            for (idx, asset) in assets_to_request.iter().enumerate() {
                assets.reborrow().get(idx as u32).set_id(*asset);
            }
            let (tx, rx) = channel();
            self.runtime
                .executor()
                .spawn(request.send().promise.then(|response| {
                    let _ = tx.send(response);
                    Ok(())
                }));
            self.pending_data_request = Some(rx);
        }
        Ok(())
    }

    fn process_metadata_requests(&mut self) -> Result<(), capnp::Error> {
        if let Some(ref mut request) = self.pending_metadata_request {
            match request.try_recv() {
                Ok(Some(request_result)) => {
                    match request_result {
                        Ok(response) => {
                            for metadata in response.get()?.get_assets()? {
                                self.update_asset_metadata(&metadata)?;
                            }
                        }
                        Err(err) => error!("error requesting metadata {}", err),
                    }
                    self.pending_metadata_request = None;
                }
                Err(e) => panic!("failed to receive result for metadata request: {:?}", e),
                _ => {}
            }
        } else if let Some(ref conn) = self.connection {
            let assets_to_request: Vec<_> = self
                .assets
                .iter()
                .filter(|(_, v)| {
                    if let AssetState::WaitingForMetadata = v.state {
                        true
                    } else {
                        false
                    }
                })
                .map(|(k, _)| k)
                .collect();
            let mut request = conn
                .snapshot
                .borrow()
                .get_asset_metadata_with_dependencies_request();
            let mut assets = request.get().init_assets(assets_to_request.len() as u32);
            for (idx, asset) in assets_to_request.iter().enumerate() {
                assets.reborrow().get(idx as u32).set_id(*asset);
            }
            let (tx, rx) = channel();
            self.runtime
                .executor()
                .spawn(request.send().promise.then(|response| {
                    let _ = tx.send(response);
                    Ok(())
                }));
            self.pending_metadata_request = Some(rx);
        }
        Ok(())
    }

    fn rpc_connect(&mut self) -> Receiver<Result<RpcConnection, Box<dyn Error>>> {
        use std::net::ToSocketAddrs;
        let addr = "127.0.0.1:9999".to_socket_addrs().unwrap().next().unwrap();
        let (tx, rx) = channel();
        self.runtime.executor().spawn(
            ::tokio::net::TcpStream::connect(&addr)
                .map_err(|e| -> Box<dyn Error> { Box::new(e) })
                .and_then(move |stream| {
                    stream.set_nodelay(true)?;
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
                    let hub: asset_hub::Client =
                        rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);
                    let disconnector = rpc_system.get_disconnector();
                    tokio_current_thread::TaskExecutor::current()
                        .spawn_local(Box::new(rpc_system.map_err(|_| ())))?;
                    let request = hub.get_snapshot_request();
                    Ok(request
                        .send()
                        .promise
                        .map(move |response| (disconnector, hub, response))
                        .map_err(|e| -> Box<dyn Error> { Box::new(e) }))
                })
                .flatten()
                .and_then(|(disconnector, hub, response)| {
                    let snapshot = Rc::new(RefCell::new(response.get()?.get_snapshot()?));
                    let listener = asset_hub::listener::ToClient::new(ListenerImpl {
                        snapshot: snapshot.clone(),
                    })
                    .into_client::<::capnp_rpc::Server>();
                    let mut request = hub.register_listener_request();
                    request.get().set_listener(listener);
                    Ok(request
                        .send()
                        .promise
                        .map(|_| RpcConnection {
                            asset_hub: hub,
                            snapshot,
                        })
                        .map_err(|e| -> Box<dyn Error> { Box::new(e) }))
                })
                .flatten()
                .then(|result| {
                    let _ = tx.send(result);
                    Ok(())
                }),
        );
        rx
    }
}

impl<S> Default for RpcLoader<S> {
    fn default() -> RpcLoader<S> {
        RpcLoader::new().unwrap()
    }
}

struct ListenerImpl {
    snapshot: Rc<RefCell<asset_hub::snapshot::Client>>,
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
