use crate::{utils, AssetUuid};
use atelier_schema::{data::asset_change_event, service::asset_hub};
use capnp::{message::ReaderOptions, Result as CapnpResult};
use capnp_rpc::{pry, rpc_twoparty_capnp, twoparty, RpcSystem};
use crossbeam_channel::{bounded, unbounded, Receiver, Sender, TryRecvError};
use futures::Future;
use std::error::Error;
use tokio::prelude::*;
use tokio_current_thread::CurrentThread;

type Promise<T> = capnp::capability::Promise<T, capnp::Error>;

struct RpcConnection {
    asset_hub: asset_hub::Client,
    snapshot: asset_hub::snapshot::Client,
    snapshot_rx: Receiver<SnapshotChange>,
    snapshot_tx: Sender<SnapshotChange>,
}

struct Runtime {
    reactor_handle: tokio_reactor::Handle,
    timer_handle: tokio_timer::timer::Handle,
    clock: tokio_timer::clock::Clock,
    executor: CurrentThread<tokio_timer::timer::Timer<tokio_reactor::Reactor>>,
}

impl Runtime {
    /// Create the configured `Runtime`.
    pub fn new() -> std::io::Result<Runtime> {
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
                        let _ = executor.run_timeout(std::time::Duration::from_secs(0));
                    })
                });
            });
        });
    }
}
#[derive(Debug)]
pub enum ConnectionState<'a> {
    None,
    Connecting,
    Connected,
    Error(&'a Box<dyn Error>),
}

enum InternalConnectionState {
    None,
    Connecting(Receiver<Result<RpcConnection, Box<dyn Error>>>),
    Connected(RpcConnection),
    Error(Box<dyn Error>),
}

pub(crate) struct ResponsePromise<T: for<'a> capnp::traits::Owned<'a>, U> {
    rx: Receiver<
        CapnpResult<
            capnp::message::TypedReader<capnp::message::Builder<capnp::message::HeapAllocator>, T>,
        >,
    >,
    user_data: U,
}
impl<T: for<'a> capnp::traits::Owned<'a>, U> ResponsePromise<T, U> {
    pub fn get_user_data(&mut self) -> &mut U {
        &mut self.user_data
    }
}
impl<T: for<'a> capnp::traits::Owned<'a>, U> Future for ResponsePromise<T, U> {
    type Item =
        capnp::message::TypedReader<capnp::message::Builder<capnp::message::HeapAllocator>, T>;
    type Error = Box<dyn Error>;
    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        match self.rx.try_recv() {
            Ok(response_result) => match response_result {
                Ok(response) => Ok(Async::Ready(response)),
                Err(e) => Err(Box::new(e)),
            },
            Err(TryRecvError::Empty) => Ok(Async::NotReady),
            Err(e) => Err(Box::new(e)),
        }
    }
}

struct SnapshotChange {
    snapshot: asset_hub::snapshot::Client,
    changed_assets: Vec<AssetUuid>,
    deleted_assets: Vec<AssetUuid>,
}

pub struct RpcState {
    runtime: Runtime,
    connection: InternalConnectionState,
}

pub struct AssetChanges {
    pub changed: Vec<AssetUuid>,
    pub deleted: Vec<AssetUuid>,
}
// While capnp_rpc does not impl Send or Sync, in our usage of the API there can only be one thread
// accessing the internal state at any time, and this is safe since we are using a single-threaded tokio runtime.
unsafe impl Send for RpcState {}
impl RpcState {
    pub fn new() -> std::io::Result<RpcState> {
        Ok(RpcState {
            runtime: Runtime::new()?,
            connection: InternalConnectionState::None,
        })
    }

    pub fn connection_state(&self) -> ConnectionState<'_> {
        match self.connection {
            InternalConnectionState::Connected(_) => ConnectionState::Connected,
            InternalConnectionState::Connecting(_) => ConnectionState::Connecting,
            InternalConnectionState::Error(ref err) => ConnectionState::Error(err),
            InternalConnectionState::None => ConnectionState::None,
        }
    }
    pub(crate) fn request<F: 'static, Params, Results, U>(
        &mut self,
        f: F,
    ) -> ResponsePromise<Results, U>
    where
        F: FnOnce(
            &asset_hub::Client,
            &asset_hub::snapshot::Client,
        ) -> (capnp::capability::Request<Params, Results>, U),
        Params: for<'a> capnp::traits::Owned<'a>,
        Results: capnp::traits::Pipelined + for<'a> capnp::traits::Owned<'a> + 'static,
        <Results as capnp::traits::Pipelined>::Pipeline: capnp::capability::FromTypelessPipeline,
    {
        if let InternalConnectionState::Connected(ref conn) = self.connection {
            let (req, user_data) = f(&conn.asset_hub, &conn.snapshot);
            let (tx, rx) = bounded(1);
            self.runtime.executor().spawn(
                req.send()
                    .promise
                    .and_then(|response| {
                        let r = response.get()?;
                        let mut m = capnp::message::Builder::new_default();
                        m.set_root(r)?;
                        Ok(capnp::message::TypedReader::new(m.into_reader()))
                    })
                    .then(move |r| {
                        let _ = tx.send(r);
                        Ok(())
                    }),
            );
            ResponsePromise { rx, user_data }
        } else {
            panic!("Need to be connected to send requests")
        }
    }

    pub fn poll(&mut self) {
        self.connection =
            match std::mem::replace(&mut self.connection, InternalConnectionState::None) {
                // update connection state
                InternalConnectionState::Connecting(pending_connection) => {
                    match pending_connection.try_recv() {
                        Ok(connection_result) => match connection_result {
                            Ok(conn) => InternalConnectionState::Connected(conn),
                            Err(err) => InternalConnectionState::Error(err),
                        },
                        Err(TryRecvError::Empty) => {
                            InternalConnectionState::Connecting(pending_connection)
                        } // still waiting
                        Err(e) => InternalConnectionState::Error(Box::new(e)),
                    }
                }
                c => c,
            };
        self.runtime.poll()
    }

    pub fn check_asset_changes(&mut self) -> Option<AssetChanges> {
        let mut changes = None;
        self.connection =
            match std::mem::replace(&mut self.connection, InternalConnectionState::None) {
                InternalConnectionState::Connected(mut conn) => {
                    if let Ok(change) = conn.snapshot_rx.try_recv() {
                        conn.snapshot = change.snapshot;
                        changes = Some(AssetChanges {
                            changed: change.changed_assets,
                            deleted: change.deleted_assets,
                        });
                    }
                    InternalConnectionState::Connected(conn)
                }
                c => c,
            };
        changes
    }

    pub fn connect(&mut self, connect_string: &String) {
        match self.connection {
            InternalConnectionState::Connected(_) | InternalConnectionState::Connecting(_) => {
                panic!("Trying to connect while already connected or connecting")
            }
            _ => {}
        };
        use std::net::ToSocketAddrs;
        let addr = connect_string.to_socket_addrs().unwrap().next().unwrap();
        let (conn_tx, conn_rx) = bounded(1);
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
                .and_then(|(_disconnector, hub, response)| {
                    let snapshot = response.get()?.get_snapshot()?;
                    let (snapshot_tx, snapshot_rx) = unbounded();
                    let listener = asset_hub::listener::ToClient::new(ListenerImpl {
                        snapshot_channel: snapshot_tx.clone(),
                        snapshot_change: None,
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
                            snapshot_rx,
                            snapshot_tx,
                        })
                        .map_err(|e| -> Box<dyn Error> { Box::new(e) }))
                })
                .flatten()
                .then(move |result| {
                    let _ = conn_tx.send(result);
                    Ok(())
                }),
        );
        self.connection = InternalConnectionState::Connecting(conn_rx)
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
            let mut request = snapshot.get_asset_changes_request();
            request.get().set_start(change_num);
            request.get().set_count(1);
            let channel = self.snapshot_channel.clone();
            let _ = tokio_current_thread::TaskExecutor::current().spawn_local(Box::new(
                request
                    .send()
                    .promise
                    .and_then(move |response| -> Result<(), capnp::Error> {
                        let response = response.get()?;
                        println!("response ");
                        let mut changed_assets = Vec::new();
                        let mut deleted_assets = Vec::new();
                        for change in response.get_changes()? {
                            match change.get_event()?.which()? {
                                asset_change_event::ContentUpdateEvent(evt) => {
                                    let id = utils::make_array(evt?.get_id()?.get_id()?);
                                    changed_assets.push(id);
                                }
                                asset_change_event::RemoveEvent(evt) => {
                                    let id = utils::make_array(evt?.get_id()?.get_id()?);
                                    deleted_assets.push(id);
                                }
                            }
                        }
                        let _ = channel.try_send(SnapshotChange {
                            snapshot: snapshot,
                            changed_assets,
                            deleted_assets,
                        });
                        Ok(())
                    })
                    .map_err(|e| {
                        panic!("error {:?}", e);
                    }),
            ));
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
