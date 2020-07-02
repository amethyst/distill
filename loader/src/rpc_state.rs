use atelier_core::{utils, AssetUuid};
use atelier_schema::{data::asset_change_event, service::asset_hub};
use capnp::message::ReaderOptions;
use capnp_rpc::{pry, rpc_twoparty_capnp, twoparty, RpcSystem};
use crossbeam_channel::{unbounded, Receiver, Sender};
use std::error::Error;
use tokio::{
    runtime::{Builder, Runtime},
    sync::oneshot::{self, error::TryRecvError},
};

type Promise<T> = capnp::capability::Promise<T, capnp::Error>;
pub type Response<T> =
    capnp::message::TypedReader<capnp::message::Builder<capnp::message::HeapAllocator>, T>;
pub type ResponsePromise<T> = oneshot::Receiver<Result<Response<T>, capnp::Error>>;

struct RpcConnection {
    asset_hub: asset_hub::Client,
    snapshot: asset_hub::snapshot::Client,
    snapshot_rx: Receiver<SnapshotChange>,
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
    Connecting(oneshot::Receiver<Result<RpcConnection, Box<dyn Error>>>),
    Connected(RpcConnection),
    Error(Box<dyn Error>),
}

// pub(crate) struct ResponsePromise<T: for<'a> capnp::traits::Owned<'a>, U> {
//     rx: Receiver<
//         CapnpResult<
//             capnp::message::TypedReader<capnp::message::Builder<capnp::message::HeapAllocator>, T>,
//         >,
//     >,
//     user_data: U,
// }
// impl<T: for<'a> capnp::traits::Owned<'a>, U> ResponsePromise<T, U> {
//     pub fn get_user_data(&mut self) -> &mut U {
//         &mut self.user_data
//     }
// }
// impl<T: for<'a> capnp::traits::Owned<'a>, U> Future for ResponsePromise<T, U> {
//     type Output = Result<capnp::message::TypedReader<capnp::message::Builder<capnp::message::HeapAllocator>, T>>;
//     type Error = Box<dyn Error>;
//     fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
//         match self.rx.try_recv() {
//             Ok(response_result) => match response_result {
//                 Ok(response) => Ok(Async::Ready(response)),
//                 Err(e) => Err(Box::new(e)),
//             },
//             Err(TryRecvError::Empty) => Ok(Async::NotReady),
//             Err(e) => Err(Box::new(e)),
//         }
//     }
// }

struct SnapshotChange {
    snapshot: asset_hub::snapshot::Client,
    changed_assets: Vec<AssetUuid>,
    deleted_assets: Vec<AssetUuid>,
}

pub struct RpcState {
    runtime: Runtime,
    local: tokio::task::LocalSet,
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
            runtime: Builder::new().basic_scheduler().enable_all().build()?,
            local: tokio::task::LocalSet::new(),
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
    ) -> (
        oneshot::Receiver<Result<Response<Results>, capnp::Error>>,
        U,
    )
    where
        F: FnOnce(
            &asset_hub::Client,
            &asset_hub::snapshot::Client,
        ) -> (capnp::capability::Request<Params, Results>, U),
        Params: for<'a> capnp::traits::Owned<'a> + 'static,
        Results: std::marker::Unpin
            + capnp::traits::Pipelined
            + for<'a> capnp::traits::Owned<'a>
            + 'static,
        <Results as capnp::traits::Pipelined>::Pipeline: capnp::capability::FromTypelessPipeline,
    {
        if let InternalConnectionState::Connected(ref conn) = self.connection {
            let (req, user_data) = f(&conn.asset_hub, &conn.snapshot);
            let (tx, rx) = oneshot::channel();
            self.local.spawn_local(async move {
                let send_func = async move {
                    let response = req.send().promise.await?;
                    let r = response.get()?;
                    let mut m = capnp::message::Builder::new_default();
                    m.set_root(r)?;
                    let message = capnp::message::TypedReader::<_, Results>::new(m.into_reader());
                    Ok(message)
                };
                let result = send_func.await;
                let _ = tx.send(result);
            });
            (rx, user_data)
        } else {
            panic!("Need to be connected to send requests")
        }
    }

    pub fn poll(&mut self) {
        self.connection =
            match std::mem::replace(&mut self.connection, InternalConnectionState::None) {
                // update connection state
                InternalConnectionState::Connecting(mut pending_connection) => {
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

        // tick the tokio runtime
        self.local.block_on(&mut self.runtime, async {
            tokio::task::yield_now().await;
        });
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
        let (conn_tx, conn_rx) = oneshot::channel();
        self.local.spawn_local(async move {
            let result = async move {
                let stream = ::tokio::net::TcpStream::connect(&addr)
                    .await
                    .map_err(|e| -> Box<dyn Error> { Box::new(e) })?;
                stream.set_nodelay(true)?;
                use futures::AsyncReadExt;
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
                let request = hub.get_snapshot_request();
                let response = request
                    .send()
                    .promise
                    .await
                    .map_err(|e| -> Box<dyn Error> { Box::new(e) })?;

                let snapshot = response.get()?.get_snapshot()?;
                let (snapshot_tx, snapshot_rx) = unbounded();
                let listener = asset_hub::listener::ToClient::new(ListenerImpl {
                    snapshot_channel: snapshot_tx,
                    snapshot_change: None,
                })
                .into_client::<::capnp_rpc::Server>();
                let mut request = hub.register_listener_request();
                request.get().set_listener(listener);
                let rpc_conn = request
                    .send()
                    .promise
                    .await
                    .map(|_| RpcConnection {
                        asset_hub: hub,
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
                    snapshot: snapshot,
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
