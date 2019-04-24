use capnp::{message::ReaderOptions, Result as CapnpResult};
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{
    sync::oneshot::{channel, Receiver},
    Future,
};
use schema::service::asset_hub;
use std::{cell::RefCell, error::Error, rc::Rc};
use tokio::prelude::*;
use tokio_current_thread::CurrentThread;

type Promise<T> = capnp::capability::Promise<T, capnp::Error>;

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
            Ok(Some(response_result)) => match response_result {
                Ok(response) => Ok(Async::Ready(response)),
                Err(e) => Err(Box::new(e)),
            },
            Ok(None) => Ok(Async::NotReady),
            Err(e) => Err(Box::new(e)),
        }
    }
}

pub struct RpcState {
    runtime: Runtime,
    connection: InternalConnectionState,
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
            let (req, user_data) = f(&conn.asset_hub, &*conn.snapshot.borrow());
            let (tx, rx) = channel();
            self.runtime.executor().spawn(
                req.send()
                    .promise
                    .and_then(|response| {
                        let r = response.get()?;
                        let mut m = capnp::message::Builder::new_default();
                        m.set_root(r)?;
                        Ok(capnp::message::TypedReader::new(m.into_reader()))
                    })
                    .then(|r| {
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
                InternalConnectionState::Connecting(mut pending_connection) => {
                    match pending_connection.try_recv() {
                        Ok(Some(connection_result)) => match connection_result {
                            Ok(conn) => InternalConnectionState::Connected(conn),
                            Err(err) => InternalConnectionState::Error(err),
                        },
                        Err(e) => InternalConnectionState::Error(Box::new(e)),
                        Ok(None) => InternalConnectionState::Connecting(pending_connection), // still waiting
                    }
                }
                c => c,
            };
        self.runtime.poll()
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
        self.connection = InternalConnectionState::Connecting(rx)
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
