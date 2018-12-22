use crate::asset_hub::AssetHub;
use crate::capnp_db::{CapnpCursor, Environment, RoTransaction};
use capnp::{self, capability::Promise};
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{Future, Stream};
use owning_ref::OwningHandle;
use schema::data::imported_metadata;
use schema::service::asset_hub;
use std::error;
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use tokio::prelude::*;
use tokio::runtime::current_thread::Runtime;

struct ServiceContext {
    hub: Arc<AssetHub>,
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

impl<'a> asset_hub::snapshot::Server for AssetHubSnapshotImpl<'a> {
    fn get_all_assets(
        &mut self,
        _params: asset_hub::snapshot::GetAllAssetsParams,
        mut results: asset_hub::snapshot::GetAllAssetsResults,
    ) -> Promise<(), capnp::Error> {
        let ctx = self.txn.as_owner();
        let txn = &**self.txn;
        let mut metadatas = Vec::new();
        for (_, value) in ctx.hub.get_metadata_iter(txn)?.capnp_iter_start() {
            let value = value?;
            let metadata = value.into_typed::<imported_metadata::Owned>();
            metadatas.push(metadata);
        }
        let mut results_builder = results.get();
        let assets = results_builder
            .reborrow()
            .init_assets(metadatas.len() as u32);
        for (idx, metadata) in metadatas.iter().enumerate() {
            let metadata = metadata.get()?;
            assets.set_with_caveats(idx as u32, metadata.get_metadata()?)?;
        }
        Promise::ok(())
    }
}

impl asset_hub::Server for AssetHubImpl {
    fn register_listener(
        &mut self,
        _params: asset_hub::RegisterListenerParams,
        _results: asset_hub::RegisterListenerResults,
    ) -> Promise<(), capnp::Error> {
        Promise::ok(())
    }
    fn get_snapshot(
        &mut self,
        _params: asset_hub::GetSnapshotParams,
        mut results: asset_hub::GetSnapshotResults,
    ) -> Promise<(), capnp::Error> {
        let ctx = self.ctx.clone();
        let snapshot = AssetHubSnapshotImpl {
            txn: Rc::new(OwningHandle::new_with_fn(ctx, |t| unsafe {
                Rc::new((*t).db.ro_txn().unwrap())
            })),
        };
        results.get().set_snapshot(
            asset_hub::snapshot::ToClient::new(snapshot).into_client::<::capnp_rpc::Server>(),
        );
        Promise::ok(())
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
        let service_impl = AssetHubImpl { ctx: ctx };
        let hub_impl = asset_hub::ToClient::new(service_impl).into_client::<::capnp_rpc::Server>();
        let mut runtime = Runtime::new().unwrap();

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
    pub fn new(db: Arc<Environment>, hub: Arc<AssetHub>) -> AssetHubService {
        AssetHubService {
            ctx: Arc::new(ServiceContext { hub, db }),
        }
    }
    pub fn run(&self, addr: std::net::SocketAddr) -> Result<(), Error> {
        use parity_tokio_ipc::Endpoint;

        let mut runtime = Runtime::new().unwrap();

        let tcp = ::tokio::net::TcpListener::bind(&addr)?;
        let tcp_future = tcp.incoming().for_each(move |stream| {
            stream.set_nodelay(true)?;
            stream.set_send_buffer_size(1 << 24)?;
            stream.set_recv_buffer_size(1 << 24)?;
            let (reader, writer) = stream.split();
            spawn_rpc(reader, writer, self.ctx.clone());
            Ok(())
        });

        let ipc = Endpoint::new(endpoint());

        let ipc_future = ipc
            .incoming(&tokio::reactor::Handle::current())
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

#[derive(Debug)]
pub enum Error {
    IO(::std::io::Error),
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::IO(ref e) => e.description(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::IO(ref e) => Some(e),
        }
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::IO(ref e) => e.fmt(f),
        }
    }
}
impl From<::std::io::Error> for Error {
    fn from(err: ::std::io::Error) -> Error {
        Error::IO(err)
    }
}
