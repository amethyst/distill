use asset_hub::AssetHub;
use capnp::{self, capability::Promise};
use capnp_db::{CapnpCursor, DBTransaction, Environment, RoTransaction};
use capnp_rpc::{pry, rpc_twoparty_capnp, twoparty, RpcSystem};
use schema::data::{asset_metadata, dirty_file_info, imported_metadata};
use schema::service::{asset_hub, asset_hub::snapshot};
use futures::{Future, Stream};
use owning_ref::OwningHandle;
use std::error;
use std::fmt;
use std::sync::Arc;
use std::rc::Rc;
use std::thread;
use tokio::runtime::current_thread::Runtime;
use tokio::prelude::*;

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
        let mut assets = results_builder
            .reborrow()
            .init_assets(metadatas.len() as u32);
        for (idx, metadata) in metadatas.iter().enumerate() {
            let metadata = metadata.get()?;
            assets.set_with_caveats(
                idx as u32,
                metadata.get_metadata()?
            );
        }
        Promise::ok(())
    }
}

impl asset_hub::Server for AssetHubImpl {
    fn register_listener(
        &mut self,
        _params: asset_hub::RegisterListenerParams,
        mut results: asset_hub::RegisterListenerResults,
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

impl AssetHubService {
    pub fn new(db: Arc<Environment>, hub: Arc<AssetHub>) -> AssetHubService {
        AssetHubService {
            ctx: Arc::new(ServiceContext { hub, db }),
        }
    }
    pub fn run(&self) -> Result<(), Error> {
        use std::net::ToSocketAddrs;

        let mut runtime = Runtime::new().unwrap();

        let addr = "localhost:9999"
            .to_socket_addrs()?
            .next()
            .map_or(Err(Error::InvalidAddress), Ok)?;
        let socket = ::tokio::net::TcpListener::bind(&addr)?;

        let done = socket.incoming().for_each(move |socket| {
            socket.set_nodelay(true)?;
            let (reader, writer) = socket.split();
            let ctx = self.ctx.clone();
            thread::spawn(move || {
                let service_impl = AssetHubImpl { ctx: ctx };
                let hub_impl =
                    asset_hub::ToClient::new(service_impl).into_client::<::capnp_rpc::Server>();
        let mut runtime = Runtime::new().unwrap();

                let network = twoparty::VatNetwork::new(
                    reader,
                    writer,
                    rpc_twoparty_capnp::Side::Server,
                    Default::default(),
                );

                let rpc_system = RpcSystem::new(Box::new(network), Some(hub_impl.clone().client));
                runtime.block_on(rpc_system.map_err(|_| ()));
            });
            Ok(())
        });

        runtime.block_on(done)?;
        Ok(())
    }
}

#[derive(Debug)]
pub enum Error {
    IO(::std::io::Error),
    InvalidAddress,
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::IO(ref e) => e.description(),
            Error::InvalidAddress => "Invalid address",
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::IO(ref e) => Some(e),
            Error::InvalidAddress => None,
        }
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::IO(ref e) => e.fmt(f),
            Error::InvalidAddress => f.write_str("Invalid address"),
        }
    }
}
impl From<::std::io::Error> for Error {
    fn from(err: ::std::io::Error) -> Error {
        Error::IO(err)
    }
}
