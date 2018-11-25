use asset_hub::AssetHub;
use capnp::{self, capability::Promise};
use capnp_db::{DBTransaction, Environment, RoTransaction};
use capnp_rpc::{pry, rpc_twoparty_capnp, twoparty, RpcSystem};
use data_capnp::{asset_metadata, dirty_file_info, imported_metadata};
use futures::{Future, Stream};
use owning_ref::OwningHandle;
use service_capnp::{asset_hub, asset_hub::snapshot};
use std::error;
use std::fmt;
use std::sync::Arc;
use tokio_core::reactor;
use tokio_io::AsyncRead;

struct ServiceContext {
    hub: Arc<AssetHub>,
    db: Arc<Environment>,
}

pub struct AssetHubService {
    ctx: Arc<ServiceContext>,
}

struct AssetHubSnapshotImpl<'a> {
    txn: OwningHandle<Arc<ServiceContext>, Box<RoTransaction<'a>>>,
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
        let txn = &*self.txn;
        let mut metadatas = Vec::new();
        for (_, value) in pry!(ctx.hub.get_metadata_iter(txn)) {
            let value = pry!(value);
            let metadata = value.into_typed::<imported_metadata::Owned>();
            metadatas.push(metadata);
        }
        let mut assets = results.get().init_assets(metadatas.len() as u32);
        for (idx, metadata) in metadatas.iter().enumerate() {
            let metadata = pry!(metadata.get());
            // assets.get(idx as u32) = pry!(metadata.get_metadata());
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
            txn: OwningHandle::new_with_fn(ctx, |t| unsafe { Box::new((*t).db.ro_txn().unwrap()) }),
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

        let mut core = reactor::Core::new()?;
        let handle = core.handle();

        let addr = "localhost:9999"
            .to_socket_addrs()?
            .next()
            .map_or(Err(Error::InvalidAddress), Ok)?;
        let socket = ::tokio_core::net::TcpListener::bind(&addr, &handle)?;

        let service_impl = AssetHubImpl {
            ctx: self.ctx.clone(),
        };

        let publisher = asset_hub::ToClient::new(service_impl).into_client::<::capnp_rpc::Server>();

        let handle1 = handle.clone();
        let done = socket.incoming().for_each(move |(socket, _addr)| {
            try!(socket.set_nodelay(true));
            let (reader, writer) = socket.split();
            let handle = handle1.clone();

            let network = twoparty::VatNetwork::new(
                reader,
                writer,
                rpc_twoparty_capnp::Side::Server,
                Default::default(),
            );

            let rpc_system = RpcSystem::new(Box::new(network), Some(publisher.clone().client));

            handle.spawn(rpc_system.map_err(|_| ()));
            Ok(())
        });

        core.run(done)?;
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
