use capnp::{self, capability::Promise};
use capnp_db::{DBTransaction, RoTransaction};
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use data_capnp::dirty_file_info;
use asset_hub::AssetHub;
use futures::{Future, Stream};
use owning_ref::OwningHandle;
use service_capnp::{asset_hub, asset_hub::snapshot};
use std::error;
use std::fmt;
use std::sync::Arc;
use tokio_core::reactor;
use tokio_io::AsyncRead;

pub struct FileService {
    tracker: Arc<AssetHub>,
}

struct AssetHubSnapshotImpl<'a> {
    txn: OwningHandle<Arc<AssetHub>, Box<RoTransaction<'a>>>,
}

struct AssetHubImpl {
    tracker: Arc<AssetHub>,
}

impl<'a> asset_hub::snapshot::Server for AssetHubSnapshotImpl<'a> {
    fn get_all_assets(
        &mut self,
        _params: asset_hub::snapshot::GetAllAssetsParams,
        mut results: asset_hub::snapshot::GetAllAssetsResults,
    ) -> Promise<(), capnp::Error> {
        // let all_files_vec = self
        //     .txn
        //     .as_owner()
        //     .read_all_files(&self.txn)
        //     .expect("Failed to read all files");
        // let mut results = results.get();
        // let mut files = results.init_files(all_files_vec.len() as u32);
        // for (i, v) in all_files_vec.iter().enumerate() {
        //     files
        //         .reborrow()
        //         .get(i as u32)
        //         .set_path(&v.path.to_string_lossy());
        //     files.reborrow().get(i as u32).set_state(v.state);
        // }
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
        // let arc_ref = Arc::clone(&self.tracker);
        // let snapshot = AssetHubSnapshotImpl {
        //     txn: OwningHandle::new_with_fn(arc_ref, |t| unsafe {
        //         Box::new((*t).get_txn().unwrap())
        //     }),
        // };
        // results.get().set_snapshot(
        //     asset_hub::snapshot::ToClient::new(snapshot).from_server::<::capnp_rpc::Server>(),
        // );
        Promise::ok(())
    }
}

impl FileService {
    pub fn new(tracker: Arc<AssetHub>) -> FileService {
        FileService { tracker: tracker }
    }
    pub fn run(&self) -> Result<(), Error> {
        use std::net::ToSocketAddrs;

        let mut core = reactor::Core::new()?;
        let handle = core.handle();

        let addr = "localhost:9999"
            .to_socket_addrs()?
            .next()
            .map_or(Err(Error::InvalidAddress), |a| Ok(a))?;
        let socket = ::tokio_core::net::TcpListener::bind(&addr, &handle)?;

        let service_impl = AssetHubImpl {
            tracker: self.tracker.clone(),
        };

        let publisher =
            asset_hub::ToClient::new(service_impl).from_server::<::capnp_rpc::Server>();

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
