use crate::{
    asset_hub, asset_hub_service, capnp_db::Environment, file_asset_source,
    file_tracker::FileTracker,
};
use atelier_importer::BoxedImporter;
use std::{
    collections::HashMap,
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    thread,
};

pub type ImporterMap = HashMap<&'static str, Box<dyn BoxedImporter>>;

pub struct AssetDaemon {
    db_dir: PathBuf,
    address: SocketAddr,
    importers: ImporterMap,
    asset_dirs: Vec<PathBuf>,
}

impl Default for AssetDaemon {
    fn default() -> Self {
        Self {
            db_dir: PathBuf::from(".amethyst"),
            address: "127.0.0.1:9999".parse().unwrap(),
            importers: Default::default(),
            asset_dirs: vec![PathBuf::from("assets")],
        }
    }
}

impl AssetDaemon {
    pub fn with_db_path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.db_dir = path.as_ref().to_owned();
        self
    }

    pub fn with_address(mut self, address: SocketAddr) -> Self {
        self.address = address;
        self
    }

    pub fn with_importer(mut self, ext: &'static str, importer: Box<dyn BoxedImporter>) -> Self {
        self.importers.insert(ext, importer);
        self
    }

    pub fn with_importers<I>(self, importers: I) -> Self
    where
        I: IntoIterator<Item = (&'static str, Box<dyn BoxedImporter>)>,
    {
        importers.into_iter().fold(self, |this, (ext, importer)| {
            this.with_importer(ext, importer)
        })
    }

    pub fn with_asset_dirs(mut self, dirs: Vec<PathBuf>) -> Self {
        self.asset_dirs = dirs;
        self
    }

    pub fn run(self) {
        let _ = fs::create_dir(&self.db_dir);
        for dir in self.asset_dirs.iter() {
            let _ = fs::create_dir_all(dir);
        }
        let asset_db = Arc::new(Environment::new(&self.db_dir).expect("failed to create asset db"));
        let tracker = Arc::new(
            FileTracker::new(
                asset_db.clone(),
                self.asset_dirs.iter().map(|p| p.to_str().unwrap()),
            )
            .expect("failed to create tracker"),
        );

        let hub = Arc::new(
            asset_hub::AssetHub::new(asset_db.clone()).expect("failed to create asset hub"),
        );
        let importers = Arc::new(self.importers);

        let asset_source = Arc::new(
            file_asset_source::FileAssetSource::new(&tracker, &hub, &asset_db, &importers)
                .expect("failed to create asset source"),
        );
        let service = asset_hub_service::AssetHubService::new(
            asset_db.clone(),
            hub.clone(),
            asset_source.clone(),
            tracker.clone(),
        );

        // create the assets folder automatically to make it easier to get started.
        // might want to remove later when watched dirs become configurable?
        let handle = thread::spawn(move || tracker.run());
        thread::spawn(move || asset_source.run().expect("FileAssetSource.run() failed"));

        service
            .run(self.address)
            .expect("Assed hub service failed to run");

        handle
            .join()
            .expect("file tracker thread panicked")
            .expect("file tracker returned error");
    }
}
