use crate::{
    asset_hub, asset_hub_service, capnp_db::Environment, file_asset_source,
    file_tracker::FileTracker,
};
use atelier_importer::{get_importer_contexts, BoxedImporter, ImporterContext};
use log::error;
use std::{
    collections::HashMap,
    fs,
    iter::FromIterator,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
};

#[derive(Default)]
pub(crate) struct ImporterMap(HashMap<&'static str, Box<dyn BoxedImporter>>);

impl ImporterMap {
    pub fn insert(&mut self, ext: &'static str, importer: Box<dyn BoxedImporter>) {
        self.0.insert(ext, importer);
    }

    pub fn get_by_path<'a>(&'a self, path: &PathBuf) -> Option<&'a dyn BoxedImporter> {
        let lower_extension = path
            .extension()
            .map(|s| s.to_str().unwrap().to_lowercase())
            .unwrap_or_else(|| "".to_string());
        self.0.get(lower_extension.as_str()).map(|i| i.as_ref())
    }
}

pub struct AssetDaemon {
    db_dir: PathBuf,
    address: SocketAddr,
    importers: ImporterMap,
    importer_contexts: Vec<Box<dyn ImporterContext>>,
    asset_dirs: Vec<PathBuf>,
}

impl Default for AssetDaemon {
    fn default() -> Self {
        Self {
            db_dir: PathBuf::from(".assets_db"),
            address: "127.0.0.1:9999".parse().unwrap(),
            importers: Default::default(),
            importer_contexts: Vec::from_iter(get_importer_contexts()),
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

    pub fn with_importer_context(mut self, context: Box<dyn ImporterContext>) -> Self {
        self.importer_contexts.push(context);
        self
    }

    pub fn with_importer_contexts<I>(mut self, contexts: I) -> Self
    where
        I: IntoIterator<Item = Box<dyn ImporterContext>>,
    {
        self.importer_contexts.extend(contexts);
        self
    }

    pub fn with_asset_dirs(mut self, dirs: Vec<PathBuf>) -> Self {
        self.asset_dirs = dirs;
        self
    }

    pub fn run(self) {
        use asset_hub::AssetHub;
        use asset_hub_service::AssetHubService;
        use file_asset_source::FileAssetSource;
        use std::panic;

        let _ = fs::create_dir(&self.db_dir);
        for dir in self.asset_dirs.iter() {
            let _ = fs::create_dir_all(dir);
        }

        let asset_db = Environment::new(&self.db_dir).expect("failed to create asset db");
        let asset_db = Arc::new(asset_db);

        let to_watch = self.asset_dirs.iter().map(|p| p.to_str().unwrap());
        let tracker = FileTracker::new(asset_db.clone(), to_watch);
        let tracker = Arc::new(tracker);

        let hub = AssetHub::new(asset_db.clone()).expect("failed to create asset hub");
        let hub = Arc::new(hub);

        let importers = Arc::new(self.importers);
        let ctxs = Arc::new(self.importer_contexts);

        let asset_source = FileAssetSource::new(&tracker, &hub, &asset_db, &importers, ctxs)
            .expect("failed to create asset source");

        let asset_source = Arc::new(asset_source);

        let service = AssetHubService::new(
            asset_db.clone(),
            hub.clone(),
            asset_source.clone(),
            tracker.clone(),
        );

        // create the assets folder automatically to make it easier to get started.
        // might want to remove later when watched dirs become configurable?
        let tracker_handle = thread::spawn(move || {
            let result = panic::catch_unwind(|| tracker.run());
            if let Err(err) = result {
                bail(err);
            }
        });

        // NOTE(happens): We have to do a silly little dance here because of the way
        // unwind boundaries work. Since `Cell`s and other pointer types can provide
        // interior mutability, they're not allowed to cross `catch_unwind` boundaries.
        // However, Mutexes implement poisoning and can be passed, so we basically
        // wrap these in a useless Mutex that gets locked instantly. This lets us catch
        // any panic and abort after logging the error.
        let asset_source_handle = thread::spawn(move || {
            let asset_source = Mutex::new(asset_source);
            let result = panic::catch_unwind(|| asset_source.lock().unwrap().run());

            if let Err(err) = result {
                bail(err);
            }
        });

        let addr = self.address;
        let service_handle = thread::spawn(move || {
            let service = Mutex::new(service);
            let result = panic::catch_unwind(|| service.lock().unwrap().run(addr));
            if let Err(err) = result {
                bail(err);
            }
        });

        tracker_handle
            .join()
            .expect("Invalid: Thread panic should be caught");

        asset_source_handle
            .join()
            .expect("Invalid: Thread panic should be caught");

        service_handle
            .join()
            .expect("Invalid: Thread panic should be caught");
    }
}

fn bail(err: std::boxed::Box<dyn std::any::Any + std::marker::Send>) {
    error!("Something unexpected happened - bailing out to prevent corrupt state");
    if let Ok(msg) = err.downcast::<&'static str>() {
        error!("panic: {}", msg);
    }

    std::process::exit(1);
}
