use crate::asset_hub::{self, AssetHub};
use crate::capnp_db::{CapnpCursor, DBTransaction, Environment, MessageReader, RwTransaction};
use crate::daemon::ImporterMap;
use crate::error::Result;
use crate::file_tracker::{FileState, FileTracker, FileTrackerEvent};
use crate::serialized_asset::SerializedAsset;
use crate::source_pair_import::{
    self, hash_file, HashedSourcePair, SourceMetadata, SourcePair, SourcePairImport,
};
use crate::utils;
use atelier_importer::{AssetMetadata, AssetUuid, BoxedImporter};
use atelier_schema::data::{self, source_metadata};
use bincode;
use crossbeam_channel::{self as channel, Receiver};
use log::{debug, error, info};
use rayon::prelude::*;
use scoped_threadpool::Pool;
use std::collections::HashMap;
use std::{iter::FromIterator, path::PathBuf, str, sync::Arc};
use time::PreciseTime;

pub(crate) struct FileAssetSource {
    hub: Arc<AssetHub>,
    tracker: Arc<FileTracker>,
    rx: Receiver<FileTrackerEvent>,
    db: Arc<Environment>,
    tables: FileAssetSourceTables,
    importers: Arc<ImporterMap>,
}

struct FileAssetSourceTables {
    /// Maps the source file path to its SourceMetadata
    /// Path -> SourceMetadata
    path_to_metadata: lmdb::Database,
    /// Maps an AssetUuid to its source file path
    /// AssetUuid -> Path
    asset_id_to_path: lmdb::Database,
}

type SerializedAssetVec = SerializedAsset<Vec<u8>>;

fn hash_files<'a, T, I>(pairs: I) -> Vec<Result<HashedSourcePair>>
where
    I: IntoParallelIterator<Item = &'a SourcePair, Iter = T>,
    T: ParallelIterator<Item = &'a SourcePair>,
{
    Vec::from_par_iter(pairs.into_par_iter().map(|s| {
        let mut hashed_pair = HashedSourcePair {
            meta: s.meta.clone(),
            source: s.source.clone(),
            source_hash: None,
            meta_hash: None,
        };
        match s.meta {
            Some(ref state) if state.state == data::FileState::Exists => {
                let (state, hash) = hash_file(state)?;
                hashed_pair.meta = Some(state);
                hashed_pair.meta_hash = hash;
            }
            _ => {}
        };
        match s.source {
            Some(ref state) if state.state == data::FileState::Exists => {
                let (state, hash) = hash_file(state)?;
                hashed_pair.source = Some(state);
                hashed_pair.source_hash = hash;
            }
            _ => {}
        };
        Ok(hashed_pair)
    }))
}

impl FileAssetSource {
    pub fn new(
        tracker: &Arc<FileTracker>,
        hub: &Arc<AssetHub>,
        db: &Arc<Environment>,
        importers: &Arc<ImporterMap>,
    ) -> Result<FileAssetSource> {
        let (tx, rx) = channel::unbounded();
        tracker.register_listener(tx);
        Ok(FileAssetSource {
            tracker: tracker.clone(),
            hub: hub.clone(),
            db: db.clone(),
            rx,
            tables: FileAssetSourceTables {
                path_to_metadata: db
                    .create_db(Some("path_to_metadata"), lmdb::DatabaseFlags::default())?,
                asset_id_to_path: db
                    .create_db(Some("asset_id_to_path"), lmdb::DatabaseFlags::default())?,
            },
            importers: importers.clone(),
        })
    }

    fn put_metadata<'a>(
        &self,
        txn: &'a mut RwTransaction<'_>,
        path: &PathBuf,
        metadata: &SourceMetadata,
    ) -> Result<()> {
        let mut value_builder = capnp::message::Builder::new_default();
        {
            let mut value = value_builder.init_root::<source_metadata::Builder<'_>>();
            {
                value.set_importer_version(metadata.importer_version);
                value.set_importer_type(&metadata.importer_type);
                value.set_importer_state_type(&metadata.importer_state.uuid());
                let mut state_buf = Vec::new();
                bincode::serialize_into(&mut state_buf, &metadata.importer_state)?;
                value.set_importer_state(&state_buf);
                value.set_importer_options_type(&metadata.importer_options.uuid());
                let mut options_buf = Vec::new();
                bincode::serialize_into(&mut options_buf, &metadata.importer_options)?;
                value.set_importer_options(&options_buf);
            }
            let mut assets = value.reborrow().init_assets(metadata.assets.len() as u32);
            for (idx, asset) in metadata.assets.iter().enumerate() {
                assets.reborrow().get(idx as u32).set_id(&asset.id);
            }
            let assets_with_pipelines: Vec<&AssetMetadata> = metadata
                .assets
                .iter()
                .filter(|a| a.build_pipeline.is_some())
                .collect();
            let mut build_pipelines = value
                .reborrow()
                .init_build_pipelines(assets_with_pipelines.len() as u32);
            for (idx, asset) in assets_with_pipelines.iter().enumerate() {
                build_pipelines
                    .reborrow()
                    .get(idx as u32)
                    .init_key()
                    .set_id(&asset.id);
                build_pipelines
                    .reborrow()
                    .get(idx as u32)
                    .init_value()
                    .set_id(&asset.build_pipeline.unwrap());
            }
        }
        let key_str = path.to_string_lossy();
        let key = key_str.as_bytes();
        txn.put(self.tables.path_to_metadata, &key, &value_builder)?;
        Ok(())
    }

    pub fn get_metadata<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
        path: &PathBuf,
    ) -> Result<Option<MessageReader<'a, source_metadata::Owned>>> {
        let key_str = path.to_string_lossy();
        let key = key_str.as_bytes();
        Ok(txn.get::<source_metadata::Owned, &[u8]>(self.tables.path_to_metadata, &key)?)
    }

    pub fn iter_metadata<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
    ) -> Result<impl Iterator<Item = Result<(PathBuf, MessageReader<'a, source_metadata::Owned>)>>>
    {
        Ok(txn
            .open_ro_cursor(self.tables.path_to_metadata)?
            .capnp_iter_start()
            .map(|(key, value)| {
                let evt = value?.into_typed::<source_metadata::Owned>();
                let path = PathBuf::from(str::from_utf8(key).expect("failed to parse key as utf8"));
                Ok((path, evt))
            }))
    }

    fn delete_metadata(&self, txn: &mut RwTransaction<'_>, path: &PathBuf) -> Result<bool> {
        let key_str = path.to_string_lossy();
        let key = key_str.as_bytes();
        Ok(txn.delete(self.tables.path_to_metadata, &key)?)
    }

    fn put_asset_path<'a>(
        &self,
        txn: &'a mut RwTransaction<'_>,
        asset_id: &AssetUuid,
        path: &PathBuf,
    ) -> Result<()> {
        let path_str = path.to_string_lossy();
        let path = path_str.as_bytes();
        txn.put_bytes(self.tables.asset_id_to_path, asset_id, &path)?;
        Ok(())
    }

    pub fn get_asset_path<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
        asset_id: &AssetUuid,
    ) -> Result<Option<PathBuf>> {
        match txn.get_as_bytes(self.tables.asset_id_to_path, asset_id)? {
            Some(p) => Ok(Some(PathBuf::from(
                str::from_utf8(p).expect("Encoded key was invalid utf8"),
            ))),
            None => Ok(None),
        }
    }

    fn delete_asset_path(&self, txn: &mut RwTransaction<'_>, asset_id: &AssetUuid) -> Result<bool> {
        Ok(txn.delete(self.tables.asset_id_to_path, asset_id)?)
    }

    pub fn regenerate_import_artifact<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
        id: &AssetUuid,
        scratch_buf: &mut Vec<u8>,
    ) -> Result<Option<(u64, SerializedAssetVec)>> {
        let path = self.get_asset_path(txn, id)?;
        if let Some(path) = path {
            let cache = DBSourceMetadataCache {
                txn,
                file_asset_source: self,
                _marker: std::marker::PhantomData,
            };
            let mut import = SourcePairImport::new(path);
            import.with_importer_from_map(&self.importers)?;
            import.generate_source_metadata(&cache)?;
            import.hash_source()?;
            let imported_assets = import.import_source(scratch_buf)?;
            Ok(imported_assets
                .into_iter()
                .find(|a| a.metadata.id == *id)
                .map(|a| {
                    a.asset
                        .map(|a| (utils::calc_asset_hash(id, import.import_hash().unwrap()), a))
                })
                .unwrap_or(None))
        } else {
            Ok(None)
        }
    }

    fn process_metadata_changes(
        &self,
        txn: &mut RwTransaction<'_>,
        changes: HashMap<PathBuf, Option<SourceMetadata>>,
        change_batch: &mut asset_hub::ChangeBatch,
    ) -> Result<()> {
        let mut affected_assets = HashMap::new();
        for (path, _) in changes.iter().filter(|(_, change)| change.is_none()) {
            debug!("deleting metadata for {}", path.to_string_lossy());
            let mut to_remove = Vec::new();
            {
                let existing_metadata = self.get_metadata(txn, path)?;
                if let Some(ref existing_metadata) = existing_metadata {
                    for asset in existing_metadata.get()?.get_assets()? {
                        to_remove.push(utils::uuid_from_slice(asset.get_id()?)?);
                    }
                }
            }
            for asset in to_remove {
                debug!("remove asset {:?}", asset);
                affected_assets.entry(asset).or_insert(None);
                self.delete_asset_path(txn, &asset)?;
            }
            self.delete_metadata(txn, path)?;
        }
        for (path, metadata) in changes.iter().filter(|(_, change)| change.is_some()) {
            let metadata = metadata.as_ref().unwrap();
            debug!("imported {}", path.to_string_lossy());
            let mut to_remove = Vec::new();
            if let Some(existing_metadata) = self.get_metadata(txn, path)? {
                for asset in existing_metadata.get()?.get_assets()? {
                    let id = utils::uuid_from_slice(asset.get_id()?)?;
                    if metadata.assets.iter().all(|a| a.id != id) {
                        to_remove.push(id);
                    }
                }
            }
            for asset in to_remove {
                debug!("removing deleted asset {:?}", asset);
                self.delete_asset_path(txn, &asset)?;
                affected_assets.entry(asset).or_insert(None);
            }
            self.put_metadata(txn, path, &metadata)?;
            for asset in metadata.assets.iter() {
                debug!("updating asset {:?}", uuid::Uuid::from_bytes(asset.id));
                match self.get_asset_path(txn, &asset.id)? {
                    Some(ref old_path) if old_path != path => {
                        error!(
                            "asset {:?} already in DB with path {} expected {}",
                            asset.id,
                            old_path.to_string_lossy(),
                            path.to_string_lossy(),
                        );
                    }
                    Some(_) => {} // asset already in DB with correct path
                    _ => self.put_asset_path(txn, &asset.id, path)?,
                }
                affected_assets.insert(asset.id, Some(asset));
            }
        }
        for (asset, maybe_metadata) in affected_assets {
            match self.get_asset_path(txn, &asset)? {
                Some(ref path) => {
                    let asset_metadata =
                        maybe_metadata.expect("metadata exists in DB but not in hashmap");
                    self.hub.update_asset(
                        txn,
                        utils::calc_asset_hash(
                            &asset,
                            changes
                                .get(path)
                                .expect("path in affected set but no change in hashmap")
                                .as_ref()
                                .unwrap()
                                .import_hash
                                .expect("path changed but no import hash present"),
                        ),
                        &asset_metadata,
                        data::AssetSource::File,
                        change_batch,
                    )?;
                }
                None => {
                    self.hub.remove_asset(txn, &asset, change_batch)?;
                }
            }
        }
        Ok(())
    }

    fn ack_dirty_file_states(
        &self,
        txn: &mut RwTransaction<'_>,
        pair: &HashedSourcePair,
    ) -> Result<()> {
        let mut skip_ack_dirty = false;
        {
            let check_file_state = |s: &Option<&FileState>| -> Result<bool> {
                match s {
                    Some(source) => {
                        let source_file_state = self.tracker.get_file_state(txn, &source.path);
                        Ok(source_file_state.map_or(false, |s| s != **source))
                    }
                    None => Ok(false),
                }
            };
            skip_ack_dirty |= check_file_state(&pair.source.as_ref().map(|f| f))?;
            skip_ack_dirty |= check_file_state(&pair.meta.as_ref().map(|f| f))?;
        }
        if !skip_ack_dirty {
            if pair.source.is_some() {
                self.tracker
                    .delete_dirty_file_state(txn, pair.source.as_ref().map(|p| &p.path).unwrap())?;
            }
            if pair.meta.is_some() {
                self.tracker
                    .delete_dirty_file_state(txn, pair.meta.as_ref().map(|p| &p.path).unwrap())?;
            }
        }
        Ok(())
    }

    fn handle_rename_events(&self, txn: &mut RwTransaction<'_>) -> Result<()> {
        let rename_events = self.tracker.read_rename_events(txn)?;
        debug!("rename events");
        for (_, evt) in rename_events.iter() {
            let dst_str = evt.dst.to_string_lossy();
            let dst = dst_str.as_bytes();
            let mut asset_ids = Vec::new();
            let mut existing_metadata = None;
            {
                let metadata = self.get_metadata(txn, &evt.src)?;
                if let Some(metadata) = metadata {
                    let metadata = metadata.get()?;
                    let mut copy = capnp::message::Builder::new_default();
                    copy.set_root(metadata)?;
                    existing_metadata = Some(copy);
                    for asset in metadata.get_assets()? {
                        asset_ids.push(Vec::from(asset.get_id()?));
                    }
                }
            }
            for asset in asset_ids {
                txn.delete(self.tables.asset_id_to_path, &asset)?;
                txn.put_bytes(self.tables.asset_id_to_path, &asset, &dst)?;
            }
            if let Some(existing_metadata) = existing_metadata {
                self.delete_metadata(txn, &evt.src)?;
                txn.put(self.tables.path_to_metadata, &dst, &existing_metadata)?;
            }
        }
        if !rename_events.is_empty() {
            self.tracker.clear_rename_events(txn)?;
        }
        Ok(())
    }

    fn check_for_importer_changes(&self) -> bool {
        let mut changed_paths = Vec::new();

        let result = self.db.ro_txn().and_then(|txn| {
            // TODO(happens): Simplify this loop
            for result in self.iter_metadata(&txn)? {
                let (path, metadata) = result?;
                let metadata = metadata.get()?;
                let changed = if let Some(importer) = self.importers.get_by_path(&path) {
                    metadata.get_importer_version() != importer.version()
                        || metadata.get_importer_options_type()?
                            != importer.default_options().uuid()
                        || metadata.get_importer_state_type()? != importer.default_state().uuid()
                        || metadata.get_importer_type()? != importer.uuid()
                } else {
                    false
                };
                if changed {
                    changed_paths.push(path);
                }
            }

            Ok(())
        });

        if let Err(err) = result {
            error!("Error during txn: {}", err);
            return false;
        }

        if !changed_paths.is_empty() {
            let result = self.db.rw_txn().and_then(|mut txn| {
                for path in changed_paths {
                    self.tracker.add_dirty_file(&mut txn, &path)?;
                }

                txn.commit()
            });

            if let Err(err) = result {
                error!("Error during txn: {}", err);
                return false;
            }

            true
        } else {
            false
        }
    }

    fn handle_dirty_files(&self, txn: &mut RwTransaction<'_>) -> HashMap<PathBuf, SourcePair> {
        // TODO(happens): Why is this suddenly throwing an error?
        let dirty_files = self.tracker.read_dirty_files(txn);
        let mut source_meta_pairs: HashMap<PathBuf, SourcePair> = HashMap::new();

        if !dirty_files.is_empty() {
            for state in dirty_files.into_iter() {
                let mut is_meta = false;
                if let Some(ext) = state.path.extension() {
                    if let Some("meta") = ext.to_str() {
                        is_meta = true;
                    }
                }
                let base_path = if is_meta {
                    state.path.with_file_name(state.path.file_stem().unwrap())
                } else {
                    state.path.clone()
                };
                let mut pair = source_meta_pairs.entry(base_path).or_insert(SourcePair {
                    source: Option::None,
                    meta: Option::None,
                });
                if is_meta {
                    pair.meta = Some(state.clone());
                } else {
                    pair.source = Some(state.clone());
                }
            }

            for (path, pair) in source_meta_pairs.iter_mut() {
                if pair.meta.is_none() {
                    let path = utils::to_meta_path(&path);
                    pair.meta = self.tracker.get_file_state(txn, &path);
                }

                if pair.source.is_none() {
                    pair.source = self.tracker.get_file_state(txn, &path);
                }
            }

            debug!("Processing {} changed file pairs", source_meta_pairs.len());
        }

        source_meta_pairs
            .into_iter()
            .filter(|(_, pair)| pair.meta.is_some() && pair.source.is_some())
            .collect()
    }

    // TODO(happens): Return for this is asset_metadata_changed. This function needs a lot
    // of work, and in the process it will hopefully clear up and get a name that will
    // make the return value more obvious.
    fn threadpool_mess(&self) -> bool {
        use std::cell::RefCell;
        thread_local!(static SCRATCH_STORE: RefCell<Option<Vec<u8>>> = RefCell::new(None));

        let mut asset_metadata_changed = false;

        // Should get rid of this scoped_threadpool madness somehow,
        // but can't use par_iter directly since I need to process the results
        // as soon as they are completed. So essentially I want futures::stream::FuturesUnordered.
        // But I couldn't figure all that future stuff out, so here we are. Scoped threadpool.
        // thread_pool.scoped(|scope| -> Result<()> {
        //     let (tx, rx) = channel::unbounded();
        //     let to_process = hashed_files.len();
        //     let mut import_iter = hashed_files.iter().map(|p| {
        //         let processed_pair = p.clone();
        //         let sender = tx.clone();
        //         scope.execute(move || {
        //             SCRATCH_STORE.with(|cell| {
        //                 let mut local_store = cell.borrow_mut();
        //                 if local_store.is_none() {
        //                     *local_store = Some(Vec::new());
        //                 }
        //                 match self.db.ro_txn() {
        //                     Err(e) => {
        //                         sender.send((processed_pair, Err(e))).unwrap();
        //                     }
        //                     Ok(read_txn) => {
        //                         let cache = DBSourceMetadataCache {
        //                             txn: &read_txn,
        //                             file_asset_source: &self,
        //                             _marker: std::marker::PhantomData,
        //                         };
        //                         let result = source_pair_import::process_pair(
        //                             &cache,
        //                             &self.importers,
        //                             &processed_pair,
        //                             local_store.as_mut().unwrap(),
        //                         );
        //                         sender.send((processed_pair, result)).unwrap();
        //                     }
        //                 }
        //             });
        //         });
        //     });

        //     let num_queued_imports = num_cpus::get() * 2;
        //     for _ in 0..num_queued_imports {
        //         import_iter.next();
        //     }

        //     let mut num_processed = 0;
        //     let mut metadata_changes = HashMap::new();
        //     while num_processed < to_process {
        //         match rx.recv() {
        //             Ok((pair, maybe_result)) => {
        //                 match maybe_result {
        //                     // Successful import
        //                     Ok(result) => {
        //                         let path = &pair
        //                             .source
        //                             .as_ref()
        //                             .or_else(|| pair.meta.as_ref())
        //                             .expect(
        //                                 "a successful import must have a source or meta FileState",
        //                             )
        //                             .path;
        //                         self.ack_dirty_file_states(&mut txn, &pair)?;
        //                         // TODO put import artifact in cache
        //                         metadata_changes.insert(
        //                             path.clone(),
        //                             result.map(|r| r.0.source_metadata()).unwrap_or(None),
        //                         );
        //                     }
        //                     Err(e) => error!("Error processing pair: {}", e),
        //                 }
        //                 num_processed += 1;
        //                 import_iter.next();
        //             }
        //             _ => {
        //                 break;
        //             }
        //         }
        //     }
        //     let mut change_batch = asset_hub::ChangeBatch::new();
        //     self.process_metadata_changes(&mut txn, metadata_changes, &mut change_batch)?;
        //     asset_metadata_changed = self.hub.add_changes(&mut txn, change_batch)?;
        //     Ok(())
        // })?;

        asset_metadata_changed
    }

    fn handle_update(&self, thread_pool: &mut Pool) {
        let start_time = PreciseTime::now();
        let mut changed_files = Vec::new();

        let result = self.db.rw_txn().and_then(|mut txn| {
            // Before reading the filesystem state we need to process rename events.
            // This must be done in the same transaction to guarantee database consistency.
            self.handle_rename_events(&mut txn);

            let source_meta_pairs = self.handle_dirty_files(&mut txn);
            // This looks a little stupid, since there is no `into_values`
            changed_files.extend(source_meta_pairs.into_iter().map(|(_, v)| v));

            if txn.dirty {
                txn.commit()?;
            }

            Ok(())
        });

        if let Err(err) = result {
            error!("Error during txn: {}", err);
            return;
        }

        let hashed_files = hash_files(&changed_files);
        debug!("Hashed {}", hashed_files.len());

        let hashed_files: Vec<HashedSourcePair> = hashed_files
            .into_iter()
            .filter_map(|f| match f {
                Ok(hashed_file) => Some(hashed_file),
                Err(err) => {
                    error!("Hashing error: {}", err);
                    None
                }
            })
            .collect();

        let elapsed = start_time.to(PreciseTime::now());
        debug!("Hashed {} pairs in {}", hashed_files.len(), elapsed);

        let result = self.db.rw_txn().and_then(|mut txn| {
            let asset_metadata_changed = self.threadpool_mess();

            if txn.dirty {
                txn.commit()?;

                if asset_metadata_changed {
                    self.hub.notify_listeners();
                }
            }

            Ok(())
        });

        if let Err(err) = result {
            error!("Error during txn: {}", err);
            return;
        }

        let elapsed = start_time.to(PreciseTime::now());
        info!("Processed {} pairs in {}", hashed_files.len(), elapsed);
    }

    pub fn run(&self) {
        let mut thread_pool = Pool::new(num_cpus::get() as u32);
        let mut started = false;
        let mut update = false;

        while let Ok(evt) = self.rx.recv() {
            match evt {
                FileTrackerEvent::Start => {
                    started = true;
                    if self.check_for_importer_changes() || update {
                        self.handle_update(&mut thread_pool);
                    }
                }
                FileTrackerEvent::Update => {
                    update = true;
                    if started {
                        self.handle_update(&mut thread_pool);
                    }
                }
            }
        }
    }
}

struct DBSourceMetadataCache<'a, 'b, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a> {
    txn: &'a V,
    file_asset_source: &'b FileAssetSource,
    _marker: std::marker::PhantomData<T>,
}

impl<'a, 'b, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>
    source_pair_import::SourceMetadataCache for DBSourceMetadataCache<'a, 'b, V, T>
{
    fn restore_metadata(
        &self,
        path: &PathBuf,
        importer: &dyn BoxedImporter,
        metadata: &mut SourceMetadata,
    ) -> Result<()> {
        let saved_metadata = self.file_asset_source.get_metadata(self.txn, path)?;
        if let Some(saved_metadata) = saved_metadata {
            let saved_metadata = saved_metadata.get()?;
            let mut build_pipelines = HashMap::new();
            for pair in saved_metadata.get_build_pipelines()?.iter() {
                build_pipelines.insert(
                    utils::uuid_from_slice(&pair.get_key()?.get_id()?)?,
                    utils::uuid_from_slice(&pair.get_value()?.get_id()?)?,
                );
            }
            if saved_metadata.get_importer_options_type()? == metadata.importer_options.uuid() {
                if let Ok(options) =
                    importer.deserialize_options(saved_metadata.get_importer_options()?)
                {
                    metadata.importer_options = options;
                }
            }
            if saved_metadata.get_importer_state_type()? == metadata.importer_state.uuid() {
                if let Ok(state) = importer.deserialize_state(saved_metadata.get_importer_state()?)
                {
                    metadata.importer_state = state;
                }
            }
            metadata.assets = build_pipelines
                .iter()
                .map(|(id, pipeline)| AssetMetadata {
                    id: *id,
                    build_pipeline: Some(*pipeline),
                    ..Default::default()
                })
                .collect();
        }
        Ok(())
    }
}
