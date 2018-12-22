use crate::asset_hub::AssetHub;
use crate::asset_import::{
    format_from_ext, AssetMetadata, BoxedImporter, SerdeObj, SourceMetadata, SOURCEMETADATA_VERSION,
};
use crate::capnp_db::{DBTransaction, Environment, MessageReader, RoTransaction, RwTransaction};
use crate::error::{Error, Result};
use crate::file_tracker::{FileState, FileTracker, FileTrackerEvent};
use crate::utils;
use crate::watcher::file_metadata;
use bincode;
use crossbeam_channel::{self as channel, Receiver};
use log::{debug, error, info};
use rayon::prelude::*;
use ron;
use schema::data::{self, source_metadata};
use scoped_threadpool::Pool;
use std::collections::HashMap;
use std::{
    ffi::OsStr,
    fs,
    hash::{Hash, Hasher},
    io::BufRead,
    io::{Read, Write},
    iter::FromIterator,
    path::PathBuf,
    str,
    sync::Arc,
};
use time::PreciseTime;
use uuid::Uuid;

pub struct FileAssetSource {
    hub: Arc<AssetHub>,
    tracker: Arc<FileTracker>,
    rx: Receiver<FileTrackerEvent>,
    db: Arc<Environment>,
    tables: FileAssetSourceTables,
}

struct FileAssetSourceTables {
    /// Maps the source file path to its SourceMetadata
    /// Path -> SourceMetadata
    path_to_metadata: lmdb::Database,
    /// Maps an AssetUUID to its source file path
    /// AssetUUID -> Path
    asset_id_to_path: lmdb::Database,
}

// Only files get Some(hash)
#[derive(Clone, Debug)]
struct HashedAssetFilePair {
    source: Option<FileState>,
    source_hash: Option<u64>,
    meta: Option<FileState>,
    meta_hash: Option<u64>,
}
#[derive(Clone, Debug)]
struct AssetFilePair {
    source: Option<FileState>,
    meta: Option<FileState>,
}

struct ImportedAsset {
    import_hash: u64,
    metadata: AssetMetadata,
    asset: Option<Vec<u8>>,
}

struct ImportResult {
    metadata: SourceMetadata<Box<SerdeObj>, Box<SerdeObj>>,
    assets: Vec<ImportedAsset>,
}

#[derive(Debug)]
struct SavedImportMetadata<'a> {
    importer_version: u32,
    options_type: u128,
    options: &'a [u8],
    state_type: u128,
    state: &'a [u8],
}

fn to_meta_path(p: &PathBuf) -> PathBuf {
    p.with_file_name(OsStr::new(
        &(p.file_name().unwrap().to_str().unwrap().to_owned() + ".meta"),
    ))
}

fn hash_file(state: &FileState) -> Result<(FileState, Option<u64>)> {
    let metadata = match fs::metadata(&state.path) {
        Err(e) => return Err(Error::IO(e)),
        Ok(m) => {
            if !m.is_file() {
                return Ok((state.clone(), None));
            }
            file_metadata(&m)
        }
    };
    Ok(fs::OpenOptions::new()
        .read(true)
        .open(&state.path)
        .and_then(|f| {
            let mut hasher = ::std::collections::hash_map::DefaultHasher::new();
            let mut reader = ::std::io::BufReader::with_capacity(64000, f);
            loop {
                let length = {
                    let buffer = reader.fill_buf()?;
                    hasher.write(buffer);
                    buffer.len()
                };
                if length == 0 {
                    break;
                }
                reader.consume(length);
            }
            Ok((
                FileState {
                    path: state.path.clone(),
                    state: data::FileState::Exists,
                    last_modified: metadata.last_modified,
                    length: metadata.length,
                },
                Some(hasher.finish()),
            ))
        })
        .map_err(Error::IO)?)
}

fn hash_files<'a, T, I>(pairs: I) -> Vec<Result<HashedAssetFilePair>>
where
    I: IntoParallelIterator<Item = &'a AssetFilePair, Iter = T>,
    T: ParallelIterator<Item = &'a AssetFilePair>,
{
    Vec::from_par_iter(pairs.into_par_iter().map(|s| {
        let mut hashed_pair = HashedAssetFilePair {
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

fn calc_import_hash(
    scratch_buf: &mut Vec<u8>,
    importer_options: &dyn SerdeObj,
    importer_state: &dyn SerdeObj,
    source_hash: u64,
) -> Result<u64> {
    let mut hasher = ::std::collections::hash_map::DefaultHasher::new();
    scratch_buf.clear();
    bincode::serialize_into(&mut *scratch_buf, importer_options)?;
    scratch_buf.hash(&mut hasher);
    scratch_buf.clear();
    bincode::serialize_into(&mut *scratch_buf, importer_state)?;
    scratch_buf.hash(&mut hasher);
    source_hash.hash(&mut hasher);
    Ok(hasher.finish())
}

fn import_source(
    path: &PathBuf,
    source_hash: u64,
    importer: &dyn BoxedImporter,
    options: Box<dyn SerdeObj>,
    state: Box<dyn SerdeObj>,
    scratch_buf: &mut Vec<u8>,
) -> Result<ImportResult> {
    let mut f = fs::File::open(&path)?;
    let imported = importer.import_boxed(&mut f, options, state)?;
    let options = imported.options;
    let state = imported.state;
    let imported = imported.value;
    let mut imported_assets = Vec::new();
    let import_hash = calc_import_hash(scratch_buf, &*options, &*state, source_hash)?;
    for mut asset in imported.assets {
        asset.search_tags.push((
            "file_name".to_string(),
            Some(
                path.file_name()
                    .expect("failed to get file stem")
                    .to_string_lossy()
                    .to_string(),
            ),
        ));
        let asset_data = &asset.asset_data;
        let mut asset_buf = Vec::new();
        bincode::serialize_into(&mut asset_buf, asset_data)?;
        imported_assets.push({
            ImportedAsset {
                import_hash: import_hash,
                metadata: AssetMetadata {
                    id: asset.id,
                    search_tags: asset.search_tags,
                    build_deps: asset.build_deps,
                    load_deps: asset.load_deps,
                    instantiate_deps: asset.instantiate_deps,
                },
                asset: Some(asset_buf),
            }
        });
        debug!(
            "Import success {} read {} bytes",
            path.to_string_lossy(),
            scratch_buf.len(),
        );
    }
    let source_metadata = SourceMetadata {
        version: SOURCEMETADATA_VERSION,
        import_hash,
        importer_version: importer.version(),
        importer_options: options,
        importer_state: state,
        assets: Vec::from_iter(imported_assets.iter().map(|m| m.metadata.clone())),
    };
    let serialized_metadata =
        ron::ser::to_string_pretty(&source_metadata, ron::ser::PrettyConfig::default()).unwrap();
    let meta_path = to_meta_path(&path);
    let mut meta_file = fs::File::create(meta_path)?;
    meta_file.write_all(serialized_metadata.as_bytes())?;
    Ok(ImportResult {
        metadata: source_metadata,
        assets: imported_assets,
    })
}

fn import_pair(
    source: &FileState,
    source_hash: u64,
    meta: Option<&FileState>,
    _meta_hash: Option<u64>,
    scratch_buf: &mut Vec<u8>,
    saved_metadata: Option<SavedImportMetadata>,
) -> Result<Option<ImportResult>> {
    let format = format_from_ext(source.path.extension().unwrap().to_str().unwrap());
    match format {
        Some(format) => {
            let mut options;
            let mut state;
            if let Some(_meta) = meta {
                let meta_path = to_meta_path(&source.path);
                let mut f = fs::File::open(&meta_path)?;
                let mut bytes = Vec::new();
                f.read_to_end(&mut bytes)?;
                let metadata = format.deserialize_metadata(&bytes)?;
                let import_hash = calc_import_hash(
                    scratch_buf,
                    &*metadata.importer_options,
                    &*metadata.importer_state,
                    source_hash,
                )?;
                if metadata.import_hash == import_hash {
                    let imported_assets: Result<Vec<ImportedAsset>> = metadata
                        .assets
                        .iter()
                        .map(|m| {
                            Ok(ImportedAsset {
                                import_hash,
                                metadata: m.clone(),
                                asset: None,
                            })
                        })
                        .collect();
                    return Ok(Some(ImportResult {
                        assets: imported_assets?,
                        metadata,
                    }));
                }
                options = metadata.importer_options;
                state = metadata.importer_state;
            } else {
                options = format.default_options();
                state = format.default_state();
                if let Some(saved_metadata) = saved_metadata {
                    if saved_metadata.options_type == options.uuid() {
                        options = format.deserialize_options(saved_metadata.options)?;
                    }
                    if saved_metadata.state_type == state.uuid() {
                        state = format.deserialize_state(saved_metadata.state)?;
                    }
                }
            }
            Ok(Some(import_source(
                &source.path,
                source_hash,
                &*format,
                options,
                state,
                scratch_buf,
            )?))
        }
        None => Ok(None),
    }
}

impl FileAssetSource {
    pub fn new(
        tracker: &Arc<FileTracker>,
        hub: &Arc<AssetHub>,
        db: &Arc<Environment>,
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
        })
    }

    fn put_metadata<'a>(
        &self,
        txn: &'a mut RwTransaction,
        path: &PathBuf,
        metadata: &SourceMetadata<Box<dyn SerdeObj>, Box<dyn SerdeObj>>,
    ) -> Result<()> {
        let mut value_builder = capnp::message::Builder::new_default();
        {
            let mut value = value_builder.init_root::<source_metadata::Builder>();
            {
                value.set_importer_version(metadata.importer_version);
                value.set_importer_state_type(&metadata.importer_state.uuid().to_le_bytes());
                let mut state_buf = Vec::new();
                bincode::serialize_into(&mut state_buf, &metadata.importer_state)?;
                value.set_importer_state(&state_buf);
                value.set_importer_options_type(&metadata.importer_options.uuid().to_le_bytes());
                let mut options_buf = Vec::new();
                bincode::serialize_into(&mut options_buf, &metadata.importer_options)?;
                value.set_importer_options(&options_buf);
            }
            let mut assets = value.reborrow().init_assets(metadata.assets.len() as u32);
            for (idx, asset) in metadata.assets.iter().enumerate() {
                assets
                    .reborrow()
                    .get(idx as u32)
                    .set_id(asset.id.as_bytes());
            }
        }
        let key_str = path.to_string_lossy();
        let key = key_str.as_bytes();
        txn.put(self.tables.path_to_metadata, &key, &value_builder)?;
        Ok(())
    }

    fn get_metadata<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
        path: &PathBuf,
    ) -> Result<Option<MessageReader<'a, source_metadata::Owned>>> {
        let key_str = path.to_string_lossy();
        let key = key_str.as_bytes();
        Ok(txn.get::<source_metadata::Owned, &[u8]>(self.tables.path_to_metadata, &key)?)
    }

    fn delete_metadata(&self, txn: &mut RwTransaction, path: &PathBuf) -> Result<bool> {
        let key_str = path.to_string_lossy();
        let key = key_str.as_bytes();
        Ok(txn.delete(self.tables.path_to_metadata, &key)?)
    }

    fn put_asset_path<'a>(
        &self,
        txn: &'a mut RwTransaction,
        asset_id: &Uuid,
        path: &PathBuf,
    ) -> Result<()> {
        let path_str = path.to_string_lossy();
        let path = path_str.as_bytes();
        txn.put_bytes(self.tables.asset_id_to_path, asset_id.as_bytes(), &path)?;
        Ok(())
    }

    fn get_asset_path<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
        asset_id: &Uuid,
    ) -> Result<Option<PathBuf>> {
        match txn.get_as_bytes(self.tables.asset_id_to_path, asset_id.as_bytes())? {
            Some(p) => Ok(Some(PathBuf::from(
                str::from_utf8(p).expect("Encoded key was invalid utf8"),
            ))),
            None => Ok(None),
        }
    }

    fn delete_asset_path(&self, txn: &mut RwTransaction, asset_id: &Uuid) -> Result<bool> {
        Ok(txn.delete(self.tables.asset_id_to_path, asset_id.as_bytes())?)
    }

    fn process_pair_cases(
        &self,
        txn: &RoTransaction,
        pair: &HashedAssetFilePair,
        scratch_buf: &mut Vec<u8>,
    ) -> Result<Option<ImportResult>> {
        let original_pair = pair.clone();
        let mut pair = pair.clone();
        // When source or meta gets deleted, the FileState has a `state` of `Deleted`.
        // For the following pattern matching, we don't want to care about the distinction between this and absence of a file.
        if let HashedAssetFilePair {
            source:
                Some(FileState {
                    state: data::FileState::Deleted,
                    ..
                }),
            ..
        } = pair
        {
            pair.source = None;
        }
        if let HashedAssetFilePair {
            meta:
                Some(FileState {
                    state: data::FileState::Deleted,
                    ..
                }),
            ..
        } = pair
        {
            pair.meta = None;
        }

        match pair {
            // Source file has been deleted
            HashedAssetFilePair {
                meta: None,
                source: None,
                ..
            } => {
                if let HashedAssetFilePair {
                    source: Some(state),
                    ..
                } = original_pair
                {
                    debug!("deleted pair {}", state.path.to_string_lossy());
                } else if let HashedAssetFilePair {
                    meta: Some(state), ..
                } = original_pair
                {
                    debug!("deleted pair {}", state.path.to_string_lossy());
                }
                Ok(None)
            }
            // Source file with metadata
            HashedAssetFilePair {
                meta: Some(meta),
                meta_hash: Some(meta_hash),
                source: Some(source),
                source_hash: Some(source_hash),
            } => {
                debug!("full pair {}", source.path.to_string_lossy());
                import_pair(
                    &source,
                    source_hash,
                    Some(&meta),
                    Some(meta_hash),
                    scratch_buf,
                    None,
                )
            }
            // Source file with no metadata
            HashedAssetFilePair {
                meta: None,
                source: Some(source),
                source_hash: Some(hash),
                ..
            } => {
                let metadata = self.get_metadata(txn, &source.path)?;
                match metadata {
                    Some(metadata) => {
                        let metadata = metadata.get()?;
                        let saved_metadata = Some(SavedImportMetadata {
                            importer_version: metadata.get_importer_version(),
                            options_type: u128::from_le_bytes(utils::make_array(
                                metadata.get_importer_options_type()?,
                            )),
                            options: metadata.get_importer_options()?,
                            state_type: u128::from_le_bytes(utils::make_array(
                                metadata.get_importer_state_type()?,
                            )),
                            state: metadata.get_importer_state()?,
                        });
                        debug!("restored metadata for {:?}", saved_metadata);
                        import_pair(&source, hash, None, None, scratch_buf, saved_metadata)
                    }
                    None => {
                        debug!("no metadata for {}", source.path.to_string_lossy());
                        import_pair(&source, hash, None, None, scratch_buf, None)
                    }
                }
            }
            HashedAssetFilePair {
                meta: Some(_meta),
                meta_hash: Some(_hash),
                source: Some(source),
                source_hash: None,
            } => {
                debug!("directory {}", source.path.to_string_lossy());
                Ok(None)
            }
            HashedAssetFilePair {
                meta: Some(_meta),
                meta_hash: None,
                source: Some(source),
                source_hash: None,
            } => {
                debug!(
                    "directory with meta directory?? {}",
                    source.path.to_string_lossy()
                );
                Ok(None)
            }
            HashedAssetFilePair {
                meta: Some(_meta),
                meta_hash: None,
                source: Some(source),
                source_hash: Some(_hash),
            } => {
                debug!(
                    "source file with meta directory?? {}",
                    source.path.to_string_lossy()
                );
                Ok(None)
            }
            HashedAssetFilePair {
                meta: None,
                source: Some(source),
                source_hash: None,
                ..
            } => {
                debug!("directory with no meta {}", source.path.to_string_lossy());
                Ok(None)
            }
            HashedAssetFilePair {
                meta: Some(meta),
                meta_hash: Some(_meta_hash),
                source: None,
                ..
            } => {
                debug!(
                    "meta file without source file {}",
                    meta.path.to_string_lossy()
                );
                fs::remove_file(&meta.path)?;
                Ok(None)
            }
            _ => {
                debug!("Unknown case for {:?}", pair);
                Ok(None)
            }
        }
    }

    fn process_import_result(
        &self,
        txn: &mut RwTransaction,
        path: &PathBuf,
        result: Option<ImportResult>,
    ) -> Result<()> {
        if let Some(imported) = result {
            debug!("imported {}", path.to_string_lossy());
            let mut to_remove = Vec::new();
            if let Some(metadata) = self.get_metadata(txn, path)? {
                for asset in metadata.get()?.get_assets()? {
                    let id = Uuid::from_slice(asset.get_id()?)?;
                    debug!("asset {:?}", asset.get_id()?);
                    if imported.assets.iter().all(|a| a.metadata.id != id) {
                        to_remove.push(id);
                    }
                }
            }
            for asset in to_remove {
                debug!("removing deleted asset {:?}", asset);
                self.hub.remove_asset(txn, &asset)?;
                self.delete_asset_path(txn, &asset)?;
            }
            self.put_metadata(txn, path, &imported.metadata)?;
            for asset in imported.assets {
                debug!("updating asset {:?}", asset.metadata.id);
                self.hub.update_asset(
                    txn,
                    asset.import_hash,
                    &asset.metadata,
                    asset.asset.as_ref(),
                )?;
                match self.get_asset_path(txn, &asset.metadata.id)? {
                    Some(ref old_path) if old_path != path => error!(
                        "asset {:?} already in DB with path {} expected {}",
                        asset.metadata.id,
                        old_path.to_string_lossy(),
                        path.to_string_lossy(),
                    ),
                    Some(_) => {} // asset already in DB with correct path
                    _ => self.put_asset_path(txn, &asset.metadata.id, path)?,
                }
            }
        } else {
            debug!("deleting metadata for {}", path.to_string_lossy());
            let mut to_remove = Vec::new();
            {
                let metadata = self.get_metadata(txn, path)?;
                if let Some(ref metadata) = metadata {
                    for asset in metadata.get()?.get_assets()? {
                        to_remove.push(Uuid::from_slice(asset.get_id()?)?);
                    }
                }
            }
            for asset in to_remove {
                debug!("remove asset {:?}", asset);
                self.hub.remove_asset(txn, &asset)?;
            }
            self.delete_metadata(txn, path)?;
        }
        Ok(())
    }

    fn process_imported_pair(
        &self,
        txn: &mut RwTransaction,
        pair: &HashedAssetFilePair,
        imports: Result<Option<ImportResult>>,
    ) -> Result<()> {
        match imports {
            Ok(import_result) => {
                let path = &pair
                    .source
                    .as_ref()
                    .or_else(|| pair.meta.as_ref())
                    .expect("a successful import must have a source or meta FileState")
                    .path;
                self.process_import_result(txn, path, import_result)?;
                let mut skip_ack_dirty = false;
                {
                    let check_file_state = |s: &Option<&FileState>| -> Result<bool> {
                        match s {
                            Some(source) => {
                                let source_file_state =
                                    self.tracker.get_file_state(txn, &source.path)?;
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
                        self.tracker.delete_dirty_file_state(
                            txn,
                            pair.source.as_ref().map(|p| &p.path).unwrap(),
                        )?;
                    }
                    if pair.meta.is_some() {
                        self.tracker.delete_dirty_file_state(
                            txn,
                            pair.meta.as_ref().map(|p| &p.path).unwrap(),
                        )?;
                    }
                }
            }
            Err(e) => error!("Error processing pair: {}", e),
        };
        Ok(())
    }

    fn handle_rename_events(&self, txn: &mut RwTransaction) -> Result<()> {
        let rename_events = self.tracker.read_rename_events(txn)?;
        debug!("rename events");
        for (_, evt) in rename_events.iter() {
            let src_str = evt.src.to_string_lossy();
            let src = src_str.as_bytes();
            let dst_str = evt.dst.to_string_lossy();
            let dst = dst_str.as_bytes();
            let mut asset_ids = Vec::new();
            let mut existing_metadata = None;
            {
                let metadata =
                    txn.get::<source_metadata::Owned, &[u8]>(self.tables.path_to_metadata, &src)?;
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
                txn.delete(self.tables.path_to_metadata, &src)?;
                txn.put(self.tables.path_to_metadata, &dst, &existing_metadata)?;
            }

            debug!("src {} dst {} ", src_str, dst_str);
        }
        if !rename_events.is_empty() {
            self.tracker.clear_rename_events(txn)?;
        }
        Ok(())
    }
    fn handle_update(&self, thread_pool: &mut Pool) -> Result<()> {
        let start_time = PreciseTime::now();
        let source_meta_pairs = {
            let mut txn = self.db.rw_txn()?;
            // Before reading the filesystem state we need to process rename events.
            // This must be done in the same transaction to stay consistent.
            self.handle_rename_events(&mut txn)?;

            let mut source_meta_pairs: HashMap<PathBuf, AssetFilePair> = HashMap::new();
            let dirty_files = self.tracker.read_dirty_files(&txn)?;
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
                    let mut pair = source_meta_pairs.entry(base_path).or_insert(AssetFilePair {
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
                        pair.meta = self.tracker.get_file_state(&txn, &to_meta_path(&path))?;
                    }
                    if pair.source.is_none() {
                        pair.source = self.tracker.get_file_state(&txn, &path)?;
                    }
                }
            }
            if txn.dirty {
                txn.commit()?;
            }
            source_meta_pairs
        };

        let hashed_files = hash_files(Vec::from_iter(source_meta_pairs.values()));
        for result in &hashed_files {
            match result {
                Err(err) => {
                    error!("Hashing error: {}", err);
                }
                Ok(_) => {}
            }
        }
        let hashed_files = Vec::from_iter(
            hashed_files
                .into_iter()
                .filter(|f| !f.is_err())
                .map(|e| e.unwrap()),
        );
        debug!("Hashed {}", hashed_files.len());
        debug!(
            "Hashed {} pairs in {}",
            source_meta_pairs.len(),
            start_time.to(PreciseTime::now())
        );

        let mut txn = self.db.rw_txn()?;
        use std::cell::RefCell;
        thread_local!(static SCRATCH_STORE: RefCell<Option<Vec<u8>>> = RefCell::new(None));

        // Should get rid of this scoped_threadpool madness somehow,
        // but can't use par_iter directly since I need to process the results
        // as soon as they are completed. So essentially I want futures::stream::FuturesUnordered.
        // But I couldn't figure all that future stuff out, so here we are. Scoped threadpool.
        thread_pool.scoped(|scope| -> Result<()> {
            let (tx, rx) = channel::unbounded();
            let to_process = hashed_files.len();
            let mut import_iter = hashed_files.iter().map(|p| {
                let processed_pair = p.clone();
                let sender = tx.clone();
                scope.execute(move || {
                    SCRATCH_STORE.with(|cell| {
                        let mut local_store = cell.borrow_mut();
                        if local_store.is_none() {
                            *local_store = Some(Vec::new());
                        }
                        match self.db.ro_txn() {
                            Err(e) => {
                                sender.send((processed_pair, Err(e))).unwrap();
                            }
                            Ok(read_txn) => {
                                let result = self.process_pair_cases(
                                    &read_txn,
                                    &processed_pair,
                                    local_store.as_mut().unwrap(),
                                );
                                sender.send((processed_pair, result)).unwrap();
                            }
                        }
                    });
                });
            });

            let num_queued_imports = num_cpus::get() * 2;
            for _ in 0..num_queued_imports {
                import_iter.next();
            }

            let mut num_processed = 0;
            while num_processed < to_process {
                match rx.recv() {
                    Ok(import) => {
                        self.process_imported_pair(&mut txn, &import.0, import.1)?;
                        num_processed += 1;
                        import_iter.next();
                    }
                    _ => {
                        break;
                    }
                }
            }
            Ok(())
        })?;
        if txn.dirty {
            txn.commit()?;
        }
        info!(
            "Processing {} pairs in {}",
            source_meta_pairs.len(),
            start_time.to(PreciseTime::now())
        );
        Ok(())
    }

    pub fn run(&self) -> Result<()> {
        let mut thread_pool = Pool::new(num_cpus::get() as u32);
        loop {
            match self.rx.recv() {
                Ok(_evt) => {
                    self.handle_update(&mut thread_pool)?;
                }
                Err(_) => {
                    return Ok(());
                }
            }
        }
    }
}
