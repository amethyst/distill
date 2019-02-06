use crate::asset_daemon::ImporterMap;
use crate::asset_hub::{self, AssetHub};
use crate::capnp_db::{DBTransaction, Environment, MessageReader, RoTransaction, RwTransaction};
use crate::error::{Error, Result};
use crate::file_tracker::{FileState, FileTracker, FileTrackerEvent};
use crate::utils;
use crate::watcher::file_metadata;
use bincode;
use crossbeam_channel::{self as channel, Receiver};
use importer::{
    AssetMetadata, AssetUUID, BoxedImporter, SerdeObj, SourceMetadata as ImporterSourceMetadata,
    SOURCEMETADATA_VERSION,
};
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
    io::{self, BufRead, Read, Write},
    iter::FromIterator,
    path::PathBuf,
    str,
    sync::Arc,
};
use time::PreciseTime;

pub type SourceMetadata = ImporterSourceMetadata<Box<dyn SerdeObj>, Box<dyn SerdeObj>>;

pub struct FileAssetSource {
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
    asset_hash: u64,
    metadata: AssetMetadata,
    asset: Option<Vec<u8>>,
}

#[derive(Debug)]
pub struct SavedImportMetadata<'a> {
    importer_version: u32,
    options_type: u128,
    options: &'a [u8],
    state_type: u128,
    state: &'a [u8],
    build_pipelines: HashMap<AssetUUID, AssetUUID>,
}

#[derive(Default)]
pub struct PairImport<'a> {
    source: PathBuf,
    importer: Option<&'a dyn BoxedImporter>,
    source_hash: Option<u64>,
    meta_hash: Option<u64>,
    import_hash: Option<u64>,
    source_metadata: Option<SourceMetadata>,
}

impl<'a> PairImport<'a> {
    pub fn new(source: PathBuf) -> PairImport<'a> {
        PairImport {
            source,
            ..Default::default()
        }
    }
    pub fn with_source_hash(&mut self, source_hash: u64) {
        self.source_hash = Some(source_hash);
    }
    pub fn with_meta_hash(&mut self, meta_hash: u64) {
        self.meta_hash = Some(meta_hash);
    }
    pub fn hash_source(&mut self) -> Result<()> {
        let (_, hash) = hash_file(&FileState {
            path: self.source.clone(),
            state: data::FileState::Exists,
            last_modified: 0,
            length: 0,
        })?;
        self.source_hash =
            Some(hash.ok_or_else(|| Error::IO(io::Error::from(io::ErrorKind::NotFound)))?);
        Ok(())
    }
    /// Returns true if an appropriate importer was found, otherwise false.
    pub fn with_importer_from_map(&mut self, importers: &'a ImporterMap) -> Result<bool> {
        let lower_extension = self
            .source
            .extension()
            .map(|s| s.to_str().unwrap().to_lowercase())
            .unwrap_or_else(|| "".to_string());
        self.importer = importers.get(lower_extension.as_str()).map(|i| i.as_ref());
        Ok(self.importer.is_some())
    }
    pub fn needs_source_import(&mut self, scratch_buf: &mut Vec<u8>) -> Result<bool> {
        if let Some(ref metadata) = self.source_metadata {
            if metadata.import_hash.is_none() {
                return Ok(true);
            }
            if self.import_hash.is_none() {
                self.import_hash = Some(self.calc_import_hash(
                    metadata.importer_options.as_ref(),
                    metadata.importer_state.as_ref(),
                    scratch_buf,
                )?);
            }
            Ok(self.import_hash.unwrap() != metadata.import_hash.unwrap())
        } else {
            Ok(true)
        }
    }
    fn calc_import_hash(
        &self,
        options: &dyn SerdeObj,
        state: &dyn SerdeObj,
        scratch_buf: &mut Vec<u8>,
    ) -> Result<u64> {
        let mut hasher = ::std::collections::hash_map::DefaultHasher::new();
        scratch_buf.clear();
        bincode::serialize_into(&mut *scratch_buf, &options)?;
        scratch_buf.hash(&mut hasher);
        scratch_buf.clear();
        bincode::serialize_into(&mut *scratch_buf, &state)?;
        scratch_buf.hash(&mut hasher);
        self.source_hash
            .expect("cannot calculate import hash without source hash")
            .hash(&mut hasher);
        self.importer
            .expect("cannot calculate import hash without importer")
            .version()
            .hash(&mut hasher);
        Ok(hasher.finish())
    }

    pub fn read_metadata_from_file(&mut self, scratch_buf: &mut Vec<u8>) -> Result<()> {
        let importer = self
            .importer
            .expect("cannot read metadata without an importer");
        let meta = to_meta_path(&self.source);
        let mut f = fs::File::open(meta)?;
        scratch_buf.clear();
        f.read_to_end(scratch_buf)?;
        self.source_metadata = Some(importer.deserialize_metadata(scratch_buf)?);
        Ok(())
    }

    pub fn default_or_saved_metadata(
        &mut self,
        saved_metadata: Option<SavedImportMetadata>,
    ) -> Result<()> {
        let importer = self
            .importer
            .expect("cannot create metadata without an importer");
        let mut options = importer.default_options();
        let mut state = importer.default_state();
        let mut build_pipelines = HashMap::new();
        if let Some(saved_metadata) = saved_metadata {
            if saved_metadata.options_type == options.uuid() {
                options = importer.deserialize_options(saved_metadata.options)?;
            }
            if saved_metadata.state_type == state.uuid() {
                state = importer.deserialize_state(saved_metadata.state)?;
            }
            build_pipelines = saved_metadata.build_pipelines;
        }
        self.source_metadata = Some(SourceMetadata {
            version: SOURCEMETADATA_VERSION,
            import_hash: None,
            importer_version: importer.version(),
            importer_options: options,
            importer_state: state,
            assets: build_pipelines
                .iter()
                .map(|(id, pipeline)| AssetMetadata {
                    id: *id,
                    build_pipeline: Some(*pipeline),
                    ..Default::default()
                })
                .collect(),
        });
        Ok(())
    }

    fn import_source(&mut self, scratch_buf: &mut Vec<u8>) -> Result<Vec<ImportedAsset>> {
        let importer = self
            .importer
            .expect("cannot import source without importer");

        let metadata = std::mem::replace(&mut self.source_metadata, None)
            .expect("cannot import source file without source_metadata");
        let imported = {
            let mut f = fs::File::open(&self.source)?;
            importer.import_boxed(&mut f, metadata.importer_options, metadata.importer_state)?
        };
        let options = imported.options;
        let state = imported.state;
        let imported = imported.value;
        let mut imported_assets = Vec::new();
        let import_hash = self.calc_import_hash(options.as_ref(), state.as_ref(), scratch_buf)?;
        for mut asset in imported.assets {
            asset.search_tags.push((
                "file_name".to_string(),
                Some(
                    self.source
                        .file_name()
                        .expect("failed to get file stem")
                        .to_string_lossy()
                        .to_string(),
                ),
            ));
            let asset_data = &asset.asset_data;
            let size = bincode::serialized_size(asset_data)? as usize;
            scratch_buf.clear();
            scratch_buf.resize(size, 0);
            bincode::serialize_into(scratch_buf.as_mut_slice(), asset_data)?;
            let asset_buf = {
                let mut encoder = lz4::EncoderBuilder::new()
                    .level(9)
                    .build(Vec::with_capacity(size / 2))?;
                encoder.write_all(&scratch_buf[..size])?;
                let (output, result) = encoder.finish();
                result?;
                output
            };
            let build_pipeline = metadata
                .assets
                .iter()
                .find(|a| a.id == asset.id)
                .map(|m| m.build_pipeline)
                .unwrap_or(None);
            imported_assets.push({
                ImportedAsset {
                    asset_hash: calc_asset_hash(&asset.id, import_hash),
                    metadata: AssetMetadata {
                        id: asset.id,
                        search_tags: asset.search_tags,
                        build_deps: asset.build_deps,
                        load_deps: asset.load_deps,
                        instantiate_deps: asset.instantiate_deps,
                        build_pipeline,
                    },
                    asset: Some(asset_buf),
                }
            });
            debug!(
                "Import success {} read {} bytes",
                self.source.to_string_lossy(),
                scratch_buf.len(),
            );
        }
        self.source_metadata = Some(SourceMetadata {
            version: SOURCEMETADATA_VERSION,
            import_hash: Some(import_hash),
            importer_version: importer.version(),
            importer_options: options,
            importer_state: state,
            assets: Vec::from_iter(imported_assets.iter().map(|m| m.metadata.clone())),
        });
        Ok(imported_assets)
    }

    pub fn write_metadata(&self) -> Result<()> {
        let serialized_metadata = ron::ser::to_string_pretty(
            self.source_metadata
                .as_ref()
                .expect("source_metadata missing"),
            ron::ser::PrettyConfig::default(),
        )
        .unwrap();
        let meta_path = to_meta_path(&self.source);
        let mut meta_file = fs::File::create(meta_path)?;
        meta_file.write_all(serialized_metadata.as_bytes())?;
        Ok(())
    }
}

fn to_meta_path(p: &PathBuf) -> PathBuf {
    p.with_file_name(OsStr::new(
        &(p.file_name().unwrap().to_str().unwrap().to_owned() + ".meta"),
    ))
}

fn calc_asset_hash(id: &AssetUUID, import_hash: u64) -> u64 {
    let mut hasher = ::std::collections::hash_map::DefaultHasher::new();
    import_hash.hash(&mut hasher);
    id.hash(&mut hasher);
    hasher.finish()
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
pub fn get_saved_import_metadata<'a>(
    metadata: &source_metadata::Reader<'a>,
) -> Result<SavedImportMetadata<'a>> {
    let mut build_pipelines = HashMap::new();
    for pair in metadata.get_build_pipelines()?.iter() {
        build_pipelines.insert(
            AssetUUID::from_bytes(utils::make_array(&pair.get_key()?.get_id()?)),
            AssetUUID::from_bytes(utils::make_array(&pair.get_value()?.get_id()?)),
        );
    }
    Ok(SavedImportMetadata {
        importer_version: metadata.get_importer_version(),
        options_type: u128::from_le_bytes(utils::make_array(metadata.get_importer_options_type()?)),
        options: metadata.get_importer_options()?,
        state_type: u128::from_le_bytes(utils::make_array(metadata.get_importer_state_type()?)),
        state: metadata.get_importer_state()?,
        build_pipelines: build_pipelines,
    })
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
        txn: &'a mut RwTransaction,
        path: &PathBuf,
        metadata: &SourceMetadata,
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
                    .set_id(asset.id.as_bytes());
                build_pipelines
                    .reborrow()
                    .get(idx as u32)
                    .init_value()
                    .set_id(asset.build_pipeline.unwrap().as_bytes());
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

    fn delete_metadata(&self, txn: &mut RwTransaction, path: &PathBuf) -> Result<bool> {
        let key_str = path.to_string_lossy();
        let key = key_str.as_bytes();
        Ok(txn.delete(self.tables.path_to_metadata, &key)?)
    }

    fn put_asset_path<'a>(
        &self,
        txn: &'a mut RwTransaction,
        asset_id: &AssetUUID,
        path: &PathBuf,
    ) -> Result<()> {
        let path_str = path.to_string_lossy();
        let path = path_str.as_bytes();
        txn.put_bytes(self.tables.asset_id_to_path, asset_id.as_bytes(), &path)?;
        Ok(())
    }

    pub fn get_asset_path<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
        asset_id: &AssetUUID,
    ) -> Result<Option<PathBuf>> {
        match txn.get_as_bytes(self.tables.asset_id_to_path, asset_id.as_bytes())? {
            Some(p) => Ok(Some(PathBuf::from(
                str::from_utf8(p).expect("Encoded key was invalid utf8"),
            ))),
            None => Ok(None),
        }
    }

    fn delete_asset_path(&self, txn: &mut RwTransaction, asset_id: &AssetUUID) -> Result<bool> {
        Ok(txn.delete(self.tables.asset_id_to_path, asset_id.as_bytes())?)
    }

    pub fn regenerate_import_artifact<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
        id: &AssetUUID,
        scratch_buf: &mut Vec<u8>,
    ) -> Result<Option<(u64, Vec<u8>)>> {
        let path = self.get_asset_path(txn, id)?;
        if let Some(path) = path {
            let metadata = self.get_metadata(txn, &path)?;
            let saved_metadata = if let Some(ref metadata) = metadata {
                Some(get_saved_import_metadata(&metadata.get()?)?)
            } else {
                None
            };
            let mut import = PairImport::new(path);
            import.with_importer_from_map(&self.importers)?;
            import.default_or_saved_metadata(saved_metadata)?;
            import.hash_source()?;
            let imported_assets = import.import_source(scratch_buf)?;
            Ok(imported_assets
                .into_iter()
                .find(|a| a.metadata.id == *id)
                .map(|a| {
                    a.asset.map(|a| {
                    (
                        calc_asset_hash(id, import.source_metadata.map(|m| m.import_hash.unwrap()).unwrap()),
                        a,
                    )})
                }).unwrap_or(None)
                )
        } else {
            Ok(None)
        }
    }

    fn process_pair_cases(
        &self,
        txn: &RoTransaction,
        pair: &HashedAssetFilePair,
        scratch_buf: &mut Vec<u8>,
    ) -> Result<Option<(PairImport, Option<Vec<ImportedAsset>>)>> {
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
                meta: Some(_meta),
                meta_hash: Some(meta_hash),
                source: Some(source),
                source_hash: Some(source_hash),
            } => {
                debug!("full pair {}", source.path.to_string_lossy());
                let mut import = PairImport::new(source.path);
                import.with_source_hash(source_hash);
                import.with_meta_hash(meta_hash);
                if !import.with_importer_from_map(&self.importers)? {
                    Ok(None)
                } else {
                    import.read_metadata_from_file(scratch_buf)?;
                    if import.needs_source_import(scratch_buf)? {
                        let imported_assets = import.import_source(scratch_buf)?;
                        import.write_metadata()?;
                        Ok(Some((import, Some(imported_assets))))
                    } else {
                        Ok(Some((import, None)))
                    }
                }
            }
            // Source file with no metadata
            HashedAssetFilePair {
                meta: None,
                source: Some(source),
                source_hash: Some(hash),
                ..
            } => {
                let metadata = self.get_metadata(txn, &source.path)?;
                let saved_metadata = if let Some(ref m) = metadata {
                    Some(get_saved_import_metadata(&m.get()?)?)
                } else {
                    None
                };
                let mut import = PairImport::new(source.path);
                import.with_source_hash(hash);
                if !import.with_importer_from_map(&self.importers)? {
                    Ok(None)
                } else {
                    import.default_or_saved_metadata(saved_metadata)?;
                    if import.needs_source_import(scratch_buf)? {
                        let imported_assets = import.import_source(scratch_buf)?;
                        import.write_metadata()?;
                        Ok(Some((import, Some(imported_assets))))
                    } else {
                        Ok(Some((import, None)))
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

    fn process_metadata_changes(
        &self,
        txn: &mut RwTransaction,
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
                        to_remove.push(AssetUUID::from_slice(asset.get_id()?)?);
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
                    let id = AssetUUID::from_slice(asset.get_id()?)?;
                    debug!("asset {:?}", asset.get_id()?);
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
                debug!("updating asset {:?}", asset.id);
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
                        calc_asset_hash(
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
        txn: &mut RwTransaction,
        pair: &HashedAssetFilePair,
    ) -> Result<()> {
        let mut skip_ack_dirty = false;
        {
            let check_file_state = |s: &Option<&FileState>| -> Result<bool> {
                match s {
                    Some(source) => {
                        let source_file_state = self.tracker.get_file_state(txn, &source.path)?;
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
            debug!(
                "src {} dst {} had metadata {}",
                src_str,
                dst_str,
                existing_metadata.is_some()
            );
            if let Some(existing_metadata) = existing_metadata {
                txn.delete(self.tables.path_to_metadata, &src)?;
                txn.put(self.tables.path_to_metadata, &dst, &existing_metadata)?;
            }
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
                debug!("Processing {} changed file pairs", source_meta_pairs.len());
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

        let mut asset_metadata_changed = false;
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
            let mut metadata_changes = HashMap::new();
            while num_processed < to_process {
                match rx.recv() {
                    Ok((pair, maybe_result)) => {
                        match maybe_result {
                            // Successful import
                            Ok(result) => {
                                let path = &pair
                                    .source
                                    .as_ref()
                                    .or_else(|| pair.meta.as_ref())
                                    .expect(
                                        "a successful import must have a source or meta FileState",
                                    )
                                    .path;
                                self.ack_dirty_file_states(&mut txn, &pair)?;
                                // TODO put import artifact in cache
                                metadata_changes.insert(
                                    path.clone(),
                                    result.map(|r| r.0.source_metadata.unwrap()),
                                );
                            }
                            Err(e) => error!("Error processing pair: {}", e),
                        }
                        num_processed += 1;
                        import_iter.next();
                    }
                    _ => {
                        break;
                    }
                }
            }
            let mut change_batch = asset_hub::ChangeBatch::new();
            self.process_metadata_changes(&mut txn, metadata_changes, &mut change_batch)?;
            asset_metadata_changed = self.hub.add_changes(&mut txn, change_batch)?;
            Ok(())
        })?;
        if txn.dirty {
            txn.commit()?;
            if asset_metadata_changed {
                self.hub.notify_listeners();
            }
        }
        info!(
            "Processed {} pairs in {}",
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
