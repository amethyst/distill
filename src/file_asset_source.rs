use asset_hub::AssetHub;
use asset_import::{
    format_from_ext, AssetMetadata, SerdeObj, SourceMetadata, SOURCEMETADATA_VERSION,
};
use bincode;
use capnp_db::{DBTransaction, Environment, RwTransaction};
use crossbeam_channel::{self as channel, Receiver};
use data_capnp;
use error::{Error, Result};
use file_tracker::{FileState, FileTracker, FileTrackerEvent};
use rayon::prelude::*;
use ron;
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
    sync::Arc,
};
use watcher::file_metadata;

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
    asset_id_to_path: lmdb::Database,
}

// Only files get Some(hash)
#[derive(Clone, Debug)]
struct HashedAssetFilePair {
    source: Option<(FileState, Option<u64>)>,
    meta: Option<(FileState, Option<u64>)>,
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
                    state: data_capnp::FileState::Exists,
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
        let meta = match s.meta {
            Some(ref state) if state.state == data_capnp::FileState::Exists => {
                Some(hash_file(state)?)
            }
            Some(ref state) if state.state == data_capnp::FileState::Deleted => {
                Some((state.clone(), None))
            }
            _ => None,
        };
        let source = match s.source {
            Some(ref state) if state.state == data_capnp::FileState::Exists => {
                Some(hash_file(&state)?)
            }
            Some(ref state) if state.state == data_capnp::FileState::Deleted => {
                Some((state.clone(), None))
            }
            _ => None,
        };
        Ok(HashedAssetFilePair { meta, source })
    }))
}

fn calc_import_hash<K>(
    scratch_buf: &mut Vec<u8>,
    importer_options: &Box<SerdeObj>,
    importer_state: &Box<SerdeObj>,
    source_hash: u64,
    asset_id: &K,
) -> u64
where
    K: AsRef<[u8]> + Hash,
{
    let mut hasher = ::std::collections::hash_map::DefaultHasher::new();
    scratch_buf.clear();
    bincode::serialize_into(&mut *scratch_buf, importer_options);
    scratch_buf.hash(&mut hasher);
    scratch_buf.clear();
    bincode::serialize_into(&mut *scratch_buf, importer_state);
    scratch_buf.hash(&mut hasher);
    source_hash.hash(&mut hasher);
    asset_id.hash(&mut hasher);
    hasher.finish()
}

fn import_pair(
    source: &FileState,
    source_hash: u64,
    meta: Option<&FileState>,
    meta_hash: Option<u64>,
    scratch_buf: &mut Vec<u8>,
) -> Result<Option<ImportResult>> {
    let format = format_from_ext(source.path.extension().unwrap().to_str().unwrap());
    match format {
        Some(format) => {
            let options;
            let state;
            if let Some(_meta) = meta {
                let meta_path = to_meta_path(&source.path);
                let mut f = fs::File::open(&meta_path)?;
                let mut bytes = Vec::new();
                f.read_to_end(&mut bytes)?;
                let metadata = format.deserialize_metadata(&bytes);
                if metadata.source_hash == source_hash {
                    return Ok(Some(ImportResult {
                        assets: Vec::from_iter(metadata.assets.iter().map(|m| ImportedAsset {
                            import_hash: calc_import_hash(
                                scratch_buf,
                                &metadata.importer_options,
                                &metadata.importer_state,
                                source_hash,
                                &m.id,
                            ),
                            metadata: m.clone(),
                            asset: None,
                        })),
                        metadata: metadata,
                    }));
                }
                options = metadata.importer_options;
                state = metadata.importer_state;
            } else {
                options = format.default_options();
                state = format.default_state();
            }

            let mut f = fs::File::open(&source.path)?;
            let mut imported = format.import_boxed(&mut f, options, state);
            match imported {
                Ok(imported) => {
                    let options = imported.options;
                    let state = imported.state;
                    let mut imported = imported.value;
                    let mut imported_assets = Vec::new();
                    for mut asset in imported.assets {
                        asset.search_tags.push((
                            "file_name".to_string(),
                            Some(
                                source
                                    .path
                                    .file_name()
                                    .expect("failed to get file stem")
                                    .to_string_lossy()
                                    .to_string(),
                            ),
                        ));
                        let asset_data = &asset.asset_data;
                        let mut asset_buf = Vec::new();
                        bincode::serialize_into(&mut asset_buf, asset_data);
                        imported_assets.push({
                            ImportedAsset {
                                import_hash: calc_import_hash(
                                    scratch_buf,
                                    &options,
                                    &state,
                                    source_hash,
                                    &asset.id,
                                ),
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
                        println!(
                            "Import success {} read {}",
                            source.path.to_string_lossy(),
                            scratch_buf.len(),
                        );
                    }
                    let source_metadata = SourceMetadata {
                        version: SOURCEMETADATA_VERSION,
                        source_hash,
                        importer_version: format.version(),
                        importer_options: options,
                        importer_state: state,
                        assets: Vec::from_iter(imported_assets.iter().map(|m| m.metadata.clone())),
                    };
                    let serialized_metadata = ron::ser::to_string_pretty(
                        &source_metadata,
                        ron::ser::PrettyConfig::default(),
                    )
                    .unwrap();
                    let meta_path = to_meta_path(&source.path);
                    // println!("Meta file {}: {}", meta_path.to_string_lossy(), serialized_metadata);
                    let mut meta_file = fs::File::create(meta_path)?;
                    meta_file.write_all(serialized_metadata.as_bytes())?;
                    Ok(Some(ImportResult {
                        metadata: source_metadata,
                        assets: imported_assets,
                    }))
                }
                Err(err) => {
                    println!(
                        "Import error {} path {}",
                        err,
                        source.path.to_string_lossy()
                    );
                    Ok(None)
                }
            }
        }
        None => Ok(None),
    }
}

fn process_pair_cases(
    pair: &mut HashedAssetFilePair,
    scratch_buf: &mut Vec<u8>,
) -> Result<Option<ImportResult>> {
    match pair {
        HashedAssetFilePair {
            meta:
                Some((
                    FileState {
                        state: data_capnp::FileState::Deleted,
                        ..
                    },
                    None,
                )),
            source:
                Some((
                    FileState {
                        state: data_capnp::FileState::Deleted,
                        ..
                    },
                    None,
                )),
        } => Ok(None),
        HashedAssetFilePair {
            meta: Some((meta, Some(meta_hash))),
            source: Some((source, Some(source_hash))),
        } => {
            // println!("full pair {}", source.path.to_string_lossy());
            import_pair(
                source,
                *source_hash,
                Some(meta),
                Some(*meta_hash),
                scratch_buf,
            )
        }
        HashedAssetFilePair {
            meta: Some((_meta, Some(_hash))),
            source: Some((_source, None)),
        } => {
            // println!("directory {}", source.path.to_string_lossy());
            Ok(None)
        }
        HashedAssetFilePair {
            meta: Some((_meta, None)),
            source: Some((source, None)),
        } => {
            println!(
                "directory with meta directory?? {}",
                source.path.to_string_lossy()
            );
            Ok(None)
        }
        HashedAssetFilePair {
            meta: Some((_meta, None)),
            source: Some((source, Some(_hash))),
        } => {
            println!(
                "source file with meta directory?? {}",
                source.path.to_string_lossy()
            );
            Ok(None)
        }
        HashedAssetFilePair {
            meta: None,
            source: Some((source, Some(hash))),
        } => {
            import_pair(source, *hash, None, None, scratch_buf)
            // println!("source file with no meta {}", source.path.to_string_lossy());
        }
        HashedAssetFilePair {
            meta: None,
            source: Some((_source, None)),
        } => {
            // println!("directory with no meta {}", source.path.to_string_lossy());
            Ok(None)
        }
        HashedAssetFilePair {
            meta: Some((meta, Some(_hash))),
            source: None,
        } => {
            println!(
                "meta file without source file {}",
                meta.path.to_string_lossy()
            );
            fs::remove_file(&meta.path)?;
            Ok(None)
        }
        _ => {
            println!("Unknown case for {:?}", pair);
            Ok(None)
        }
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

    pub fn get_metadata<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
        path: &PathBuf,
    ) -> Result<Option<FileState>> {
        let key_str = path.to_string_lossy();
        let key = key_str.as_bytes();
        match txn.get(self.tables.path_to_metadata, &key)? {
            Some(value) => {
                let info = value
                    .get_root::<dirty_file_info::Reader>()?
                    .get_source_info()?;
                Ok(Some(FileState {
                    path: path.clone(),
                    state: data_capnp::FileState::Exists,
                    last_modified: info.get_last_modified(),
                    length: info.get_length(),
                }))
            }
            None => Ok(None),
        }
    }


    fn delete_path_entry(&self, txn: &mut RwTransaction, path: &PathBuf) {

    }

    fn process_import(
        &self,
        txn: &mut RwTransaction,
        pair: &HashedAssetFilePair,
        imports: Result<Option<ImportResult>>,
    ) -> Result<()> {
        match imports {
            Ok(import_result) => {
                match import_result {
                    Some(imported) => {
                        for asset in imported.assets {
                            self.hub.update_asset::<&Vec<u8>>(
                                txn,
                                asset.import_hash,
                                &asset.metadata,
                                asset.asset.as_ref(),
                            )?;
                        }
                    }
                    None => {
                        self.delete_path_entry(txn, pair.)
                    }
                }
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
                    skip_ack_dirty |= check_file_state(&pair.source.as_ref().map(|f| &f.0))?;
                    skip_ack_dirty |= check_file_state(&pair.meta.as_ref().map(|f| &f.0))?;
                }
                if !skip_ack_dirty {
                    if pair.source.is_some() {
                        self.tracker.delete_dirty_file_state(
                            txn,
                            pair.source.as_ref().map(|p| &p.0.path).unwrap(),
                        )?;
                    }
                    if pair.meta.is_some() {
                        self.tracker.delete_dirty_file_state(
                            txn,
                            pair.meta.as_ref().map(|p| &p.0.path).unwrap(),
                        )?;
                    }
                }
            }
            Err(e) => println!("Error processing pair: {}", e),
        };
        Ok(())
    }

    fn handle_update(&self, thread_pool: &mut Pool) -> Result<()> {
        let source_meta_pairs = {
            let mut source_meta_pairs: HashMap<PathBuf, AssetFilePair> = HashMap::new();
            let txn = self.tracker.get_ro_txn()?;
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
            source_meta_pairs
        };

        let hashed_files = hash_files(Vec::from_iter(source_meta_pairs.values()));
        for result in &hashed_files {
            match result {
                Err(err) => {
                    println!(
                        "Error hashing {}",
                        // state.path.to_string_lossy(),
                        err
                    );
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
        println!("Hashed {}", hashed_files.len());

        {
            let mut txn = self.tracker.get_rw_txn()?;
            use std::cell::RefCell;
            thread_local!(static SCRATCH_STORE: RefCell<Option<Vec<u8>>> = RefCell::new(None));

            thread_pool.scoped(|scope| -> Result<()> {
                let (tx, rx) = channel::unbounded();
                let to_process = hashed_files.len();
                let mut import_iter = hashed_files.iter().map(|p| {
                    let mut processed_pair = p.clone();
                    let sender = tx.clone();
                    scope.execute(move || {
                        SCRATCH_STORE.with(|cell| {
                            let mut local_store = cell.borrow_mut();
                            if local_store.is_none() {
                                *local_store = Some(Vec::new());
                            }
                            let result = process_pair_cases(
                                &mut processed_pair,
                                local_store.as_mut().unwrap(),
                            );
                            sender.send((processed_pair, result));
                        });
                    });
                });

                for _ in 0..20 {
                    import_iter.next();
                }
                let mut num_processed = 0;
                while num_processed < to_process {
                    match rx.recv() {
                        Some(import) => {
                            self.process_import(&mut txn, &import.0, import.1)?;
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
            // let successful_hashes = filter_and_unwrap_hashes(hashed_files);
            // for file in deleted.iter() {
            //     println!("deleted: {}", file.path.to_string_lossy());
            //     self.tracker.delete_dirty_file_state(&mut txn, &file.path)?;
            // }
            txn.commit()?;
        }
        Ok(())
    }

    pub fn run(&self) -> Result<()> {
        let mut thread_pool = Pool::new(num_cpus::get() as u32);
        loop {
            match self.rx.recv() {
                Some(_evt) => {
                    self.handle_update(&mut thread_pool)?;
                }
                None => {
                    return Ok(());
                }
            }
        }
    }
}
