use asset_import::{format_from_ext, AssetMetadata, SourceMetadata, SOURCEMETADATA_VERSION};
use bincode;
use crossbeam_channel::{self as channel, Receiver};
use data_capnp;
use file_error::FileError;
use file_tracker::{FileState, FileTracker, FileTrackerEvent};
use rayon::prelude::*;
use ron;
use std::collections::HashMap;
use std::{
    ffi::OsStr,
    fs,
    hash::Hasher,
    io::BufRead,
    io::{Read, Write},
    iter::FromIterator,
    path::PathBuf,
    sync::Arc,
};
use watcher::file_metadata;

pub struct AssetHub {
    tracker: Arc<FileTracker>,
    rx: Receiver<FileTrackerEvent>,
    tables: AssetHubTables,
}

pub struct AssetHubTables {
    /// Maps path of non-meta source file to SourcePairInfo
    /// Path -> SourcePairInfo
    source_pairs: lmdb::Database,
    /// Maps the hash of (source, meta, importer_version, importer_options) to an import artifact
    /// ImportArtifactKey -> ImportArtifact
    import_artifacts: lmdb::Database,
    /// Maps an AssetID to its location within an ImportArtifact
    /// AssetID -> ImportArtifactLocation
    asset_to_import_artifact: lmdb::Database,
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

fn to_meta_path(p: &PathBuf) -> PathBuf {
    p.with_file_name(OsStr::new(
        &(p.file_name().unwrap().to_str().unwrap().to_owned() + ".meta"),
    ))
}

fn hash_file(state: &FileState) -> Result<(FileState, Option<u64>), FileError> {
    let metadata = match fs::metadata(&state.path) {
        Err(e) => return Err(FileError::IO(e)),
        Ok(m) => {
            if !m.is_file() {
                return Ok((state.clone(), None));
            }
            file_metadata(m)
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
        .map_err(|e| FileError::IO(e))?)
}

fn hash_files<'a, T, I>(pairs: I) -> Vec<Result<HashedAssetFilePair, FileError>>
where
    I: IntoParallelIterator<Item = &'a AssetFilePair, Iter = T>,
    T: ParallelIterator<Item = &'a AssetFilePair>,
{
    Vec::from_par_iter(pairs.into_par_iter().map(|s| {
        let meta;
        match s.meta {
            Some(ref m) => meta = Some(hash_file(&m)?),
            None => meta = None,
        }
        let source;
        match s.source {
            Some(ref m) => source = Some(hash_file(&m)?),
            None => source = None,
        }
        Ok(HashedAssetFilePair {
            meta: meta,
            source: source,
        })
    }))
}

fn import_pair(
    source: &FileState,
    source_hash: u64,
    meta: Option<&FileState>,
    meta_hash: Option<u64>,
    scratch_buf: &mut Vec<u8>,
) -> Result<(), FileError> {
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
                    return Ok(());
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
                    let mut asset_metadata = Vec::new();
                    for mut asset in imported.assets {
                        asset.search_tags.push((
                            "file_name".to_string(),
                            Some(
                                source
                                    .path
                                    .file_stem()
                                    .expect("failed to get file stem")
                                    .to_string_lossy()
                                    .to_string(),
                            ),
                        ));
                        let asset_data = &asset.asset_data;
                        scratch_buf.clear();
                        bincode::serialize_into(&mut *scratch_buf, asset_data).unwrap();
                        asset_metadata.push(AssetMetadata {
                            id: asset.id,
                            search_tags: asset.search_tags,
                            build_deps: asset.build_deps,
                            load_deps: asset.load_deps,
                            instantiate_deps: asset.instantiate_deps,
                        });
                        println!(
                            "Import success {} read {} serialized {}",
                            source.path.to_string_lossy(),
                            scratch_buf.len(),
                            0
                        );
                    }
                    let source_metadata = SourceMetadata {
                        version: SOURCEMETADATA_VERSION,
                        source_hash: source_hash,
                        importer_version: format.version(),
                        importer_options: options,
                        importer_state: state,
                        assets: asset_metadata,
                    };
                    let serialized_metadata = ron::ser::to_string_pretty(
                        &source_metadata,
                        ron::ser::PrettyConfig::default(),
                    )
                    .unwrap();
                    let meta_path = to_meta_path(&source.path);
                    // println!("Meta file {}: {}", meta_path.to_string_lossy(), serialized_metadata);
                    let mut meta_file = fs::File::create(meta_path)?;
                    meta_file.write(serialized_metadata.as_bytes())?;
                    Ok(())
                }
                Err(err) => {
                    println!(
                        "Import error {} path {}",
                        err,
                        source.path.to_string_lossy()
                    );
                    Ok(())
                }
            }
        }
        None => Ok(()),
    }
}

fn process_pair_cases(
    pair: &mut HashedAssetFilePair,
    scratch_buf: &mut Vec<u8>,
) -> Result<(), FileError> {
    match pair {
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
            )?;
        }
        HashedAssetFilePair {
            meta: Some((_meta, Some(_hash))),
            source: Some((_source, None)),
        } => {
            // println!("directory {}", source.path.to_string_lossy());
        }
        HashedAssetFilePair {
            meta: Some((_meta, None)),
            source: Some((source, None)),
        } => {
            println!(
                "directory with meta directory?? {}",
                source.path.to_string_lossy()
            );
        }
        HashedAssetFilePair {
            meta: Some((_meta, None)),
            source: Some((source, Some(_hash))),
        } => {
            println!(
                "source file with meta directory?? {}",
                source.path.to_string_lossy()
            );
        }
        HashedAssetFilePair {
            meta: None,
            source: Some((source, Some(hash))),
        } => {
            import_pair(source, *hash, None, None, scratch_buf)?;
            // println!("source file with no meta {}", source.path.to_string_lossy());
        }
        HashedAssetFilePair {
            meta: None,
            source: Some((_source, None)),
        } => {
            // println!("directory with no meta {}", source.path.to_string_lossy());
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
        }
        _ => println!("Unknown case for {:?}", pair),
    }
    Ok(())
}

impl AssetHub {
    pub fn new(tracker: Arc<FileTracker>) -> Result<AssetHub, FileError> {
        let (tx, rx) = channel::unbounded();
        tracker.register_listener(tx);
        Ok(AssetHub {
            tracker: tracker.clone(),
            rx: rx,
            tables: AssetHubTables {
                source_pairs: tracker.db.create_db(Some("source_pairs"), lmdb::DatabaseFlags::default())?,
                import_artifacts: tracker.db.create_db(Some("import_artifacts"), lmdb::DatabaseFlags::default())?,
                asset_to_import_artifact: tracker.db.create_db(Some("asset_to_import_artifact"), lmdb::DatabaseFlags::default())?,
            }
        })
    }

    fn handle_update(&self) -> Result<(), FileError> {
        let mut deleted = Vec::new();
        let mut processing_results: Vec<(HashedAssetFilePair, Result<(), FileError>)> = Vec::new();
        {
            let txn = self.tracker.get_ro_txn()?;
            let dirty_files = self.tracker.read_dirty_files(&txn)?;
            if !dirty_files.is_empty() {
                let mut source_meta_pairs: HashMap<PathBuf, AssetFilePair> = HashMap::new();
                for state in dirty_files.into_iter() {
                    let mut is_meta = false;
                    match state.path.extension() {
                        Some(ext) => match ext.to_str().unwrap() {
                            "meta" => {
                                is_meta = true;
                            }
                            _ => {}
                        },
                        None => {}
                    }
                    let base_path;
                    if is_meta {
                        base_path = state.path.with_file_name(state.path.file_stem().unwrap());
                    } else {
                        base_path = state.path.clone();
                    }
                    let mut pair = source_meta_pairs.entry(base_path).or_insert(AssetFilePair {
                        source: Option::None,
                        meta: Option::None,
                    });
                    if state.state == data_capnp::FileState::Deleted {
                        deleted.push(state);
                    } else {
                        if is_meta {
                            pair.meta = Some(state.clone());
                        } else {
                            pair.source = Some(state.clone());
                        }
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
                let hashed_files = hash_files(Vec::from_iter(
                    source_meta_pairs
                        .values()
                        .filter(|pair| pair.source.is_some() || pair.meta.is_some()),
                ));
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
                        .filter(|f| f.is_err() == false)
                        .map(|e| e.unwrap()),
                );

                use std::cell::RefCell;
                thread_local!(static SCRATCH_STORE: RefCell<Option<Vec<u8>>> = RefCell::new(None));
                processing_results.extend(Vec::from_par_iter(hashed_files.par_iter().map(|p| {
                    SCRATCH_STORE.with(|cell| {
                        let mut local_store = cell.borrow_mut();
                        if local_store.is_none() {
                            *local_store = Some(Vec::new());
                        }
                        let mut processed_pair = p.clone();
                        let result =
                            process_pair_cases(&mut processed_pair, local_store.as_mut().unwrap());
                        (processed_pair, result)
                    })
                })));
                // let successful_hashes = filter_and_unwrap_hashes(hashed_files);
                println!("Hashed {}", hashed_files.len());
            }
        }

        {
            let mut txn = self.tracker.get_rw_txn()?;
            for file in deleted.iter() {
                println!("deleted: {}", file.path.to_string_lossy());
                self.tracker.delete_dirty_file_state(&mut txn, &file.path)?;
            }
            for result in processing_results {
                let pair = result.0;
                match result.1 {
                    Ok(_) => {
                        let mut skip_ack_dirty = false;
                        {
                            let check_file_state =
                                |s: &Option<&FileState>| -> Result<bool, FileError> {
                                    match s {
                                        Some(source) => {
                                            let source_file_state =
                                                self.tracker.get_file_state(&txn, &source.path)?;
                                            Ok(source_file_state.map_or(false, |s| s != **source))
                                        }
                                        None => Ok(false),
                                    }
                                };
                            skip_ack_dirty |=
                                check_file_state(&pair.source.as_ref().map(|f| &f.0))?;
                            skip_ack_dirty |= check_file_state(&pair.meta.as_ref().map(|f| &f.0))?;
                        }
                        if skip_ack_dirty == false {
                            if pair.source.is_some() {
                                let source = pair.source.unwrap().0;
                                self.tracker
                                    .delete_dirty_file_state(&mut txn, &source.path)?;
                            }
                            if pair.meta.is_some() {
                                let meta = pair.meta.unwrap().0;
                                self.tracker.delete_dirty_file_state(&mut txn, &meta.path)?;
                            }
                        }
                    }
                    Err(e) => println!("Error processing pair: {}", e),
                }
            }
            txn.commit()?;
        }

        Ok(())
    }

    pub fn run(&self) -> Result<(), FileError> {
        loop {
            match self.rx.recv() {
                Some(_evt) => {
                    self.handle_update()?;
                }
                None => {
                    return Ok(());
                }
            }
        }
    }
}
