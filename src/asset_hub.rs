use asset_import::{format_from_ext, AnySerialize};
use bincode;
use crossbeam_channel::{self as channel, Receiver, Sender};
use data_capnp::{self, file_hash_info};
use file_error::FileError;
use file_tracker::{FileState, FileTracker, FileTrackerEvent};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::{
    ffi::OsStr, fs, hash::Hasher, io::BufRead, io::Read, iter::FromIterator, path::PathBuf,
    sync::Arc,
};
use watcher::file_metadata;

pub struct AssetHub {
    tracker: Arc<FileTracker>,
    rx: Receiver<FileTrackerEvent>,
}

pub struct AssetHubTables {
    file_hashes: lmdb::Database,
    import_artifacts: lmdb::Database,
}

// Only files get Some(hash)
#[derive(Debug)]
struct HashedAssetFilePair {
    source: Option<(FileState, Option<u64>)>,
    meta: Option<(FileState, Option<u64>)>,
}
struct AssetFilePair {
    source: Option<FileState>,
    meta: Option<FileState>,
}

pub trait Importer {
    type SourceAssetID: Serialize;
    type Options: Serialize + Default;
    fn version(&self) -> u32;
    fn new_metadata(&self, source_hash: u64) -> SourceMetadata<Self> where Self : Sized {
        SourceMetadata {
            version: 1,
            source_hash: source_hash,
            importer_version: self.version(),
            importer_options: Self::Options::default(),
            assets: Vec::new(),
        }
    }
}

#[derive(Serialize)]
struct SearchTag {
    key: String,
    value: Option<String>,
}

#[derive(Serialize)]
struct AssetUUID {
    a: u64,
    b: u64,
}

#[derive(Serialize)]
struct AssetMetadata<T: Serialize> {
    id: AssetUUID,
    source_id: T,
    search_tags: Vec<SearchTag>,
}

#[derive(Serialize)]
struct SourceMetadata<T: Importer> {
    /// Metadata struct version
    version: u32,
    /// Hash of the source file this metadata was generated from
    source_hash: u64,
    importer_version: u32,
    importer_options: <T as Importer>::Options,
    assets: Vec<AssetMetadata<<T as Importer>::SourceAssetID>>,
}

impl<T: Importer> SourceMetadata<T> {
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

fn process_pair(pair: &HashedAssetFilePair, scratch_buf: &mut Vec<u8>) -> Result<(), FileError> {
    match pair {
        HashedAssetFilePair {
            meta: Some((meta, Some(meta_hash))),
            source: Some((source, Some(source_hash))),
        } => {

            // println!("full pair {}", source.path.to_string_lossy());
        }
        HashedAssetFilePair {
            meta: Some((meta, Some(hash))),
            source: Some((source, None)),
        } => {
            // println!("directory {}", source.path.to_string_lossy());
        }
        HashedAssetFilePair {
            meta: Some((meta, None)),
            source: Some((source, None)),
        } => {
            println!(
                "directory with meta directory?? {}",
                source.path.to_string_lossy()
            );
        }
        HashedAssetFilePair {
            meta: Some((meta, None)),
            source: Some((source, Some(hash))),
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
            let format = format_from_ext(source.path.extension().unwrap().to_str().unwrap());
            match format {
                Some(format) => {
                    let mut f = fs::File::open(&source.path)?;
                    scratch_buf.clear();
                    f.read_to_end(scratch_buf)?;
                    let buf_len = scratch_buf.len();
                    let imported =
                        format.import_boxed(scratch_buf.to_vec(), format.default_metadata());
                    scratch_buf.clear();
                    match imported {
                        Ok(asset) => {
                            let ser = bincode::serialize_into(scratch_buf, &asset).unwrap();
                            println!(
                                "Import success {} read {} serialized {}",
                                source.path.to_string_lossy(),
                                buf_len,
                                0
                            );
                        }
                        Err(err) => {
                            println!("Import error {}", err);
                        }
                    }
                }
                None => {}
            }
            // println!("source file with no meta {}", source.path.to_string_lossy());
        }
        HashedAssetFilePair {
            meta: None,
            source: Some((source, None)),
        } => {
            println!("directory with no meta {}", source.path.to_string_lossy());
        }
        HashedAssetFilePair {
            meta: Some((meta, Some(hash))),
            source: None,
        } => {
            println!(
                "meta file without source file {}",
                meta.path.to_string_lossy()
            );
        }
        _ => println!("Unknown case for {:?}", pair),
    }
    Ok(())
}

impl AssetHub {
    pub fn new(tracker: Arc<FileTracker>) -> AssetHub {
        let (tx, rx) = channel::unbounded();
        tracker.register_listener(tx);
        AssetHub {
            tracker: tracker.clone(),
            rx: rx,
        }
    }

    fn handle_update(&self) -> Result<(), FileError> {
        let txn = self.tracker.get_ro_txn()?;
        let dirty_files = self.tracker.read_dirty_files(&txn)?;
        let mut deleted = Vec::new();
        if !dirty_files.is_empty() {
            let mut source_meta_pairs: HashMap<PathBuf, AssetFilePair> = HashMap::new();
            for state in dirty_files.into_iter() {
                if state.state == data_capnp::FileState::Deleted {
                    deleted.push(state);
                    continue;
                }
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
                if is_meta {
                    pair.meta = Some(state.clone());
                } else {
                    pair.source = Some(state.clone());
                }
            }
            for (path, pair) in source_meta_pairs.iter_mut() {
                let other_path;
                let other_is_meta;
                match pair {
                    AssetFilePair {
                        meta: Some(_),
                        source: None,
                    } => {
                        other_is_meta = false;
                        other_path = path.clone();
                    }
                    AssetFilePair {
                        meta: None,
                        source: Some(_),
                    } => {
                        other_is_meta = true;
                        other_path = path.with_file_name(OsStr::new(
                            &(path.file_name().unwrap().to_str().unwrap().to_owned() + ".meta"),
                        ));
                    }
                    _ => continue,
                }
                let other_state = self.tracker.get_file_state(&txn, &other_path)?;
                if other_is_meta {
                    pair.meta = other_state;
                } else {
                    pair.source = other_state;
                }
            }
            let pairs_vec = Vec::from_iter(source_meta_pairs.values());
            let mut hashed_files = hash_files(pairs_vec);
            for result in &hashed_files {
                match result {
                    Err(err) => {
                        println!(
                            "Error hashing {}",
                            // state.path.to_string_lossy(),
                            err
                        );
                    }
                    Ok(pair) => {}
                }
            }
            let len = hashed_files.len();
            let hashed_files = Vec::from_iter(
                hashed_files
                    .into_iter()
                    .filter(|f| f.is_err() == false)
                    .map(|e| e.unwrap()),
            );

            use std::cell::RefCell;
            thread_local!(static STORE: RefCell<Option<Vec<u8>>> = RefCell::new(None));
            let result = Vec::from_par_iter(hashed_files.par_iter().map(|p| {
                STORE.with(|cell| {
                    let mut local_store = cell.borrow_mut();
                    if local_store.is_none() {
                        *local_store = Some(Vec::new());
                    }
                    process_pair(p, local_store.as_mut().unwrap())
                })
            }));
            // let successful_hashes = filter_and_unwrap_hashes(hashed_files);
            println!("Hashed {}", hashed_files.len());
        }

        for d in deleted {
            println!("deleted: {}", d.path.to_string_lossy());
        }
        Ok(())
    }

    pub fn run(&self) -> Result<(), FileError> {
        loop {
            match self.rx.recv() {
                Some(evt) => {
                    self.handle_update()?;
                }
                None => {
                    return Ok(());
                }
            }
        }
    }
}
