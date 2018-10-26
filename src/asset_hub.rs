use crossbeam_channel::{self as channel, Receiver, Sender};
use data_capnp::{self, file_hash_info};
use file_error::FileError;
use file_tracker::{FileState, FileTracker, FileTrackerEvent};
use rayon::prelude::*;
use std::collections::HashMap;
use std::{
    ffi::OsStr, fs, hash::Hasher, io::BufRead, iter::FromIterator, path::PathBuf, sync::Arc,
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

struct HashedAssetFilePair {
    source: Option<(FileState, u64)>,
    meta: Option<(FileState, u64)>,
}
struct AssetFilePair {
    source: Option<FileState>,
    meta: Option<FileState>,
}

fn filter_and_unwrap_hashes(
    mut hashes: Vec<(FileState, Result<Option<u64>, FileError>)>,
) -> Vec<(FileState, u64)> {
    let len = hashes.len();
    let mut filtered = Vec::with_capacity(len);
    for (state, hash) in hashes.drain(0..len) {
        match hash {
            Err(_) => {}
            Ok(maybe_h) => match maybe_h {
                None => {}
                Some(hash) => filtered.push((state, hash)),
            },
        }
    }
    filtered
}
fn hash_file(state: &FileState) -> Result<(FileState, Option<u64>), FileError> {
    let metadata = match fs::metadata(&state.path) {
        Err(e) => return Err(FileError::IO(e)),
        Ok(m) => {
            if !m.is_file() {
                return Ok((state.clone(), Option::None));
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
fn hash_files(files: Vec<FileState>) -> Vec<(FileState, Result<Option<u64>, FileError>)> {
    Vec::from_par_iter(
        files
            .par_iter()
            .map(|s| -> (FileState, Result<Option<u64>, FileError>) {
                match hash_file(s) {
                    Ok((s, maybe_hash)) => return (s.clone(), Ok(maybe_hash)),
                    Err(e) => return (s.clone(), Err(e)),
                }
            }),
    )
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

    pub fn run(&self) -> Result<(), FileError> {
        loop {
            match self.rx.recv() {
                Some(evt) => {
                    let txn = self.tracker.get_ro_txn()?;
                    let dirty_files = self.tracker.read_dirty_files(&txn)?;
                    if !dirty_files.is_empty() {
                        let mut source_meta_pairs: HashMap<
                            PathBuf,
                            AssetFilePair,
                        > = HashMap::new();
                        for state in dirty_files.iter() {
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
                                base_path =
                                    state.path.with_file_name(state.path.file_stem().unwrap());
                            } else {
                                base_path = state.path.clone();
                            }
                            let mut pair =
                                source_meta_pairs.entry(base_path).or_insert(AssetFilePair {
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
                                    meta: Some(m),
                                    source: None,
                                } => {
                                    other_is_meta = false;
                                    other_path = path.clone();
                                }
                                AssetFilePair {
                                    meta: None,
                                    source: Some(s),
                                } => {
                                    other_is_meta = true;
                                    other_path = path.with_file_name(OsStr::new(
                                        &(path.file_name().unwrap().to_str().unwrap().to_owned()
                                            + ".meta"),
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
                            println!(
                                "path {} meta {} source {}",
                                path.to_string_lossy(),
                                pair.meta.is_some(),
                                pair.source.is_some()
                            );
                        }
                        let hashed_files = hash_files(dirty_files);
                        for result in &hashed_files {
                            match result {
                                (state, Err(err)) => {
                                    println!(
                                        "Error hashing {}:{}",
                                        state.path.to_string_lossy(),
                                        err
                                    );
                                }
                                (state, Ok(hash)) => {}
                            }
                        }
                        let successful_hashes = filter_and_unwrap_hashes(hashed_files);
                        println!("Hashed {}", successful_hashes.len());
                    }
                }
                None => {
                    return Ok(());
                }
            }
        }
    }
}
