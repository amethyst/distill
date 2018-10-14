extern crate capnp;
extern crate lmdb;
extern crate rayon;

use capnp_db::{self, CapnpCursor, DBTransaction, Environment, RoTransaction, RwTransaction};
use data_capnp::{self, dirty_file_info, source_file_info, FileType};
use file_error::FileError;
use lmdb::Cursor;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::hash::Hasher;
use std::io::BufRead;
use std::iter::FromIterator;
use std::ops::IndexMut;
use std::path::{Path, PathBuf};
use std::str;
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError};
use std::thread;
use std::time::Duration;
use watcher::{self, FileEvent, FileMetadata};

fn db_file_type(t: fs::FileType) -> FileType {
    if t.is_dir() {
        FileType::Directory
    } else if t.is_symlink() {
        FileType::Symlink
    } else {
        FileType::File
    }
}

fn build_dirty_file_info(
    state: data_capnp::FileState,
    source_info: source_file_info::Reader,
) -> capnp::message::Builder<capnp::message::HeapAllocator> {
    let mut value_builder = capnp::message::Builder::new_default();
    {
        let mut value = value_builder.init_root::<dirty_file_info::Builder>();
        value.set_state(state);
        value.set_source_info(source_info);
    }
    return value_builder;
}
fn build_source_info(
    metadata: watcher::FileMetadata,
) -> capnp::message::Builder<capnp::message::HeapAllocator> {
    let mut value_builder = capnp::message::Builder::new_default();
    {
        let mut value = value_builder.init_root::<source_file_info::Builder>();
        value.set_last_modified(metadata.last_modified);
        value.set_length(metadata.length);
        value.set_type(db_file_type(metadata.file_type));
    }
    return value_builder;
}

fn update_deleted_dirty_entry<K>(
    txn: &mut RwTransaction,
    tables: &FileTrackerTables,
    key: &K,
) -> Result<(), FileError>
where
    K: AsRef<[u8]>,
{
    let dirty_value = {
        txn.get(tables.dirty_files, key)?.map(|v| {
            let info = v.get_root::<dirty_file_info::Reader>().unwrap();
            build_dirty_file_info(
                data_capnp::FileState::Deleted,
                info.get_source_info().unwrap(),
            )
        })
    };
    if dirty_value.is_some() {
        txn.put(tables.dirty_files, key, &dirty_value.unwrap())?;
    }
    Ok(())
}

#[derive(Clone)]
pub struct FileTrackerTables {
    source_files: lmdb::Database,
    dirty_files: lmdb::Database,
}
pub struct FileTracker {
    pub db: Environment,
    pub tables: FileTrackerTables,
}
pub struct FileState {
    pub path: PathBuf,
    pub state: data_capnp::FileState,
}
struct ScanContext {
    path: PathBuf,
    files: HashMap<PathBuf, FileMetadata>,
}

fn handle_file_event(
    txn: &mut RwTransaction,
    tables: &FileTrackerTables,
    evt: watcher::FileEvent,
    scan_stack: &mut Vec<ScanContext>,
) -> Result<(), FileError> {
    match evt {
        FileEvent::Updated(path, metadata) => {
            let path_str = path.to_string_lossy();
            let key = path_str.as_bytes();
            let mut changed = true;
            {
                let maybe_msg = txn.get(tables.source_files, &key)?;
                match maybe_msg {
                    Some(msg) => {
                        let info = msg.get_root::<source_file_info::Reader>()?;
                        if info.get_length() == metadata.length
                            && info.get_last_modified() == metadata.last_modified
                            && info.get_type()? == db_file_type(metadata.file_type)
                        {
                            changed = false;
                        } else {
                            println!("CHANGED {} metadata {:?}", path_str, metadata);
                        }
                    }
                    None => {}
                }
            }
            if !scan_stack.is_empty() {
                let head_idx = scan_stack.len() - 1;
                let scan_ctx = scan_stack.index_mut(head_idx);
                scan_ctx.files.insert(path.clone(), metadata.clone());
            }
            if changed {
                let value = build_source_info(metadata);
                let dirty_value = build_dirty_file_info(
                    data_capnp::FileState::Exists,
                    value.get_root_as_reader::<source_file_info::Reader>()?,
                );
                txn.put(tables.source_files, &key, &value)?;
                txn.put(tables.dirty_files, &key, &dirty_value)?;
            }
        }
        FileEvent::Renamed(src, dst, metadata) => {
            if !scan_stack.is_empty() {
                let head_idx = scan_stack.len() - 1;
                let scan_ctx = scan_stack.index_mut(head_idx);
                scan_ctx.files.insert(dst.clone(), metadata.clone());
                scan_ctx.files.remove(&src);
            }
            let src_str = src.to_string_lossy();
            let src_key = src_str.as_bytes();
            let dst_str = dst.to_string_lossy();
            let dst_key = dst_str.as_bytes();
            println!("rename {} to {} metadata {:?}", src_str, dst_str, metadata);
            let value = build_source_info(metadata);
            txn.delete(tables.source_files, &src_key)?;
            txn.put(tables.source_files, &dst_key, &value)?;
            let dirty_value = {
                txn.get_as_bytes(tables.dirty_files, &src_key)?
                    .map(|x| x.to_owned())
            };
            if dirty_value.is_some() {
                txn.delete(tables.dirty_files, &src_key)?;
                let dirty_value = dirty_value.unwrap();
                txn.put_bytes(tables.dirty_files, &dst_key, &dirty_value)?;
            }
        }
        FileEvent::Removed(path) => {
            if !scan_stack.is_empty() {
                let head_idx = scan_stack.len() - 1;
                let scan_ctx = scan_stack.index_mut(head_idx);
                scan_ctx.files.remove(&path);
            }
            let path_str = path.to_string_lossy();
            let key = path_str.as_bytes();
            println!("removed {}", path_str);
            txn.delete(tables.source_files, &key)?;
            update_deleted_dirty_entry(txn, &tables, &key)?;
        }
        FileEvent::FileError(err) => {
            println!("file event error: {}", err);
            return Err(err);
        }
        FileEvent::ScanStart(path) => {
            println!("scan start: {}", path.to_string_lossy());
            scan_stack.push(ScanContext {
                path: path,
                files: HashMap::new(),
            });
        }
        FileEvent::ScanEnd(path, watched_dirs) => {
            // When we finish a scan, we know which files exist in the subdirectories.
            // This means we can scan our DB for files we've tracked and delete removed files from DB
            let scan_ctx = scan_stack.pop().unwrap();
            let mut db_file_set = HashSet::new();
            {
                let path_str = path.to_string_lossy();
                let key = path_str.as_bytes();
                let path_string = scan_ctx.path.to_string_lossy().into_owned();
                let mut cursor = txn.open_ro_cursor(tables.source_files)?;
                for (key, _) in cursor.capnp_iter_from(&key) {
                    let key = str::from_utf8(key).expect("Encoded key was invalid utf8");
                    if !key.starts_with(&path_string) {
                        break;
                    }
                    db_file_set.insert(PathBuf::from(key));
                }
            }
            let scan_ctx_set = HashSet::from_iter(scan_ctx.files.keys().map(|p| p.clone()));
            let to_remove = db_file_set.difference(&scan_ctx_set);
            for p in to_remove {
                let p_str = p.to_string_lossy();
                let p_key = p_str.as_bytes();
                txn.delete(tables.source_files, &p_key)?;
                update_deleted_dirty_entry(txn, &tables, &p_key)?;
            }
            println!(
                "Scanned and compared {} + {}, deleted {}",
                scan_ctx_set.len(),
                db_file_set.len(),
                db_file_set.difference(&scan_ctx_set).count()
            );
            // If this is the top-level scan, we have a final set of watched directories,
            // so we can delete any files that are not in any watched directories from the DB.
            if scan_stack.len() == 0 {
                let mut to_delete = Vec::new();
                {
                    let mut cursor = txn.open_ro_cursor(tables.source_files)?;
                    let dirs_as_strings = Vec::from_iter(
                        watched_dirs
                            .into_iter()
                            .map(|f| f.to_string_lossy().into_owned()),
                    );
                    for (key_bytes, _) in cursor.iter_start() {
                        let key = str::from_utf8(key_bytes).expect("Encoded key was invalid utf8");
                        if !dirs_as_strings.iter().any(|dir| key.starts_with(dir)) {
                            println!("TRASH! {}", key);
                            to_delete.push(key.clone());
                        }
                    }
                }
                for key in to_delete {
                    txn.delete(tables.source_files, &key)?;
                    update_deleted_dirty_entry(txn, &tables, &key)?;
                }
            }
            println!("scan end: {}", path.to_string_lossy());
        }
    }
    Ok(())
}
fn read_file_events(
    is_running: &mut bool,
    txn: &mut RwTransaction,
    tables: &FileTrackerTables,
    rx: &Receiver<FileEvent>,
    scan_stack: &mut Vec<ScanContext>,
) -> Result<(), FileError> {
    while *is_running {
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(evt) => {
                // println!("evt: {:?}", evt);
                handle_file_event(txn, tables, evt, scan_stack)?;
            }
            Err(err) => match err {
                RecvTimeoutError::Disconnected => {
                    println!("asset error: {}", err);
                    *is_running = false;
                    break;
                }
                RecvTimeoutError::Timeout => {
                    break;
                }
            },
        }
    }
    Ok(())
}

impl FileTracker {
    pub fn new(db_dir: &Path) -> Result<FileTracker, FileError> {
        let _ = fs::create_dir(db_dir);
        let asset_db = Environment::new(db_dir)?;
        Ok(FileTracker {
            tables: FileTrackerTables {
                source_files: asset_db
                    .create_db(Some("source_files"), lmdb::DatabaseFlags::default())?,
                dirty_files: asset_db
                    .create_db(Some("dirty_files"), lmdb::DatabaseFlags::default())?,
            },
            db: asset_db,
        })
    }

    pub fn read_all_files<'a>(
        &self,
        iter_txn: &RoTransaction,
    ) -> Result<Vec<FileState>, FileError> {
        let mut file_states = Vec::new();
        {
            let mut cursor = iter_txn.open_ro_cursor(self.tables.source_files)?;
            for (key_result, value_result) in cursor.capnp_iter_start() {
                let key = str::from_utf8(key_result).expect("Failed to parse key as utf8");
                let value_result = value_result?;
                let info = value_result.get_root::<dirty_file_info::Reader>()?;
                file_states.push(FileState {
                    path: PathBuf::from(key),
                    state: info.get_state()?,
                });
            }
        }
        Ok(file_states)
    }

    pub fn get_txn<'a>(&'a self) -> Result<RoTransaction<'a>, FileError> {
        Ok(self.db.ro_txn()?)
    }

    pub fn run(&self, to_watch: Vec<&str>) -> Result<(), FileError> {
        let (tx, rx) = channel();
        let mut watcher = watcher::DirWatcher::new(to_watch, tx)?;

        let stop_handle = watcher.stop_handle();
        let handle = thread::spawn(move || {
            watcher.run();
        });
        let mut is_running = true;
        while is_running {
            {
                let mut txn = self.db.rw_txn()?;
                let mut scan_stack = Vec::new();
                read_file_events(
                    &mut is_running,
                    &mut txn,
                    &self.tables,
                    &rx,
                    &mut scan_stack,
                )?;
                assert!(scan_stack.len() == 0);
                if txn.dirty {
                    txn.commit()?;
                    println!("Commit");
                }
            }

            {
                let mut to_hash = Vec::new();
                let mut iter_txn = self.db.rw_txn()?;
                {
                    let mut cursor = iter_txn.open_ro_cursor(self.tables.dirty_files)?;
                    for (key_result, value_result) in cursor.capnp_iter_start() {
                        let key = str::from_utf8(key_result).expect("Failed to parse key as utf8");
                        let value_result = value_result?;
                        to_hash.push(key.clone())
                    }
                }

                if !to_hash.is_empty() {
                    let hashed_files = Vec::from_par_iter(
                        to_hash
                            .par_iter()
                            .filter(|p| {
                                fs::metadata(p)
                                    .and_then(|m| Ok(m.is_file())) // only hash files
                                    .unwrap_or(false)
                            }).map(|p| {
                                return (
                                    p,
                                    fs::OpenOptions::new().read(true).open(p).and_then(|f| {
                                        let mut hasher =
                                            ::std::collections::hash_map::DefaultHasher::new();
                                        let mut reader =
                                            ::std::io::BufReader::with_capacity(64000, f);
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
                                        Ok(hasher.finish())
                                    }),
                                );
                            }),
                    );

                    for (path, result) in &hashed_files {
                        match result {
                            Err(err) => {
                                println!("Error hashing {}:{}", path, err);
                            }
                            Ok(hash) => {
                                println!("path {} hash {}", path, hash);
                            }
                        }
                    }
                    println!("Hashed {}", hashed_files.len());
                    iter_txn.clear_db(self.tables.dirty_files)?;
                    iter_txn.commit()?;
                    // {
                    //     let mut cursor = iter_txn.open_ro_cursor(self.tables.source_files)?;
                    //     for (key_result, value_result) in cursor.capnp_iter_start() {
                    //         let key_result = key_result?;
                    //         let value_result = value_result?;
                    //         let key =
                    //             key_result.get_root::<data_capnp::source_file_info_key::Reader>()?;
                    //         let value =
                    //             value_result.get_root::<data_capnp::source_file_info::Reader>()?;
                    //         println!(
                    //             "length {} last_modified {} path {}",
                    //             value.get_length(),
                    //             value.get_last_modified(),
                    //             key.get_path()?
                    //         );
                    //     }
                    // }
                    println!("End loop");
                }
            }
        }
        stop_handle.stop();
        handle.join();
        Ok(())
    }
}
