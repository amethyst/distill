use crate::capnp_db::{
    CapnpCursor, DBTransaction, Environment, MessageReader, RoTransaction, RwTransaction,
};
use crate::error::{Error, Result};
use crate::utils;
use crate::watcher::{self, FileEvent, FileMetadata};
use atelier_schema::data::{self, dirty_file_info, rename_file_event, source_file_info, FileType};
use crossbeam_channel::{self as channel, select, Receiver, Sender};
use lmdb::Cursor;
use log::{debug, error, info};
use std::{
    cmp::PartialEq,
    collections::{HashMap, HashSet},
    fs, io,
    iter::FromIterator,
    ops::IndexMut,
    path::PathBuf,
    str,
    sync::atomic::{AtomicBool, Ordering},
    sync::Arc,
    thread,
    time::Duration,
};

#[derive(Clone)]
struct FileTrackerTables {
    /// Contains Path -> SourceFileInfo
    source_files: lmdb::Database,
    /// Contains Path -> DirtyFileInfo
    dirty_files: lmdb::Database,
    /// Contains SequenceNum -> DirtyFileInfo
    rename_file_events: lmdb::Database,
}
#[derive(Copy, Clone, Debug)]
pub enum FileTrackerEvent {
    Start,
    Update,
}
pub struct FileTracker {
    db: Arc<Environment>,
    tables: FileTrackerTables,
    listener_rx: Receiver<Sender<FileTrackerEvent>>,
    listener_tx: Sender<Sender<FileTrackerEvent>>,
    is_running: AtomicBool,
    watch_dirs: Vec<PathBuf>,
}
#[derive(Clone, Debug)]
pub struct FileState {
    pub path: PathBuf,
    pub state: data::FileState,
    pub last_modified: u64,
    pub length: u64,
}

impl PartialEq for FileState {
    fn eq(&self, other: &FileState) -> bool {
        self.path == other.path
            && self.state == other.state
            && self.last_modified == other.last_modified
            && self.length == other.length
    }
}
#[derive(Clone, Debug)]
pub struct RenameFileEvent {
    pub src: PathBuf,
    pub dst: PathBuf,
}

struct ScanContext {
    path: PathBuf,
    files: HashMap<PathBuf, FileMetadata>,
}

fn db_file_type(t: fs::FileType) -> FileType {
    if t.is_dir() {
        FileType::Directory
    } else if t.is_symlink() {
        FileType::Symlink
    } else {
        FileType::File
    }
}

fn add_rename_event(
    tables: &FileTrackerTables,
    txn: &mut RwTransaction<'_>,
    src: &[u8],
    dst: &[u8],
) -> Result<()> {
    let mut last_seq: u64 = 0;
    let last_element = txn
        .open_ro_cursor(tables.rename_file_events)?
        .capnp_iter_start()
        .last();
    if let Some((key, _)) = last_element {
        last_seq = u64::from_le_bytes(utils::make_array(key));
    }
    let mut value_builder = capnp::message::Builder::new_default();
    {
        let mut value = value_builder.init_root::<rename_file_event::Builder<'_>>();
        value.set_src(src);
        value.set_dst(dst);
    }
    last_seq += 1;
    txn.put(
        tables.rename_file_events,
        &last_seq.to_le_bytes(),
        &value_builder,
    )?;
    Ok(())
}

fn build_dirty_file_info(
    state: data::FileState,
    source_info: source_file_info::Reader<'_>,
) -> capnp::message::Builder<capnp::message::HeapAllocator> {
    let mut value_builder = capnp::message::Builder::new_default();
    {
        let mut value = value_builder.init_root::<dirty_file_info::Builder<'_>>();
        value.set_state(state);
        value
            .set_source_info(source_info)
            .expect("failed to set source info");
    }
    value_builder
}
fn build_source_info(
    metadata: &watcher::FileMetadata,
) -> capnp::message::Builder<capnp::message::HeapAllocator> {
    let mut value_builder = capnp::message::Builder::new_default();
    {
        let mut value = value_builder.init_root::<source_file_info::Builder<'_>>();
        value.set_last_modified(metadata.last_modified);
        value.set_length(metadata.length);
        value.set_type(db_file_type(metadata.file_type));
    }
    value_builder
}

fn update_deleted_dirty_entry<K>(
    txn: &mut RwTransaction<'_>,
    tables: &FileTrackerTables,
    key: &K,
) -> Result<()>
where
    K: AsRef<[u8]>,
{
    let dirty_value = {
        txn.get::<source_file_info::Owned, K>(tables.source_files, key)?
            .map(|v| {
                let info = v.get().expect("failed to get source_file_info");
                build_dirty_file_info(data::FileState::Deleted, info)
            })
    };
    if dirty_value.is_some() {
        txn.put(tables.dirty_files, key, &dirty_value.unwrap())?;
    }
    Ok(())
}

mod events {
    use super::*;
    fn handle_update(
        txn: &mut RwTransaction<'_>,
        tables: &FileTrackerTables,
        path: &PathBuf,
        metadata: &watcher::FileMetadata,
        scan_stack: &mut Vec<ScanContext>,
    ) -> Result<()> {
        let path_str = path.to_string_lossy();
        let key = path_str.as_bytes();
        let mut changed = true;
        {
            let maybe_msg: Option<MessageReader<'_, source_file_info::Owned>> =
                txn.get(tables.source_files, &key)?;
            if let Some(msg) = maybe_msg {
                let info = msg.get()?;
                if info.get_length() == metadata.length
                    && info.get_last_modified() == metadata.last_modified
                    && info.get_type()? == db_file_type(metadata.file_type)
                {
                    changed = false;
                } else {
                    debug!("CHANGED {} metadata {:?}", path_str, metadata);
                }
            }
        }
        if !scan_stack.is_empty() {
            let head_idx = scan_stack.len() - 1;
            let scan_ctx = scan_stack.index_mut(head_idx);
            scan_ctx.files.insert(path.clone(), metadata.clone());
        }
        if changed {
            let value = build_source_info(&metadata);
            let dirty_value = build_dirty_file_info(
                data::FileState::Exists,
                value.get_root_as_reader::<source_file_info::Reader<'_>>()?,
            );
            txn.put(tables.source_files, &key, &value)?;
            txn.put(tables.dirty_files, &key, &dirty_value)?;
        }
        Ok(())
    }

    pub(super) fn handle_file_event(
        txn: &mut RwTransaction<'_>,
        tables: &FileTrackerTables,
        evt: watcher::FileEvent,
        scan_stack: &mut Vec<ScanContext>,
    ) -> Result<Option<FileTrackerEvent>> {
        match evt {
            FileEvent::Updated(path, metadata) => {
                handle_update(txn, tables, &path, &metadata, scan_stack)?;
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
                debug!("rename {} to {} metadata {:?}", src_str, dst_str, metadata);
                let value = build_source_info(&metadata);
                txn.delete(tables.source_files, &src_key)?;
                txn.put(tables.source_files, &dst_key, &value)?;
                let dirty_value_new = build_dirty_file_info(
                    data::FileState::Exists,
                    value.get_root_as_reader::<source_file_info::Reader<'_>>()?,
                );
                let dirty_value_old = build_dirty_file_info(
                    data::FileState::Deleted,
                    value.get_root_as_reader::<source_file_info::Reader<'_>>()?,
                );
                txn.put(tables.dirty_files, &src_key, &dirty_value_old)?;
                txn.put(tables.dirty_files, &dst_key, &dirty_value_new)?;
                add_rename_event(tables, txn, &src_key, &dst_key)?;
            }
            FileEvent::Removed(path) => {
                if !scan_stack.is_empty() {
                    let head_idx = scan_stack.len() - 1;
                    let scan_ctx = scan_stack.index_mut(head_idx);
                    scan_ctx.files.remove(&path);
                }
                let path_str = path.to_string_lossy();
                let key = path_str.as_bytes();
                debug!("removed {}", path_str);
                update_deleted_dirty_entry(txn, &tables, &key)?;
                txn.delete(tables.source_files, &key)?;
            }
            FileEvent::FileError(err) => {
                debug!("file event error: {}", err);
                return Err(err);
            }
            FileEvent::ScanStart(path) => {
                debug!("scan start: {}", path.to_string_lossy());
                scan_stack.push(ScanContext {
                    path,
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
                let scan_ctx_set = HashSet::from_iter(scan_ctx.files.keys().cloned());
                let to_remove = db_file_set.difference(&scan_ctx_set);
                for p in to_remove {
                    let p_str = p.to_string_lossy();
                    let p_key = p_str.as_bytes();
                    update_deleted_dirty_entry(txn, &tables, &p_key)?;
                    txn.delete(tables.source_files, &p_key)?;
                }
                info!(
                    "Scanned and compared {} + {}, deleted {}",
                    scan_ctx_set.len(),
                    db_file_set.len(),
                    db_file_set.difference(&scan_ctx_set).count()
                );
                // If this is the top-level scan, we have a final set of watched directories,
                // so we can delete any files that are not in any watched directories from the DB.
                if scan_stack.is_empty() {
                    let mut to_delete = Vec::new();
                    {
                        let mut cursor = txn.open_ro_cursor(tables.source_files)?;
                        let dirs_as_strings = Vec::from_iter(
                            watched_dirs
                                .into_iter()
                                .map(|f| f.to_string_lossy().into_owned()),
                        );
                        for (key_bytes, _) in cursor.iter_start() {
                            let key =
                                str::from_utf8(key_bytes).expect("Encoded key was invalid utf8");
                            if !dirs_as_strings.iter().any(|dir| key.starts_with(dir)) {
                                to_delete.push(key);
                            }
                        }
                    }
                    for key in to_delete {
                        txn.delete(tables.source_files, &key)?;
                        update_deleted_dirty_entry(txn, &tables, &key)?;
                    }
                }
                debug!("scan end: {}", path.to_string_lossy());
                return Ok(Some(FileTrackerEvent::Start));
            }
        }
        Ok(None)
    }
}

impl FileTracker {
    pub fn new<'a, I, T>(db: Arc<Environment>, to_watch: I) -> FileTracker
    where
        I: IntoIterator<Item = &'a str, IntoIter = T>,
        T: Iterator<Item = &'a str>,
    {
        let watch_dirs: Vec<PathBuf> = to_watch
            .into_iter()
            .map(fs::canonicalize)
            // TODO(happens): Log which paths could not be added
            .filter_map(io::Result::ok)
            .collect();

        let source_files = db
            .create_db(Some("source_files"), lmdb::DatabaseFlags::default())
            .expect("Failed to create source_files table");

        let dirty_files = db
            .create_db(Some("dirty_files"), lmdb::DatabaseFlags::default())
            .expect("Failed to create dirty_files table");

        let rename_file_events = db
            .create_db(Some("rename_file_events"), lmdb::DatabaseFlags::INTEGER_KEY)
            .expect("Failed to create rename_file_events table");

        let (listener_tx, listener_rx) = channel::unbounded();

        FileTracker {
            is_running: AtomicBool::new(false),
            tables: FileTrackerTables {
                source_files,
                dirty_files,
                rename_file_events,
            },
            db,
            listener_rx,
            listener_tx,
            watch_dirs,
        }
    }

    pub fn get_watch_dirs(&self) -> impl Iterator<Item = &'_ PathBuf> {
        self.watch_dirs.iter()
    }

    pub fn get_rw_txn<'a>(&'a self) -> RwTransaction<'a> {
        self.db.rw_txn().expect("Failed to open rw txn")
    }

    pub fn get_ro_txn<'a>(&'a self) -> RoTransaction<'a> {
        self.db.ro_txn().expect("Failed to open ro txn")
    }

    pub fn read_rename_events<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        iter_txn: &'a V,
    ) -> Vec<(u64, RenameFileEvent)> {
        iter_txn
            .open_ro_cursor(self.tables.rename_file_events)
            .expect("Failed to open ro cursor for rename_file_events table")
            .capnp_iter_start()
            .filter_map(|(key, val)| {
                let val = val.ok()?;
                let evt = val.into_typed::<rename_file_event::Owned>();
                let evt = evt.get().ok()?;
                let seq_num = u64::from_le_bytes(utils::make_array(key));

                let src_raw = evt.get_src().ok()?;
                let src = PathBuf::from(str::from_utf8(src_raw).ok()?);

                let dst_raw = evt.get_dst().ok()?;
                let dst = PathBuf::from(str::from_utf8(dst_raw).ok()?);

                Some((seq_num, RenameFileEvent { src, dst }))
            })
            .collect()
    }

    pub fn clear_rename_events(&self, txn: &mut RwTransaction<'_>) -> Result<()> {
        txn.clear_db(self.tables.rename_file_events)?;
        Ok(())
    }

    pub fn add_dirty_file(&self, txn: &mut RwTransaction<'_>, path: &PathBuf) -> Result<()> {
        let metadata = match fs::metadata(path) {
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(Error::IO(e)),
            Ok(metadata) => Some(watcher::file_metadata(&metadata)),
        };
        let path_str = path.to_string_lossy();
        let key = path_str.as_bytes();
        if let Some(metadata) = metadata {
            let source_info = build_source_info(&metadata);
            let dirty_file_info = build_dirty_file_info(
                data::FileState::Exists,
                source_info.get_root_as_reader::<source_file_info::Reader<'_>>()?,
            );
            txn.put(self.tables.source_files, &key, &source_info)?;
            txn.put(self.tables.dirty_files, &key, &dirty_file_info)?;
        } else {
            update_deleted_dirty_entry(txn, &self.tables, &key)?;
        }
        Ok(())
    }

    pub fn read_dirty_files<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        iter_txn: &'a V,
    ) -> Vec<FileState> {
        // TODO(happens): If we have any errors while looping over a dirty file, we
        // should somehow be able to mark it for a retry. We can probably rely on
        // the fact that since we skip them, they will still be dirty on the next attempt.
        iter_txn
            .open_ro_cursor(self.tables.dirty_files)
            .expect("Failed to open ro cursor for dirty_files table")
            .capnp_iter_start()
            .filter_map(|(key, val)| {
                // TODO(happens): Do we want logging on why things are skipped here?
                // We could map to Result<_> first, and then log any errors in a filter_map
                // that just calls ok() afterwards.
                let key = str::from_utf8(key).ok()?;
                let val = val.ok()?;
                let info = val.get_root::<dirty_file_info::Reader<'_>>().ok()?;
                let source_info = info.get_source_info().ok()?;

                Some(FileState {
                    path: PathBuf::from(key),
                    state: info.get_state().ok()?,
                    last_modified: source_info.get_last_modified(),
                    length: source_info.get_length(),
                })
            })
            .collect()
    }

    pub fn read_all_files(&self, iter_txn: &RoTransaction<'_>) -> Vec<FileState> {
        iter_txn
            .open_ro_cursor(self.tables.source_files)
            .expect("Failed to open ro cursor for source_files table")
            .capnp_iter_start()
            .filter_map(|(key, val)| {
                let key = str::from_utf8(key).ok()?;
                let val = val.ok()?;
                let info = val.get_root::<source_file_info::Reader<'_>>().ok()?;

                Some(FileState {
                    path: PathBuf::from(key),
                    state: data::FileState::Exists,
                    last_modified: info.get_last_modified(),
                    length: info.get_length(),
                })
            })
            .collect()
    }

    pub fn delete_dirty_file_state<'a>(
        &self,
        txn: &'a mut RwTransaction<'_>,
        path: &PathBuf,
    ) -> bool {
        let key_str = path.to_string_lossy();
        let key = key_str.as_bytes();

        txn.delete(self.tables.dirty_files, &key)
            .expect("Failed to delete entry from dirty_files table")
    }

    pub fn get_dirty_file_state<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
        path: &PathBuf,
    ) -> Option<FileState> {
        let key_str = path.to_string_lossy();
        let key = key_str.as_bytes();

        txn.get::<dirty_file_info::Owned, &[u8]>(self.tables.dirty_files, &key)
            .expect("Failed to get entry from dirty_files table")
            .and_then(|value| {
                let value = value.get().ok()?;
                let info = value.get_source_info().ok()?;

                Some(FileState {
                    path: path.clone(),
                    state: data::FileState::Exists,
                    last_modified: info.get_last_modified(),
                    length: info.get_length(),
                })
            })
    }

    pub fn get_file_state<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
        path: &PathBuf,
    ) -> Option<FileState> {
        let key_str = path.to_string_lossy();
        let key = key_str.as_bytes();

        txn.get::<source_file_info::Owned, &[u8]>(self.tables.source_files, &key)
            .expect("Failed to get entry from source_files table")
            .and_then(|value| {
                let info = value.get().ok()?;

                Some(FileState {
                    path: path.clone(),
                    state: data::FileState::Exists,
                    last_modified: info.get_last_modified(),
                    length: info.get_length(),
                })
            })
    }

    pub fn register_listener(&self, sender: Sender<FileTrackerEvent>) {
        self.listener_tx.send(sender).unwrap();
    }

    pub fn stop(&self) {
        self.is_running.store(false, Ordering::Release);
    }

    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Acquire)
    }

    fn read_file_events(
        &self,
        is_running: &mut bool,
        rx: &Receiver<FileEvent>,
        scan_stack: &mut Vec<ScanContext>,
    ) -> Vec<FileTrackerEvent> {
        let mut txn = None;
        let mut output_evts = Vec::new();
        let timeout = Duration::from_millis(100);

        while *is_running {
            select! {
                recv(rx) -> evt => {
                    if let Ok(evt) = evt {
                        if txn.is_none() {
                            let new_txn = self.db
                                .rw_txn()
                                .expect("Failed to renew rw txn");

                            txn = Some(new_txn);
                        }

                        let txn = txn.as_mut().expect("Failed to get txn");

                        events::handle_file_event(txn, &self.tables, evt, scan_stack)
                            .ok()
                            .and_then(|evt| evt)
                            .map(|evt| output_evts.push(evt))
                            .unwrap_or(());
                    } else {
                        error!("Receive error");
                        *is_running = false;
                        break;
                    }
                }

                recv(channel::after(timeout)) -> _msg => {
                    break;
                }
            }
        }

        if let Some(txn) = txn {
            if txn.dirty {
                txn.commit().expect("Failed to commit txn");
                debug!("Commit read file events txn");

                output_evts.push(FileTrackerEvent::Update);
            }
        }

        output_evts
    }

    pub fn run(&self) -> Result<()> {
        if self
            .is_running
            .compare_and_swap(false, true, Ordering::AcqRel)
        {
            return Ok(());
        }
        let (tx, rx) = channel::unbounded();
        let mut watcher = watcher::DirWatcher::from_path_iter(
            self.watch_dirs.iter().map(|p| p.to_str().unwrap()),
            tx,
        )?;

        let stop_handle = watcher.stop_handle();
        let handle = thread::spawn(move || watcher.run());
        let mut listeners = vec![];
        while self.is_running.load(Ordering::Acquire) {
            let mut scan_stack = Vec::new();
            let mut is_running = true;
            let events = self.read_file_events(&mut is_running, &rx, &mut scan_stack);

            select! {
                recv(self.listener_rx) -> listener => {
                    if let Ok(listener) = listener {
                        listeners.push(listener);
                    }
                },
                default => {}
            }
            for event in events {
                for listener in listeners.iter() {
                    debug!("Sent to listener");
                    select! {
                        send(listener, event) -> _ => {}
                        default => {}
                    }
                }
            }
            assert!(scan_stack.is_empty());
            if !is_running {
                self.is_running.store(false, Ordering::Release);
            }
        }
        stop_handle.stop();
        handle.join().expect("thread panicked")?;
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use crate::capnp_db::Environment;
    use crate::file_tracker::{FileTracker, FileTrackerEvent};
    use crossbeam_channel::{self as channel, select, Receiver};
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::Arc,
        thread,
        time::Duration,
    };
    use tempfile;

    pub fn with_tracker<F>(f: F)
    where
        F: FnOnce(Arc<FileTracker>, Receiver<FileTrackerEvent>, &Path),
    {
        let db_dir = tempfile::tempdir().unwrap();
        let asset_dir = tempfile::tempdir().unwrap();
        {
            let _ = fs::create_dir(db_dir.path());
            let asset_paths = vec![asset_dir.path().to_str().unwrap()];
            let db = Arc::new(
                Environment::with_map_size(db_dir.path(), 1 << 21).expect(
                    format!(
                        "failed to create db environment {}",
                        db_dir.path().to_string_lossy()
                    )
                    .as_str(),
                ),
            );
            let tracker = Arc::new(FileTracker::new(db, asset_paths));
            let (tx, rx) = channel::unbounded();
            tracker.register_listener(tx);

            let handle = {
                let run_tracker = tracker.clone();
                thread::spawn(move || {
                    run_tracker
                        .clone()
                        .run()
                        .expect("error running file tracker");
                })
            };
            while !tracker.is_running() {
                thread::sleep(Duration::from_millis(1));
            }

            expect_event(&rx);

            f(tracker.clone(), rx, asset_dir.path());

            tracker.stop();

            handle.join().unwrap();
        }
    }

    fn expect_no_event(rx: &Receiver<FileTrackerEvent>) {
        select! {
            recv(rx) -> evt => {
                if let Ok(evt) = evt {
                    assert!(false, "Received unexpected event {:?}", evt);
                } else {
                    assert!(false, "Receive error when waiting for file event");
                }
            }
            recv(channel::after(Duration::from_millis(1000))) -> _ => {
                return;
            }
        };
        unreachable!();
    }

    fn expect_event(rx: &Receiver<FileTrackerEvent>) -> FileTrackerEvent {
        select! {
            recv(rx) -> evt => {
                if let Ok(evt) = evt {
                    return evt;
                } else {
                    assert!(false, "Receive error when waiting for file event");
                }
            }
            recv(channel::after(Duration::from_millis(1000))) -> _ => {
                    assert!(false, "Timed out waiting for file event");
            }
        };
        unreachable!();
    }

    fn expect_no_file_state(t: &FileTracker, asset_dir: &Path, name: &str) {
        let txn = t.get_ro_txn();
        let path = fs::canonicalize(&asset_dir)
            .unwrap_or_else(|_| panic!("failed to canonicalize {}", asset_dir.to_string_lossy()));
        let canonical_path = path.join(name);
        let maybe_state = t.get_file_state(&txn, &canonical_path);

        assert!(
            maybe_state.is_none(),
            "expected no file state for file {}",
            name
        );
    }

    fn expect_file_state(t: &FileTracker, asset_dir: &Path, name: &str) {
        let txn = t.get_ro_txn();
        let canonical_path =
            fs::canonicalize(asset_dir.join(name)).expect("failed to canonicalize");
        t.get_file_state(&txn, &canonical_path)
            .unwrap_or_else(|| panic!("expected file state for file {}", name));
    }

    pub fn add_test_dir(asset_dir: &Path, name: &str) -> PathBuf {
        let path = PathBuf::from(asset_dir).join(name);
        fs::create_dir(&path).expect("create dir");
        path
    }

    pub fn add_test_file(asset_dir: &Path, name: &str) {
        fs::copy(
            PathBuf::from("tests/file_tracker/").join(name),
            asset_dir.join(name),
        )
        .expect("copy test file");
    }

    pub fn delete_test_file(asset_dir: &Path, name: &str) {
        fs::remove_file(asset_dir.join(name)).expect("delete test file");
    }
    pub fn truncate_test_file(asset_dir: &Path, name: &str) {
        fs::File::create(asset_dir.join(name)).expect("truncate test file");
    }

    fn expect_dirty_file_state(t: &FileTracker, asset_dir: &Path, name: &str) {
        let txn = t.get_ro_txn();
        let path = fs::canonicalize(&asset_dir)
            .unwrap_or_else(|_| panic!("failed to canonicalize {}", asset_dir.to_string_lossy()));
        let canonical_path = path.join(name);
        t.get_dirty_file_state(&txn, &canonical_path)
            .unwrap_or_else(|| panic!("expected dirty file state for file {}", name));
    }

    fn clear_dirty_file_state(t: &FileTracker) {
        let mut txn = t.get_rw_txn();
        for f in t.read_dirty_files(&txn) {
            t.delete_dirty_file_state(&mut txn, &f.path);
        }
    }

    #[test]
    fn test_create_file() {
        with_tracker(|t, rx, asset_dir| {
            add_test_file(asset_dir, "test.txt");
            expect_event(&rx);
            expect_no_event(&rx);
            expect_file_state(&t, asset_dir, "test.txt");
            expect_dirty_file_state(&t, asset_dir, "test.txt");
        });
    }

    #[test]
    fn test_modify_file() {
        with_tracker(|t, rx, asset_dir| {
            add_test_file(asset_dir, "test.txt");
            expect_event(&rx);
            expect_file_state(&t, asset_dir, "test.txt");
            expect_dirty_file_state(&t, asset_dir, "test.txt");
            clear_dirty_file_state(&t);
            truncate_test_file(asset_dir, "test.txt");
            expect_event(&rx);
            expect_no_event(&rx);
            expect_file_state(&t, asset_dir, "test.txt");
            expect_dirty_file_state(&t, asset_dir, "test.txt");
        })
    }

    #[test]
    fn test_delete_file() {
        with_tracker(|t, rx, asset_dir| {
            add_test_file(asset_dir, "test.txt");
            expect_event(&rx);
            expect_file_state(&t, asset_dir, "test.txt");
            expect_dirty_file_state(&t, asset_dir, "test.txt");
            clear_dirty_file_state(&t);
            delete_test_file(asset_dir, "test.txt");
            expect_event(&rx);
            expect_no_event(&rx);
            expect_no_file_state(&t, asset_dir, "test.txt");
            expect_dirty_file_state(&t, asset_dir, "test.txt");
        })
    }

    #[test]
    fn test_create_dir() {
        with_tracker(|t, rx, asset_dir| {
            add_test_dir(asset_dir, "testdir");
            expect_event(&rx);
            expect_no_event(&rx);
            expect_file_state(&t, asset_dir, "testdir");
            expect_dirty_file_state(&t, asset_dir, "testdir");
        });
    }

    #[test]
    fn test_create_file_in_dir() {
        with_tracker(|t, rx, asset_dir| {
            let dir = add_test_dir(asset_dir, "testdir");
            {
                expect_event(&rx);
                expect_no_event(&rx);
                expect_file_state(&t, asset_dir, "testdir");
                expect_dirty_file_state(&t, asset_dir, "testdir");
            }
            {
                add_test_file(&dir, "test.txt");
                expect_event(&rx);
                expect_no_event(&rx);
                expect_file_state(&t, &dir, "test.txt");
                expect_dirty_file_state(&t, &dir, "test.txt");
            }
        });
    }
}
