use crate::capnp_db::{
    CapnpCursor, DBTransaction, Environment, MessageReader, RoTransaction, RwTransaction,
};
use crate::error::{Error, Result};
use crate::watcher::{self, FileEvent, FileMetadata};
use atelier_core::utils::{self, canonicalize_path};
use atelier_schema::data::{self, dirty_file_info, rename_file_event, source_file_info, FileType};
use event_listener::Event;
use futures::stream::StreamExt;
use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    FutureExt,
};
use lmdb::Cursor;
use log::{debug, info};
use std::{
    cell::Cell,
    cmp::PartialEq,
    collections::{HashMap, HashSet},
    fs,
    ops::IndexMut,
    path::{Path, PathBuf},
    str,
    sync::atomic::{AtomicBool, Ordering},
    sync::Arc,
    thread,
    time::Duration,
};
use tokio::{
    sync::{mpsc, Mutex},
    time,
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
    listener_rx: Mutex<Cell<UnboundedReceiver<UnboundedSender<FileTrackerEvent>>>>,
    listener_tx: UnboundedSender<UnboundedSender<FileTrackerEvent>>,
    is_running: AtomicBool,
    stopping_event: event_listener::Event,
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

struct ListenersList {
    listeners: Vec<UnboundedSender<FileTrackerEvent>>,
}

impl ListenersList {
    fn new() -> Self {
        Self {
            listeners: Vec::new(),
        }
    }
    fn register(&mut self, new_listener: Option<UnboundedSender<FileTrackerEvent>>) {
        if let Some(new_listener) = new_listener {
            self.listeners.push(new_listener);
        }
    }
    fn send_event(&mut self, event: FileTrackerEvent) {
        self.listeners.retain(|listener| {
            match listener.unbounded_send(event) {
                Ok(()) => {
                    debug!("Sent to listener");
                    true
                }
                // channel was closed, drop the listener
                Err(_) => {
                    debug!("Listener dropped");
                    false
                }
            }
        })
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
        .open_ro_cursor(tables.rename_file_events)
        .expect("Failed to open RO cursor for rename_file_events")
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

// TODO(happens): Improve error handling for event handlers
mod events {
    use std::path::Path;

    use super::*;
    fn handle_update(
        txn: &mut RwTransaction<'_>,
        tables: &FileTrackerTables,
        path: &Path,
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
            scan_ctx.files.insert(path.to_path_buf(), metadata.clone());
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
                    let cursor = txn
                        .open_ro_cursor(tables.source_files)
                        .expect("Failed to open RO cursor for source_files table");
                    for (key, _) in cursor.capnp_iter_from(&key) {
                        let key = str::from_utf8(key).expect("Encoded key was invalid utf8");
                        if !key.starts_with(&path_string) {
                            break;
                        }
                        db_file_set.insert(PathBuf::from(key));
                    }
                }
                let scan_ctx_set: HashSet<PathBuf> = scan_ctx.files.keys().cloned().collect();
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
                        let mut cursor = txn
                            .open_ro_cursor(tables.source_files)
                            .expect("Failed to open RO cursor for source_files table");
                        let dirs_as_strings: Vec<String> = watched_dirs
                            .into_iter()
                            .map(|f| f.to_string_lossy().into_owned())
                            .collect();
                        for iter_result in cursor.iter_start() {
                            let (key_bytes, _) =
                                iter_result.expect("Error while iterating source file metadata");
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
            .map(|s| {
                let path = PathBuf::from(s);
                let path = if path.is_relative() {
                    std::env::current_dir()
                        .expect("failed to get current dir")
                        .join(path)
                } else {
                    path
                };
                canonicalize_path(&path)
            })
            .collect();

        let source_files = db
            .create_db(Some("source_files"), lmdb::DatabaseFlags::default())
            .expect("db: Failed to create source_files table");

        let dirty_files = db
            .create_db(Some("dirty_files"), lmdb::DatabaseFlags::default())
            .expect("db: Failed to create dirty_files table");

        let rename_file_events = db
            .create_db(Some("rename_file_events"), lmdb::DatabaseFlags::INTEGER_KEY)
            .expect("db: Failed to create rename_file_events table");

        let (listener_tx, listener_rx) = unbounded();

        FileTracker {
            is_running: AtomicBool::new(false),
            stopping_event: Event::new(),
            tables: FileTrackerTables {
                source_files,
                dirty_files,
                rename_file_events,
            },
            db,
            listener_rx: Mutex::new(Cell::new(listener_rx)),
            listener_tx,
            watch_dirs,
        }
    }

    pub fn make_relative_path(&self, absolute_path: &Path) -> Option<PathBuf> {
        for dir in self.get_watch_dirs() {
            let canonicalized_dir = canonicalize_path(&dir);
            if absolute_path.starts_with(&canonicalized_dir) {
                let relative_path = absolute_path
                    .strip_prefix(canonicalized_dir)
                    .expect("error stripping prefix")
                    .to_path_buf();
                let relative_path = canonicalize_path(&relative_path)
                    .to_string_lossy()
                    .replace("\\", "/");
                return Some(PathBuf::from(relative_path));
            }
        }
        None
    }

    pub fn get_watch_dirs(&self) -> impl Iterator<Item = &'_ PathBuf> {
        self.watch_dirs.iter()
    }

    pub async fn get_rw_txn(&self) -> RwTransaction<'_> {
        self.db.rw_txn().await.expect("db: Failed to open rw txn")
    }

    pub fn read_rename_events<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        iter_txn: &'a V,
    ) -> Vec<(u64, RenameFileEvent)> {
        iter_txn
            .open_ro_cursor(self.tables.rename_file_events)
            .expect("db: Failed to open ro cursor for rename_file_events table")
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

    pub fn clear_rename_events(&self, txn: &mut RwTransaction<'_>) {
        txn.clear_db(self.tables.rename_file_events)
            .expect("db: Failed to clear rename_file_events table");
    }

    pub async fn add_dirty_file(&self, txn: &mut RwTransaction<'_>, path: &Path) -> Result<()> {
        let metadata = match tokio::fs::metadata(path).await {
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
        // NOTE(happens): If we have any errors while looping over a dirty file, we
        // should somehow be able to mark it for a retry. We can probably rely on
        // the fact that since we skip them, they will still be dirty on the next attempt.
        iter_txn
            .open_ro_cursor(self.tables.dirty_files)
            .expect("db: Failed to open ro cursor for dirty_files table")
            .capnp_iter_start()
            .filter_map(|(key, val)| {
                // TODO(happens): Do we want logging on why things are skipped here?
                // We could map to Result<_> first, and then log any errors in a filter_map
                // that just calls ok() afterwards.
                let key = str::from_utf8(key).expect("utf8: Failed to parse file path");
                let val = val.expect("capnp: Failed to get value in iterator");
                let info = val.get_root::<dirty_file_info::Reader<'_>>().ok()?;
                let source_info = info
                    .get_source_info()
                    .expect("capnp: Failed to get source info");

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
            .expect("db: Failed to open ro cursor for source_files table")
            .capnp_iter_start()
            .filter_map(|(key, val)| {
                let key = str::from_utf8(key).expect("utf8: Failed to parse file path");
                let val = val.expect("capnp: Failed to get value in iterator");
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

    pub fn delete_dirty_file_state<'a>(&self, txn: &'a mut RwTransaction<'_>, path: &Path) -> bool {
        let key_str = path.to_string_lossy();
        let key = key_str.as_bytes();

        txn.delete(self.tables.dirty_files, &key)
            .expect("db: Failed to delete entry from dirty_files table")
    }

    #[cfg(not(target_os = "macos"))] // FIXME: these tests fail in macos CI
    #[cfg(test)]
    pub fn get_dirty_file_state<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
        path: &Path,
    ) -> Option<FileState> {
        let key_str = path.to_string_lossy();
        let key = key_str.as_bytes();

        txn.get::<dirty_file_info::Owned, &[u8]>(self.tables.dirty_files, &key)
            .expect("db: Failed to get entry from dirty_files table")
            .map(|value| {
                let value = value.get().expect("capnp: Failed to get dirty file info");

                let info = value
                    .get_source_info()
                    .expect("capnp: Failed to get source info");

                FileState {
                    path: path.to_path_buf(),
                    state: data::FileState::Exists,
                    last_modified: info.get_last_modified(),
                    length: info.get_length(),
                }
            })
    }

    pub fn get_file_state<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
        path: &Path,
    ) -> Option<FileState> {
        let key_str = path.to_string_lossy();
        let key = key_str.as_bytes();

        txn.get::<source_file_info::Owned, &[u8]>(self.tables.source_files, &key)
            .expect("db: Failed to get entry from source_files table")
            .map(|value| {
                let info = value.get().expect("capnp: Failed to get source file info");

                FileState {
                    path: path.to_path_buf(),
                    state: data::FileState::Exists,
                    last_modified: info.get_last_modified(),
                    length: info.get_length(),
                }
            })
    }

    pub fn register_listener(&self, sender: UnboundedSender<FileTrackerEvent>) {
        self.listener_tx
            .unbounded_send(sender)
            .expect("Failed registering listener")
    }

    pub async fn stop(&self) {
        if self.is_running() {
            self.stopping_event.notify(std::usize::MAX);
            self.listener_rx.lock().await;
            assert_eq!(false, self.is_running.load(Ordering::Acquire));
        }
    }

    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Acquire)
    }

    pub async fn run(&self) {
        if self
            .is_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let (watcher_tx, mut watcher_rx) = mpsc::unbounded_channel();
        let to_watch = self
            .watch_dirs
            .iter()
            .map(|p| p.to_str().expect("Invalid path"));

        // NOTE(happens): If we can't watch the dir, we want to abort
        let mut watcher = watcher::DirWatcher::from_path_iter(to_watch, watcher_tx)
            .expect("watcher: Failed to watch specified path");

        let stop_handle = watcher.stop_handle();
        thread::spawn(move || watcher.run());

        let mut listeners = ListenersList::new();
        let mut scan_stack = Vec::new();

        let mut stopping = self.stopping_event.listen();

        let mut listener_tx_guard = self.listener_rx.lock().await;
        let listener_tx = listener_tx_guard.get_mut();

        let mut dirty = false;

        #[allow(unused_mut)]
        loop {
            let mut delay = time::sleep(Duration::from_millis(40));

            tokio::select! {
                new_listener = listener_tx.next() => listeners.register(new_listener),
                _ = delay, if dirty => {
                    listeners.send_event(FileTrackerEvent::Update);
                    dirty = false;
                },
                mut maybe_file_event = watcher_rx.recv() => {
                    if maybe_file_event.is_none() {
                        debug!("FileTracker: stopping due to exhausted watcher");
                        break;
                    }

                    let mut txn = self.get_rw_txn().await;

                    // batch watcher events into single transaction and update
                    while let Some(file_event) = maybe_file_event {
                        match events::handle_file_event(&mut txn, &self.tables, file_event, &mut scan_stack) {
                            Ok(Some(evt)) => listeners.send_event(evt),
                            Ok(None) => {},
                            Err(err) => panic!("Error while handling file event: {}", err),
                        }

                        maybe_file_event = watcher_rx.recv().now_or_never().flatten();
                    }

                    if txn.dirty {
                        txn.commit().expect("Failed to commit");
                        dirty = true;
                    }
                }
                _ = &mut stopping => {
                    debug!("FileTracker: stopping due to stop() notification");
                    break;
                }
            }
        }

        listeners.send_event(FileTrackerEvent::Update);
        drop(stop_handle);
        self.is_running.store(false, Ordering::Release);
    }
}

#[cfg(not(target_os = "macos"))] // FIXME: these tests fail in macos CI
#[cfg(test)]
pub mod tests {

    use tokio::time::timeout;

    use super::*;
    use crate::capnp_db::Environment;
    use crate::file_tracker::{FileTracker, FileTrackerEvent};
    use std::future::Future;
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::Arc,
        time::Duration,
    };

    pub async fn with_tracker<F, T>(f: F)
    where
        T: Future<Output = ()>,
        F: FnOnce(Arc<FileTracker>, UnboundedReceiver<FileTrackerEvent>, PathBuf) -> T,
    {
        let _ = crate::init_logging();
        let db_dir = tempfile::tempdir().unwrap();
        let asset_dir = tempfile::tempdir().unwrap();

        let _ = fs::create_dir(db_dir.path());
        let asset_paths = vec![asset_dir.path().to_str().unwrap()];
        let db = Arc::new(
            Environment::with_map_size(db_dir.path(), 1 << 21).unwrap_or_else(|_| {
                panic!(
                    "failed to create db environment {}",
                    db_dir.path().to_string_lossy()
                )
            }),
        );
        let tracker = Arc::new(FileTracker::new(db, asset_paths));
        let (tx, mut rx) = unbounded();
        tracker.register_listener(tx);

        let tracker_clone = tracker.clone();
        let handle = tokio::task::spawn(async move { tracker_clone.run().await });
        expect_event(&mut rx).await;

        f(tracker.clone(), rx, asset_dir.into_path()).await;

        tracker.stop().await;
        handle.await.unwrap();
    }

    async fn expect_no_event(rx: &mut UnboundedReceiver<FileTrackerEvent>) {
        match timeout(Duration::from_millis(1000), rx.next()).await {
            Err(_) => return,
            Ok(evt) => panic!("Received unexpected event {:?}", evt),
        }
    }

    async fn expect_event(rx: &mut UnboundedReceiver<FileTrackerEvent>) -> FileTrackerEvent {
        match timeout(Duration::from_millis(10000), rx.next()).await {
            Err(_) => panic!("Timed out waiting for file event"),
            Ok(evt) => return evt.unwrap(),
        }
    }

    async fn expect_no_file_state(t: &FileTracker, asset_dir: &Path, name: &str) {
        let txn = t.get_rw_txn().await;
        let canonical_path = canonicalize_path(&asset_dir.join(name));

        assert!(
            t.get_file_state(&txn, &canonical_path).is_none(),
            "expected no file state for file {}",
            name
        );
    }

    async fn expect_file_state(t: &FileTracker, asset_dir: &Path, name: &str) {
        let txn = t.get_rw_txn().await;
        let canonical_path = canonicalize_path(&asset_dir.join(name));

        t.get_file_state(&txn, &canonical_path)
            .unwrap_or_else(|| panic!("expected file state for file {}", name));
    }

    pub async fn add_test_dir(asset_dir: &Path, name: &str) -> PathBuf {
        let path = PathBuf::from(asset_dir).join(name);
        tokio::fs::create_dir(&path).await.expect("create dir");
        path
    }

    pub async fn add_test_file(asset_dir: &Path, name: &str) {
        tokio::fs::copy(
            PathBuf::from("tests/file_tracker/").join(name),
            asset_dir.join(name),
        )
        .await
        .expect("copy test file");
    }

    #[cfg(target_family = "windows")]
    pub fn add_symlink_file(asset_dir: &Path, name: &str, target: &str) {
        match std::os::windows::fs::symlink_file(target, PathBuf::from(asset_dir).join(name)) {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
            err => panic!("failed to create symlink file: {:?}", err),
        }
    }

    #[cfg(target_family = "unix")]
    pub fn add_symlink_file(asset_dir: &Path, name: &str, target: &str) {
        match std::os::unix::fs::symlink(target, PathBuf::from(asset_dir).join(name)) {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
            err => panic!("failed to create symlink file: {:?}", err),
        }
    }

    async fn expect_dirty_file_state(t: &FileTracker, asset_dir: &Path, name: &str) {
        let txn = t.get_rw_txn().await;
        let path = canonicalize_path(&PathBuf::from(asset_dir));
        let canonical_path = path.join(name);
        t.get_dirty_file_state(&txn, &canonical_path)
            .unwrap_or_else(|| panic!("expected dirty file state for file {}", name));
    }

    async fn clear_dirty_file_state(t: &FileTracker) {
        let mut txn = t.get_rw_txn().await;
        for f in t.read_dirty_files(&txn) {
            t.delete_dirty_file_state(&mut txn, &f.path);
        }
    }

    #[tokio::test]
    async fn test_create_file() {
        with_tracker(|t, mut rx, asset_dir| async move {
            add_test_file(&asset_dir, "test.txt").await;
            expect_event(&mut rx).await;
            expect_no_event(&mut rx).await;
            expect_file_state(&t, &asset_dir, "test.txt").await;
            expect_dirty_file_state(&t, &asset_dir, "test.txt").await;
        })
        .await;
    }

    #[tokio::test]
    async fn test_modify_file() {
        with_tracker(|t, mut rx, asset_dir| async move {
            add_test_file(&asset_dir, "test.txt").await;
            expect_event(&mut rx).await;
            expect_file_state(&t, &asset_dir, "test.txt").await;
            expect_dirty_file_state(&t, &asset_dir, "test.txt").await;
            clear_dirty_file_state(&t).await;

            tokio::fs::File::create(asset_dir.join("test.txt"))
                .await
                .expect("truncate test file");

            expect_event(&mut rx).await;
            expect_no_event(&mut rx).await;
            expect_file_state(&t, &asset_dir, "test.txt").await;
            expect_dirty_file_state(&t, &asset_dir, "test.txt").await;
        })
        .await;
    }

    #[tokio::test]
    async fn test_delete_file() {
        with_tracker(|t, mut rx, asset_dir| async move {
            add_test_file(&asset_dir, "test.txt").await;
            expect_event(&mut rx).await;
            expect_file_state(&t, &asset_dir, "test.txt").await;
            expect_dirty_file_state(&t, &asset_dir, "test.txt").await;
            clear_dirty_file_state(&t).await;

            tokio::fs::remove_file(asset_dir.join("test.txt"))
                .await
                .expect("test file could not be deleted");

            expect_event(&mut rx).await;
            expect_no_event(&mut rx).await;
            expect_no_file_state(&t, &asset_dir, "test.txt").await;
            expect_dirty_file_state(&t, &asset_dir, "test.txt").await;
        })
        .await;
    }

    #[tokio::test]
    async fn test_create_dir() {
        with_tracker(|t, mut rx, asset_dir| async move {
            add_test_dir(&asset_dir, "testdir").await;
            expect_event(&mut rx).await;
            expect_no_event(&mut rx).await;
            expect_file_state(&t, &asset_dir, "testdir").await;
            expect_dirty_file_state(&t, &asset_dir, "testdir").await;
        })
        .await;
    }

    #[tokio::test]
    async fn test_create_file_in_dir() {
        with_tracker(|t, mut rx, asset_dir| async move {
            let dir = add_test_dir(&asset_dir, "testdir").await;

            expect_event(&mut rx).await;
            expect_no_event(&mut rx).await;
            expect_file_state(&t, &asset_dir, "testdir").await;
            expect_dirty_file_state(&t, &asset_dir, "testdir").await;

            add_test_file(&dir, "test.txt").await;
            expect_event(&mut rx).await;
            expect_no_event(&mut rx).await;
            expect_file_state(&t, &dir, "test.txt").await;
            expect_dirty_file_state(&t, &dir, "test.txt").await;
        })
        .await;
    }

    #[tokio::test]
    async fn test_create_emacs_lockfile() {
        with_tracker(|t, mut rx, asset_dir| async move {
            add_symlink_file(&asset_dir, "emacs.symlink", "emacs@lock.file:buffer");
            expect_event(&mut rx).await;
            expect_no_event(&mut rx).await;
            expect_file_state(&t, &asset_dir, "emacs.symlink").await;
            expect_dirty_file_state(&t, &asset_dir, "emacs.symlink").await;
        }).await;
    }
}
