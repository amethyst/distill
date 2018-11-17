extern crate capnp;
extern crate lmdb;
extern crate rayon;

use capnp_db::{CapnpCursor, DBTransaction, Environment, RoTransaction, RwTransaction, MessageReader};
use crossbeam_channel::{self as channel, Receiver, Sender};
use data_capnp::{self, dirty_file_info, source_file_info, FileType};
use error::Result;
use lmdb::Cursor;
use std::{
    cmp::PartialEq,
    collections::{HashMap, HashSet},
    fs,
    iter::FromIterator,
    ops::IndexMut,
    path::PathBuf,
    str,
    sync::atomic::{AtomicBool, Ordering},
    sync::Arc,
    thread,
    time::Duration,
};
use watcher::{self, FileEvent, FileMetadata};

#[derive(Clone)]
struct FileTrackerTables {
    /// Contains Path -> SourceFileInfo
    source_files: lmdb::Database,
    /// Contains Path -> DirtyFileInfo
    dirty_files: lmdb::Database,
}
#[derive(Debug)]
pub enum FileTrackerEvent {
    Updated,
}
pub struct FileTracker {
    db: Arc<Environment>,
    tables: FileTrackerTables,
    listener_rx: Receiver<Sender<FileTrackerEvent>>,
    listener_tx: Sender<Sender<FileTrackerEvent>>,
    is_running: AtomicBool,
}
impl ::std::fmt::Debug for data_capnp::FileState {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match self {
            data_capnp::FileState::Exists => {
                write!(f, "FileState::Exists")?;
            }
            data_capnp::FileState::Deleted => {
                write!(f, "FileState::Deleted")?;
            }
        }
        Ok(())
    }
}
#[derive(Clone, Debug)]
pub struct FileState {
    pub path: PathBuf,
    pub state: data_capnp::FileState,
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

fn build_dirty_file_info(
    state: data_capnp::FileState,
    source_info: source_file_info::Reader,
) -> capnp::message::Builder<capnp::message::HeapAllocator> {
    let mut value_builder = capnp::message::Builder::new_default();
    {
        let mut value = value_builder.init_root::<dirty_file_info::Builder>();
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
        let mut value = value_builder.init_root::<source_file_info::Builder>();
        value.set_last_modified(metadata.last_modified);
        value.set_length(metadata.length);
        value.set_type(db_file_type(metadata.file_type));
    }
    value_builder
}

fn update_deleted_dirty_entry<K>(
    txn: &mut RwTransaction,
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
                build_dirty_file_info(data_capnp::FileState::Deleted, info)
            })
    };
    if dirty_value.is_some() {
        txn.put(tables.dirty_files, key, &dirty_value.unwrap())?;
    }
    Ok(())
}

fn handle_file_event(
    txn: &mut RwTransaction,
    tables: &FileTrackerTables,
    evt: watcher::FileEvent,
    scan_stack: &mut Vec<ScanContext>,
) -> Result<()> {
    match evt {
        FileEvent::Updated(path, metadata) => {
            let path_str = path.to_string_lossy();
            let key = path_str.as_bytes();
            let mut changed = true;
            {
                let maybe_msg: Option<MessageReader<source_file_info::Owned>> =
                    txn.get(tables.source_files, &key)?;
                if let Some(msg) = maybe_msg {
                    let info = msg.get()?;
                    if info.get_length() == metadata.length
                        && info.get_last_modified() == metadata.last_modified
                        && info.get_type()? == db_file_type(metadata.file_type)
                    {
                        changed = false;
                    } else {
                        println!("CHANGED {} metadata {:?}", path_str, metadata);
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
            let value = build_source_info(&metadata);
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
            update_deleted_dirty_entry(txn, &tables, &key)?;
            txn.delete(tables.source_files, &key)?;
        }
        FileEvent::FileError(err) => {
            println!("file event error: {}", err);
            return Err(err);
        }
        FileEvent::ScanStart(path) => {
            println!("scan start: {}", path.to_string_lossy());
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
            println!(
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
                        let key = str::from_utf8(key_bytes).expect("Encoded key was invalid utf8");
                        if !dirs_as_strings.iter().any(|dir| key.starts_with(dir)) {
                            println!("TRASH! {}", key);
                            to_delete.push(key);
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
) -> Result<()> {
    while *is_running {
        let timeout = Duration::from_millis(100);
        select! {
            recv(rx, evt) => {
                if let Some(evt) = evt {
                    handle_file_event(txn, tables, evt, scan_stack)?;
                } else {
                    println!("receive error");
                    *is_running = false;
                    break;
                }
            }
            recv(channel::after(timeout)) => {
                break;
            }
        }
    }
    Ok(())
}

impl FileTracker {
    pub fn new(db: Arc<Environment>) -> Result<FileTracker> {
        let (tx, rx) = channel::unbounded();
        Ok(FileTracker {
            tables: FileTrackerTables {
                source_files: db.create_db(Some("source_files"), lmdb::DatabaseFlags::default())?,
                dirty_files: db.create_db(Some("dirty_files"), lmdb::DatabaseFlags::default())?,
            },
            db: db,
            listener_rx: rx,
            listener_tx: tx,
            is_running: AtomicBool::new(false),
        })
    }

    pub fn read_dirty_files<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        iter_txn: &'a V,
    ) -> Result<Vec<FileState>> {
        let mut file_states = Vec::new();
        {
            let mut cursor = iter_txn.open_ro_cursor(self.tables.dirty_files)?;
            for (key_result, value_result) in cursor.capnp_iter_start() {
                let key = str::from_utf8(key_result).expect("Failed to parse key as utf8");
                let value_result = value_result?;
                let info = value_result.get_root::<dirty_file_info::Reader>()?;
                let source_info = info.get_source_info()?;
                file_states.push(FileState {
                    path: PathBuf::from(key),
                    state: info.get_state()?,
                    last_modified: source_info.get_last_modified(),
                    length: source_info.get_length(),
                });
            }
        }
        Ok(file_states)
    }

    pub fn read_all_files(&self, iter_txn: &RoTransaction) -> Result<Vec<FileState>> {
        let mut file_states = Vec::new();
        {
            let mut cursor = iter_txn.open_ro_cursor(self.tables.source_files)?;
            for (key_result, value_result) in cursor.capnp_iter_start() {
                let key = str::from_utf8(key_result).expect("Failed to parse key as utf8");
                let value_result = value_result?;
                let info = value_result.get_root::<source_file_info::Reader>()?;
                file_states.push(FileState {
                    path: PathBuf::from(key),
                    state: data_capnp::FileState::Exists,
                    last_modified: info.get_last_modified(),
                    length: info.get_length(),
                });
            }
        }
        Ok(file_states)
    }

    pub fn delete_dirty_file_state<'a>(
        &self,
        txn: &'a mut RwTransaction,
        path: &PathBuf,
    ) -> Result<bool> {
        let key_str = path.to_string_lossy();
        let key = key_str.as_bytes();
        Ok(txn.delete(self.tables.dirty_files, &key)?)
    }

    pub fn get_dirty_file_state<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
        path: &PathBuf,
    ) -> Result<Option<FileState>> {
        let key_str = path.to_string_lossy();
        let key = key_str.as_bytes();
        match txn.get::<dirty_file_info::Owned, &[u8]>(self.tables.dirty_files, &key)? {
            Some(value) => {
                let info = value
                    .get()?
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

    pub fn get_file_state<'a, V: DBTransaction<'a, T>, T: lmdb::Transaction + 'a>(
        &self,
        txn: &'a V,
        path: &PathBuf,
    ) -> Result<Option<FileState>> {
        let key_str = path.to_string_lossy();
        let key = key_str.as_bytes();
        match txn.get::<source_file_info::Owned, &[u8]>(self.tables.source_files, &key)? {
            Some(value) => {
                let info = value.get()?;
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

    pub fn register_listener(&self, sender: Sender<FileTrackerEvent>) {
        self.listener_tx.send(sender)
    }

    pub fn stop(&self) {
        self.is_running.store(false, Ordering::Release);
    }

    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Acquire)
    }

    pub fn run(&self, to_watch: Vec<&str>) -> Result<()> {
        if self
            .is_running
            .compare_and_swap(false, true, Ordering::AcqRel)
        {
            return Ok(());
        }
        let (tx, rx) = channel::unbounded();
        let mut watcher = watcher::DirWatcher::new(to_watch, tx)?;

        let stop_handle = watcher.stop_handle();
        let handle = thread::spawn(move || watcher.run());
        let mut listeners = vec![];
        while self.is_running.load(Ordering::Acquire) {
            select! {
                recv(self.listener_rx, listener) => {
                    if let Some(listener) = listener {
                        listeners.push(listener);
                    }
                },
                default => {}
            }
            {
                let mut txn = self.db.rw_txn()?;
                let mut scan_stack = Vec::new();
                let mut is_running = true;
                read_file_events(
                    &mut is_running,
                    &mut txn,
                    &self.tables,
                    &rx,
                    &mut scan_stack,
                )?;
                assert!(scan_stack.is_empty());
                if txn.dirty {
                    txn.commit()?;
                    println!("Commit");
                    for listener in &listeners {
                    println!("Sent to listener");
                        select! {
                            send(listener, FileTrackerEvent::Updated) => {}
                            default => {}
                        }
                    }
                }
                if !is_running {
                    self.is_running.store(false, Ordering::Release);
                }
            }
        }
        stop_handle.stop();
        handle.join().expect("thread panicked")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use capnp_db::Environment;
    use crossbeam_channel::{self as channel, Receiver};
    use file_tracker::{FileTracker, FileTrackerEvent};
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::Arc,
        thread,
        time::Duration,
    };
    use tempfile;

    fn with_tracker<F>(f: F)
    where
        F: FnOnce(Arc<FileTracker>, Receiver<FileTrackerEvent>, &Path),
    {
        let db_dir = tempfile::tempdir().unwrap();
        let asset_dir = tempfile::tempdir().unwrap();
        {
            let _ = fs::create_dir(db_dir.path());
            let db =
                Arc::new(Environment::new(db_dir.path()).expect("failed to create db environment"));
            let tracker = Arc::new(FileTracker::new(db).expect("failed to create tracker"));
            let (tx, rx) = channel::unbounded();
            tracker.register_listener(tx);

            let asset_path = PathBuf::from(asset_dir.path());
            let handle = {
                let run_tracker = tracker.clone();
                thread::spawn(move || {
                    run_tracker
                        .clone()
                        .run(vec![asset_path.to_str().unwrap()])
                        .expect("error running file tracker");
                })
            };
            while !tracker.is_running() {
                thread::sleep(Duration::from_millis(1));
            }

            expect_no_event(&rx);

            f(tracker.clone(), rx, asset_dir.path());

            tracker.stop();

            handle.join().unwrap();
        }
    }

    fn expect_no_event(rx: &Receiver<FileTrackerEvent>) {
        select! {
            recv(rx, evt) => {
                if let Some(evt) = evt {
                    assert!(false, "Received unexpected event {:?}", evt);
                } else {
                    assert!(false, "Receive error when waiting for file event");
                }
            }
            recv(channel::after(Duration::from_millis(1000))) => {
                return;
            }
        };
        unreachable!();
    }

    fn expect_event(rx: &Receiver<FileTrackerEvent>) -> FileTrackerEvent {
        select! {
            recv(rx, evt) => {
                if let Some(evt) = evt {
                    return evt;
                } else {
                    assert!(false, "Receive error when waiting for file event");
                }
            }
            recv(channel::after(Duration::from_millis(1000))) => {
                    assert!(false, "Timed out waiting for file event");
            }
        };
        unreachable!();
    }

    fn expect_no_file_state(t: &FileTracker, asset_dir: &Path, name: &str) {
        let txn = t.get_ro_txn().expect("failed to open ro txn");
        let path = fs::canonicalize(&asset_dir).expect(&format!(
            "failed to canonicalize {}",
            asset_dir.to_string_lossy()
        ));
        let canonical_path = path.join(name);
        let maybe_state = t
            .get_file_state(&txn, &canonical_path)
            .expect("error getting file state");
        assert!(
            maybe_state.is_none(),
            "expected no file state for file {}",
            name
        );
    }

    fn expect_file_state(t: &FileTracker, asset_dir: &Path, name: &str) {
        let txn = t.get_ro_txn().expect("failed to open ro txn");
        let canonical_path =
            fs::canonicalize(asset_dir.join(name)).expect("failed to canonicalize");
        t.get_file_state(&txn, &canonical_path)
            .expect("error getting file state")
            .expect(&format!("expected file state for file {}", name));
    }

    fn add_test_dir(asset_dir: &Path, name: &str) -> PathBuf {
        let path = PathBuf::from(asset_dir).join(name);
        fs::create_dir(&path).expect("create dir");
        path
    }

    fn add_test_file(asset_dir: &Path, name: &str) {
        fs::copy(
            PathBuf::from("tests/file_tracker/").join(name),
            asset_dir.join(name),
        )
        .expect("copy test file");
    }

    fn delete_test_file(asset_dir: &Path, name: &str) {
        fs::remove_file(asset_dir.join(name)).expect("delete test file");
    }
    fn truncate_test_file(asset_dir: &Path, name: &str) {
        fs::File::create(asset_dir.join(name)).expect("truncate test file");
    }

    fn expect_dirty_file_state(t: &FileTracker, asset_dir: &Path, name: &str) {
        let txn = t.get_ro_txn().expect("failed to open ro txn");
        let path = fs::canonicalize(&asset_dir).expect(&format!(
            "failed to canonicalize {}",
            asset_dir.to_string_lossy()
        ));
        let canonical_path = path.join(name);
        t.get_dirty_file_state(&txn, &canonical_path)
            .expect("error getting dirty file state")
            .expect(&format!("expected dirty file state for file {}", name));
    }

    fn clear_dirty_file_state(t: &FileTracker) {
        let mut txn = t.get_rw_txn().expect("failed to open rw txn");
        for f in t
            .read_dirty_files(&txn)
            .expect("failed to read dirty files")
        {
            t.delete_dirty_file_state(&mut txn, &f.path)
                .expect("failed to delete dirty file state");
        }
    }

    #[test]
    fn test_create_file() {
        with_tracker(|t, rx, asset_dir| {
            add_test_file(asset_dir, "Amplifier.obj");
            expect_event(&rx);
            expect_no_event(&rx);
            expect_file_state(&t, asset_dir, "Amplifier.obj");
            expect_dirty_file_state(&t, asset_dir, "Amplifier.obj");
        });
    }

    #[test]
    fn test_modify_file() {
        with_tracker(|t, rx, asset_dir| {
            add_test_file(asset_dir, "Amplifier.obj");
            expect_event(&rx);
            expect_file_state(&t, asset_dir, "Amplifier.obj");
            expect_dirty_file_state(&t, asset_dir, "Amplifier.obj");
            clear_dirty_file_state(&t);
            truncate_test_file(asset_dir, "Amplifier.obj");
            expect_event(&rx);
            expect_no_event(&rx);
            expect_file_state(&t, asset_dir, "Amplifier.obj");
            expect_dirty_file_state(&t, asset_dir, "Amplifier.obj");
        })
    }

    #[test]
    fn test_delete_file() {
        with_tracker(|t, rx, asset_dir| {
            add_test_file(asset_dir, "Amplifier.obj");
            expect_event(&rx);
            expect_file_state(&t, asset_dir, "Amplifier.obj");
            expect_dirty_file_state(&t, asset_dir, "Amplifier.obj");
            clear_dirty_file_state(&t);
            delete_test_file(asset_dir, "Amplifier.obj");
            expect_event(&rx);
            expect_no_event(&rx);
            expect_no_file_state(&t, asset_dir, "Amplifier.obj");
            expect_dirty_file_state(&t, asset_dir, "Amplifier.obj");
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
                add_test_file(&dir, "Amplifier.obj");
                expect_event(&rx);
                expect_no_event(&rx);
                expect_file_state(&t, &dir, "Amplifier.obj");
                expect_dirty_file_state(&t, &dir, "Amplifier.obj");
            }
        });
    }
}
