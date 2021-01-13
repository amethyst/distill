use crate::error::{Error, Result};
use atelier_core::utils::canonicalize_path;
use futures::channel::mpsc::UnboundedSender;
use notify::{watcher, DebouncedEvent, RecommendedWatcher, RecursiveMode, Watcher};
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::time::{Duration, UNIX_EPOCH};
use std::{collections::HashMap, path::Path};

/// The purpose of DirWatcher is to provide enough information to
/// determine which files may be candidates for going through the asset import process.
/// It handles updating watches for directories behind symlinks and scans directories on create/delete.
pub struct DirWatcher {
    watcher: RecommendedWatcher,
    symlink_map: HashMap<PathBuf, PathBuf>,
    watch_refs: HashMap<PathBuf, i32>,
    dirs: Vec<PathBuf>,
    rx: Receiver<DebouncedEvent>,
    tx: Sender<DebouncedEvent>,
    asset_tx: UnboundedSender<FileEvent>,
}

pub struct StopHandle {
    tx: Sender<DebouncedEvent>,
}

#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub file_type: fs::FileType,
    pub last_modified: u64,
    pub length: u64,
}
#[derive(Debug)]
pub enum FileEvent {
    Updated(PathBuf, FileMetadata),
    Renamed(PathBuf, PathBuf, FileMetadata),
    Removed(PathBuf),
    FileError(Error),
    // ScanStart is called when a directory is about to be scanned.
    // Scanning can be recursive
    ScanStart(PathBuf),
    // ScanEnd indicates the end of a scan. The set of all watched directories is also sent
    ScanEnd(PathBuf, Vec<PathBuf>),
}
pub(crate) fn file_metadata(metadata: &fs::Metadata) -> FileMetadata {
    let modify_time = metadata.modified().unwrap_or(UNIX_EPOCH);
    let since_epoch = modify_time
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    let in_ms = since_epoch.as_secs() * 1000 + u64::from(since_epoch.subsec_nanos()) / 1_000_000;
    FileMetadata {
        file_type: metadata.file_type(),
        length: metadata.len(),
        last_modified: in_ms,
    }
}

impl DirWatcher {
    pub fn from_path_iter<'a, T>(paths: T, chan: UnboundedSender<FileEvent>) -> Result<DirWatcher>
    where
        T: IntoIterator<Item = &'a str>,
    {
        let (tx, rx) = channel();
        let mut asset_watcher = DirWatcher {
            watcher: watcher(tx.clone(), Duration::from_millis(50))?,
            symlink_map: HashMap::new(),
            watch_refs: HashMap::new(),
            dirs: Vec::new(),
            rx,
            tx,
            asset_tx: chan,
        };
        for path in paths {
            let path = PathBuf::from(path);
            let path = if path.is_relative() {
                std::env::current_dir()?.join(path)
            } else {
                path
            };
            let path = canonicalize_path(&path);
            asset_watcher.watch(&path)?;
        }
        Ok(asset_watcher)
    }

    pub fn stop_handle(&self) -> StopHandle {
        StopHandle {
            tx: self.tx.clone(),
        }
    }
    fn scan_directory<F>(&mut self, dir: &Path, evt_create: &F) -> Result<()>
    where
        F: Fn(PathBuf) -> DebouncedEvent,
    {
        let canonical_dir = canonicalize_path(dir);
        self.asset_tx
            .unbounded_send(FileEvent::ScanStart(canonical_dir.clone()))
            .map_err(|_| Error::SendError)?;
        let result = self.scan_directory_recurse(&canonical_dir, evt_create);
        self.asset_tx
            .unbounded_send(FileEvent::ScanEnd(canonical_dir, self.dirs.clone()))
            .map_err(|_| Error::SendError)?;
        result
    }
    fn scan_directory_recurse<F>(&mut self, dir: &Path, evt_create: &F) -> Result<()>
    where
        F: Fn(PathBuf) -> DebouncedEvent,
    {
        match fs::read_dir(dir) {
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(Error::IO(e)),
            Ok(dir_entry) => {
                for entry in dir_entry {
                    match entry {
                        Err(ref e) if e.kind() == io::ErrorKind::NotFound => continue,
                        Err(e) => return Err(Error::IO(e)),
                        Ok(entry) => {
                            let evt = self.handle_notify_event(evt_create(entry.path()), true)?;
                            if let Some(evt) = evt {
                                self.asset_tx
                                    .unbounded_send(evt)
                                    .map_err(|_| Error::SendError)?;
                            }
                            let metadata;
                            match entry.metadata() {
                                Err(ref e) if e.kind() == io::ErrorKind::NotFound => continue,
                                Err(e) => return Err(Error::IO(e)),
                                Ok(m) => metadata = m,
                            }
                            let is_dir = metadata.is_dir();
                            if is_dir {
                                self.scan_directory_recurse(&entry.path(), evt_create)?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn run(&mut self) {
        for dir in &self.dirs.clone() {
            if let Err(err) = self.scan_directory(&dir, &|path| DebouncedEvent::Create(path)) {
                self.asset_tx
                    .unbounded_send(FileEvent::FileError(err))
                    .expect("Failed to send file error event. Ironic...");
            }
        }

        loop {
            match self.rx.recv() {
                Ok(event) => match self.handle_notify_event(event, false) {
                    Ok(maybe_event) => {
                        if let Some(evt) = maybe_event {
                            log::debug!("File event: {:?}", evt);
                            self.asset_tx.unbounded_send(evt).unwrap();
                        }
                    }
                    Err(err) => match err {
                        Error::RescanRequired => {
                            for dir in &self.dirs.clone() {
                                if let Err(err) =
                                    self.scan_directory(&dir, &|path| DebouncedEvent::Create(path))
                                {
                                    self.asset_tx
                                        .unbounded_send(FileEvent::FileError(err))
                                        .expect("Failed to send file error event");
                                }
                            }
                        }
                        Error::Exit => break,
                        _ => self
                            .asset_tx
                            .unbounded_send(FileEvent::FileError(err))
                            .expect("Failed to send file error event"),
                    },
                },
                Err(_) => {
                    self.asset_tx
                        .unbounded_send(FileEvent::FileError(Error::RecvError))
                        .expect("Failed to send file error event");

                    return;
                }
            }
        }
    }

    fn watch(&mut self, path: &Path) -> Result<bool> {
        let refs = *self.watch_refs.get(path).unwrap_or(&0);
        match refs {
            0 => {
                self.watcher.watch(path, RecursiveMode::Recursive)?;
                self.dirs.push(path.to_path_buf());
                self.watch_refs.insert(path.to_path_buf(), 1);
                return Ok(true);
            }
            refs if refs > 0 => {
                self.watch_refs
                    .entry(path.to_path_buf())
                    .and_modify(|r| *r += 1);
            }
            _ => {}
        }
        Ok(false)
    }

    fn unwatch(&mut self, path: &Path) -> Result<bool> {
        let refs = *self.watch_refs.get(path).unwrap_or(&0);
        if refs == 1 {
            self.watcher.unwatch(path)?;
            for i in 0..self.dirs.len() {
                if *path == self.dirs[i] {
                    self.dirs.remove(i);
                    break;
                }
            }
            self.watch_refs.remove(path);
            return Ok(true);
        } else if refs > 0 {
            self.watch_refs
                .entry(path.to_path_buf())
                .and_modify(|r| *r -= 1);
        }
        Ok(false)
    }

    fn handle_updated_symlink(
        &mut self,
        src: Option<&PathBuf>,
        dst: Option<&PathBuf>,
    ) -> Result<()> {
        if let Some(src) = src {
            if self.symlink_map.contains_key(src) {
                let to_unwatch = self.symlink_map[src].clone();
                if self.unwatch(&to_unwatch)? {
                    self.scan_directory(&to_unwatch, &|p| DebouncedEvent::Remove(p))?;
                }
                self.symlink_map.remove(src);
            }
        }
        if let Some(dst) = dst {
            let link = fs::read_link(&dst);
            if let Ok(link_path) = link {
                let link_path = canonicalize_path(&dst.join(link_path));
                if self.watch(&link_path)? {
                    self.scan_directory(&link_path, &|p| DebouncedEvent::Create(p))?;
                }
                self.symlink_map.insert(dst.clone(), link_path);
            }
        }
        Ok(())
    }

    fn handle_notify_event(
        &mut self,
        event: DebouncedEvent,
        is_scanning: bool,
    ) -> Result<Option<FileEvent>> {
        match event {
            DebouncedEvent::Create(path) | DebouncedEvent::Write(path) => {
                let path = canonicalize_path(&path);
                self.handle_updated_symlink(Option::None, Some(&path))?;
                match fs::metadata(&path) {
                    Err(ref e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
                    Err(e) => Err(Error::IO(e)),
                    Ok(metadata) => Ok(Some(FileEvent::Updated(path, file_metadata(&metadata)))),
                }
            }
            DebouncedEvent::Rename(src, dest) => {
                let src = canonicalize_path(&src);
                let dest = canonicalize_path(&dest);
                self.handle_updated_symlink(Some(&src), Some(&dest))?;
                match fs::metadata(&dest) {
                    Err(ref e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
                    Err(e) => Err(Error::IO(e)),
                    Ok(metadata) => {
                        if metadata.is_dir() && !is_scanning {
                            self.scan_directory(&dest, &|p| {
                                let replaced = canonicalize_path(&src.join(
                                    p.strip_prefix(&dest).expect("Failed to strip prefix dir"),
                                ));
                                DebouncedEvent::Rename(replaced, p)
                            })?;
                        }
                        Ok(Some(FileEvent::Renamed(
                            src,
                            dest,
                            file_metadata(&metadata),
                        )))
                    }
                }
            }
            DebouncedEvent::Remove(path) => {
                let path = canonicalize_path(&path);
                self.handle_updated_symlink(Some(&path), Option::None)?;
                Ok(Some(FileEvent::Removed(path)))
            }
            DebouncedEvent::Rescan => Err(Error::RescanRequired),
            DebouncedEvent::Error(_, _) => Err(Error::Exit),
            _ => Ok(None),
        }
    }
}

impl Drop for StopHandle {
    fn drop(&mut self) {
        let _ = self.tx.send(DebouncedEvent::Error(
            notify::Error::Generic("EXIT".to_string()),
            Option::None,
        ));
    }
}
