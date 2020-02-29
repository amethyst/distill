use crate::AssetUuid;
use std::{
    ffi::OsStr,
    hash::{Hash, Hasher},
    path::PathBuf,
};

use core::pin::Pin;
use core::task::Context;
use core::task::Poll;

use futures::{AsyncRead, AsyncWrite};

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

pub fn make_array<A, T>(slice: &[T]) -> A
where
    A: Sized + Default + AsMut<[T]>,
    T: Copy,
{
    let mut a = Default::default();
    <A as AsMut<[T]>>::as_mut(&mut a).copy_from_slice(slice);
    a
}

pub fn uuid_from_slice(slice: &[u8]) -> Option<AssetUuid> {
    const BYTES_LEN: usize = 16;

    let len = slice.len();

    if len != BYTES_LEN {
        return None;
    }

    let mut bytes: uuid::Bytes = [0; 16];
    bytes.copy_from_slice(slice);
    Some(AssetUuid(bytes))
}

pub fn to_meta_path(p: &PathBuf) -> PathBuf {
    p.with_file_name(OsStr::new(
        &(p.file_name().unwrap().to_str().unwrap().to_owned() + ".meta"),
    ))
}

pub fn calc_import_artifact_hash<T>(id: &AssetUuid, import_hash: u64, dep_list: T) -> u64
where
    T: IntoIterator,
    T::Item: Hash,
{
    let mut hasher = ::std::collections::hash_map::DefaultHasher::new();
    import_hash.hash(&mut hasher);
    id.hash(&mut hasher);
    for dep in dep_list {
        dep.hash(&mut hasher);
    }
    hasher.finish()
}

pub struct AsyncSender {
    pub buf: Arc<Mutex<VecDeque<u8>>>,
}

impl AsyncSender {
    fn new() -> Self {
        Self {
            buf: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

impl AsyncWrite for AsyncSender {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, std::io::Error>> {
        let mut guard = self.get_mut().buf.lock().unwrap();
        let mut buf: VecDeque<u8> = buf.to_vec().into_iter().collect();
        let n = buf.len();
        guard.append(&mut buf);

        match n {
            0 => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            _ => Poll::Ready(Ok(n)),
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }
}

pub struct AsyncReceiver {
    buf: Arc<Mutex<VecDeque<u8>>>,
}

impl AsyncReceiver {
    fn new() -> Self {
        Self {
            buf: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

impl AsyncRead for AsyncReceiver {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::result::Result<usize, std::io::Error>> {
        let mut guard = self.get_mut().buf.lock().unwrap();
        let mut n = 0;

        for i in 0..buf.len() {
            match guard.pop_front() {
                Some(byte) => {
                    buf[i] = byte;
                    n += 1;
                }
                None => break,
            }
        }

        match n {
            0 => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            _ => Poll::Ready(Ok(n)),
        }
    }
}

pub fn async_channel() -> (AsyncSender, AsyncReceiver) {
    (AsyncSender::new(), AsyncReceiver::new())
}
