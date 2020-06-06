#![allow(dead_code)]
use crate::error::{Error, Result};
use capnp;
use lmdb::{self, Cursor, Transaction};
use std::path::Path;
use std::result::Result as StdResult;
use tokio::sync::{Semaphore, SemaphorePermit};

pub type MessageReader<'a, T> = capnp::message::TypedReader<capnp::serialize::SliceSegments<'a>, T>;

pub struct Environment {
    env: lmdb::Environment,
    write_semaphore: Semaphore,
    read_semaphore: Semaphore,
}
pub struct RoTransaction<'a> {
    txn: lmdb::RoTransaction<'a>,
    permit: SemaphorePermit<'a>,
}

#[must_use]
pub struct RwTransaction<'a> {
    txn: lmdb::RwTransaction<'a>,
    permit: SemaphorePermit<'a>,
    pub dirty: bool,
}

// Safety: We are always using NO_TLS environment flag, which guarantees that
// lmdb transactions are not using thread-local storage.
unsafe impl Send for RoTransaction<'_> {}
unsafe impl Send for RwTransaction<'_> {}
unsafe impl Sync for RoTransaction<'_> {}
unsafe impl Sync for RwTransaction<'_> {}

// pub type RoTransaction<'a> = lmdb::RoTransaction<'a>;

pub struct Iter<'cursor, 'txn> {
    cursor: lmdb::RoCursor<'txn>,
    iter: lmdb::Iter<'txn>,
    _marker: std::marker::PhantomData<&'cursor ()>,
}

pub trait CapnpCursor<'txn> {
    fn capnp_iter_start<'cursor>(self) -> Iter<'cursor, 'txn>;
    fn capnp_iter_from<'cursor, K>(
        self,
        key: &K,
    ) -> Iter<'cursor, 'txn>
    where
        K: AsRef<[u8]>;
}
impl<'txn> CapnpCursor<'txn> for lmdb::RoCursor<'txn> {
    fn capnp_iter_start<'cursor>(mut self) -> Iter<'cursor, 'txn> {
        Iter {
            iter: self.iter_start(),
            cursor: self,
            _marker: std::marker::PhantomData,
        }
    }
    fn capnp_iter_from<'cursor, K>(
        mut self,
        key: &K,
    ) -> Iter<'cursor, 'txn>
    where
        K: AsRef<[u8]>,
    {
        Iter {
            iter: self.iter_from(key),
            cursor: self,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'cursor, 'txn> Iterator for Iter<'cursor, 'txn> {
    type Item = (
        &'txn [u8],
        StdResult<capnp::message::Reader<capnp::serialize::SliceSegments<'txn>>, capnp::Error>,
    );
    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some((key_bytes, value_bytes)) => {
                let mut slice = value_bytes;
                let value_msg = capnp::serialize::read_message_from_flat_slice(
                    &mut slice,
                    capnp::message::ReaderOptions::default(),
                );
                Some((key_bytes, value_msg))
            }
            None => None,
        }
    }
}

pub trait DBTransaction<'a, T: lmdb::Transaction + 'a>: Sized {
    fn txn(&'a self) -> &'a T;

    fn open_ro_cursor(
        &'a self,
        db: lmdb::Database,
    ) -> Result<lmdb::RoCursor<'a>> {
        Ok(self.txn().open_ro_cursor(db)?)
    }

    fn get<V: for<'b> capnp::traits::Owned<'b>, K>(
        &'a self,
        db: lmdb::Database,
        key: &K,
    ) -> Result<Option<MessageReader<'a, V>>>
    where
        K: AsRef<[u8]>,
    {
        let get_result = self.txn().get(db, key);
        if get_result.is_err() {
            Ok(None)
        } else {
            let mut slice = get_result.unwrap();
            let msg = capnp::serialize::read_message_from_flat_slice(
                &mut slice,
                capnp::message::ReaderOptions::default(),
            )?
            .into_typed::<V>();
            Ok(Some(msg))
        }
    }
    fn get_as_bytes<K>(
        &'a self,
        db: lmdb::Database,
        key: &K,
    ) -> Result<Option<&[u8]>>
    where
        K: AsRef<[u8]>,
    {
        let get_result = self.txn().get(db, key);
        if get_result.is_err() {
            Ok(None)
        } else {
            Ok(Some(get_result.unwrap()))
        }
    }
    fn get_capnp<K: capnp::message::Allocator, V: for<'b> capnp::traits::Owned<'b>>(
        &'a self,
        db: lmdb::Database,
        key: &capnp::message::Builder<K>,
    ) -> Result<Option<MessageReader<'a, V>>> {
        let key_vec = capnp::serialize::write_message_to_words(key);
        let get_result = self.txn().get(db, &key_vec);
        if get_result.is_err() {
            Ok(None)
        } else {
            let mut slice = get_result.unwrap();
            let msg = capnp::serialize::read_message_from_flat_slice(
                &mut slice,
                capnp::message::ReaderOptions::default(),
            )?
            .into_typed::<V>();
            Ok(Some(msg))
        }
    }
}

impl<'a> DBTransaction<'a, lmdb::RwTransaction<'a>> for RwTransaction<'a> {
    fn txn(&'a self) -> &'a lmdb::RwTransaction<'a> {
        &self.txn
    }
}

impl<'a> DBTransaction<'a, lmdb::RoTransaction<'a>> for RoTransaction<'a> {
    fn txn(&'a self) -> &'a lmdb::RoTransaction<'a> {
        &self.txn
    }
}

impl<'a> RwTransaction<'a> {
    pub fn put<K, V: capnp::message::Allocator>(
        &mut self,
        db: lmdb::Database,
        key: &K,
        value: &capnp::message::Builder<V>,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let value_vec = capnp::serialize::write_message_to_words(value);
        self.txn
            .put(db, key, &value_vec, lmdb::WriteFlags::default())?;
        self.dirty = true;
        Ok(())
    }

    pub fn put_capnp<K: capnp::message::Allocator, V: capnp::message::Allocator>(
        &mut self,
        db: lmdb::Database,
        key: &capnp::message::Builder<K>,
        value: &capnp::message::Builder<V>,
    ) -> Result<()> {
        let key_vec = capnp::serialize::write_message_to_words(key);
        let value_vec = capnp::serialize::write_message_to_words(value);
        self.txn
            .put(db, &key_vec, &value_vec, lmdb::WriteFlags::default())?;
        self.dirty = true;
        Ok(())
    }
    pub fn put_bytes<K, V>(
        &mut self,
        db: lmdb::Database,
        key: &K,
        value: &V,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.txn.put(db, key, value, lmdb::WriteFlags::default())?;
        self.dirty = true;
        Ok(())
    }
    pub fn delete<K>(
        &mut self,
        db: lmdb::Database,
        key: &K,
    ) -> Result<bool>
    where
        K: AsRef<[u8]>,
    {
        self.dirty = true;
        match self.txn.del(db, key, Option::None) {
            Err(err) => match err {
                lmdb::Error::NotFound => Ok(false),
                _ => Err(Error::Lmdb(err)),
            },
            Ok(_) => Ok(true),
        }
    }

    pub fn delete_capnp<K: capnp::message::Allocator>(
        &mut self,
        db: lmdb::Database,
        key: &capnp::message::Builder<K>,
    ) -> Result<()> {
        let key_vec = capnp::serialize::write_message_to_words(key);
        self.txn.del(db, &key_vec, Option::None)?;
        self.dirty = true;
        Ok(())
    }

    pub fn clear_db(
        &mut self,
        db: lmdb::Database,
    ) -> Result<()> {
        self.txn.clear_db(db)?;
        Ok(())
    }

    pub fn commit(self) -> Result<()> {
        if self.dirty {
            self.txn.commit()?;
        }
        Ok(())
    }

    pub fn open_rw_cursor(
        &'a mut self,
        db: lmdb::Database,
    ) -> Result<lmdb::RwCursor<'a>> {
        Ok(self.txn.open_rw_cursor(db)?)
    }
}

impl Environment {
    pub fn new(path: &Path) -> Result<Environment> {
        #[cfg(target_pointer_width = "32")]
        let map_size = 1 << 31;
        #[cfg(target_pointer_width = "64")]
        let map_size = 1 << 31;

        Self::with_map_size(path, map_size)
    }

    pub fn with_map_size(
        path: &Path,
        map_size: usize,
    ) -> Result<Environment> {
        // safety notice:
        // - NO_TLS flag is required for RwTransaction Send derive to be safe.
        let flags = lmdb::EnvironmentFlags::NO_TLS;

        #[cfg(not(target_os = "macos"))]
        let flags = flags | lmdb::EnvironmentFlags::WRITE_MAP;

        const MAX_READERS: u32 = 126;

        let env = lmdb::Environment::new()
            .set_max_dbs(64)
            .set_max_readers(MAX_READERS)
            .set_map_size(map_size)
            .set_flags(flags)
            .open(path)?;
        Ok(Environment {
            env,
            read_semaphore: Semaphore::new(MAX_READERS as _),
            write_semaphore: Semaphore::new(1),
        })
    }

    pub fn create_db(
        &self,
        name: Option<&str>,
        flags: lmdb::DatabaseFlags,
    ) -> Result<lmdb::Database> {
        Ok(self.env.create_db(name, flags)?)
    }

    pub async fn rw_txn(&self) -> Result<RwTransaction<'_>> {
        Ok(RwTransaction {
            permit: self.write_semaphore.acquire().await,
            txn: self.env.begin_rw_txn()?,
            dirty: false,
        })
    }

    pub async fn ro_txn(&self) -> Result<RoTransaction<'_>> {
        Ok(RoTransaction {
            permit: self.read_semaphore.acquire().await,
            txn: self.env.begin_ro_txn()?,
        })
    }
}
