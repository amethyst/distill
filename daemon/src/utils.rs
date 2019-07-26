use crate::error::Error;
use atelier_importer::AssetUuid;
use std::{
    ffi::OsStr,
    hash::{Hash, Hasher},
    path::PathBuf,
};

pub(crate) fn make_array<A, T>(slice: &[T]) -> A
where
    A: Sized + Default + AsMut<[T]>,
    T: Copy,
{
    let mut a = Default::default();
    <A as AsMut<[T]>>::as_mut(&mut a).copy_from_slice(slice);
    a
}

pub(crate) fn uuid_from_slice(slice: &[u8]) -> Result<uuid::Bytes, Error> {
    const BYTES_LEN: usize = 16;

    let len = slice.len();

    if len != BYTES_LEN {
        return Err(Error::UuidBytesError(uuid::BytesError::new(BYTES_LEN, len)));
    }

    let mut bytes: uuid::Bytes = [0; 16];
    bytes.copy_from_slice(slice);
    Ok(bytes)
}

pub(crate) fn to_meta_path(p: &PathBuf) -> PathBuf {
    p.with_file_name(OsStr::new(
        &(p.file_name().unwrap().to_str().unwrap().to_owned() + ".meta"),
    ))
}

pub(crate) fn calc_asset_hash(id: &AssetUuid, import_hash: u64) -> u64 {
    let mut hasher = ::std::collections::hash_map::DefaultHasher::new();
    import_hash.hash(&mut hasher);
    id.hash(&mut hasher);
    hasher.finish()
}
