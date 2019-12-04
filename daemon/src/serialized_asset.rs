use crate::error::Result;
use atelier_importer::SerdeObj;
use atelier_schema::data::CompressionType;
use bincode;

pub struct SerializedAsset<T: AsRef<[u8]>> {
    pub compression: CompressionType,
    pub uncompressed_size: usize,
    pub type_uuid: [u8; 16],
    pub data: T,
}

impl SerializedAsset<Vec<u8>> {
    pub fn create(
        value: &dyn SerdeObj,
        compression: CompressionType,
        scratch_buf: &mut Vec<u8>,
    ) -> Result<SerializedAsset<Vec<u8>>> {
        let size = bincode::serialized_size(value)? as usize;
        scratch_buf.clear();
        scratch_buf.resize(size, 0);
        bincode::serialize_into(scratch_buf.as_mut_slice(), value)?;
        let asset_buf = {
            #[cfg(feature = "compression")]
            use smush::{encode, Encoding, Quality};
            #[cfg(feature = "compression")]
            let quality = Quality::Maximum;
            match compression {
                CompressionType::None => scratch_buf.clone(),
                #[cfg(feature = "compression")]
                CompressionType::Lz4 => encode(scratch_buf, Encoding::Lz4, quality)?,
                #[cfg(not(feature = "compression"))]
                CompressionType::Lz4 => {
                    panic!("compression not enabled: compile with `compression` feature")
                }
            }
        };

        Ok(SerializedAsset {
            compression,
            uncompressed_size: size,
            type_uuid: value.uuid(),
            data: asset_buf,
        })
    }
}
