use std::fmt;
#[cfg(feature = "serde-1")]
use std::str::FromStr;

#[cfg(feature = "serde-1")]
use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
pub use uuid;
use uuid::Uuid;

pub mod importer_context;
pub mod utils;

pub mod distill_signal;

/// A universally unique identifier for an asset.
/// An asset can be a value of any Rust type that implements
/// [`TypeUuidDynamic`] + [serde::Serialize] + [Send].
///
/// If using a human-readable format, serializes to a hyphenated UUID format and deserializes from
/// any format supported by the `uuid` crate. Otherwise, serializes to and from a `[u8; 16]`.
#[derive(PartialEq, Eq, Clone, Copy, Default, Hash, Ord, PartialOrd)]
pub struct AssetUuid(pub [u8; 16]);

impl<S: AsRef<str>> From<S> for AssetUuid {
    fn from(s: S) -> Self {
        AssetUuid(
            *Uuid::parse_str(s.as_ref())
                .expect("Macro input is not a UUID string")
                .as_bytes(),
        )
    }
}

impl AsMut<[u8]> for AssetUuid {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

impl AsRef<[u8]> for AssetUuid {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for AssetUuid {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("AssetUuid")
            .field(&uuid::Uuid::from_bytes(self.0))
            .finish()
    }
}

impl fmt::Display for AssetUuid {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        uuid::Uuid::from_bytes(self.0).fmt(f)
    }
}

#[cfg(feature = "serde-1")]
impl Serialize for AssetUuid {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_string())
        } else {
            self.0.serialize(serializer)
        }
    }
}

#[cfg(feature = "serde-1")]
struct AssetUuidVisitor;

#[cfg(feature = "serde-1")]
impl<'a> Visitor<'a> for AssetUuidVisitor {
    type Value = AssetUuid;

    fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "a UUID-formatted string")
    }

    fn visit_str<E: de::Error>(self, s: &str) -> Result<Self::Value, E> {
        uuid::Uuid::from_str(s)
            .map(|id| AssetUuid(*id.as_bytes()))
            .map_err(|_| de::Error::invalid_value(de::Unexpected::Str(s), &self))
    }
}

#[cfg(feature = "serde-1")]
impl<'de> Deserialize<'de> for AssetUuid {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            deserializer.deserialize_string(AssetUuidVisitor)
        } else {
            Ok(AssetUuid(<[u8; 16]>::deserialize(deserializer)?))
        }
    }
}

/// UUID of an asset's Rust type. Produced by [`TypeUuidDynamic::uuid`].
///
/// If using a human-readable format, serializes to a hyphenated UUID format and deserializes from
/// any format supported by the `uuid` crate. Otherwise, serializes to and from a `[u8; 16]`.
#[derive(PartialEq, Eq, Debug, Clone, Copy, Default, Hash)]
pub struct AssetTypeId(pub [u8; 16]);

impl AsMut<[u8]> for AssetTypeId {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

impl AsRef<[u8]> for AssetTypeId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Display for AssetTypeId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        uuid::Uuid::from_bytes(self.0).fmt(f)
    }
}

#[cfg(feature = "serde-1")]
impl Serialize for AssetTypeId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_string())
        } else {
            self.0.serialize(serializer)
        }
    }
}

#[cfg(feature = "serde-1")]
struct AssetTypeIdVisitor;

#[cfg(feature = "serde-1")]
impl<'a> Visitor<'a> for AssetTypeIdVisitor {
    type Value = AssetTypeId;

    fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "a UUID-formatted string")
    }

    fn visit_str<E: de::Error>(self, s: &str) -> Result<Self::Value, E> {
        uuid::Uuid::parse_str(s)
            .map(|id| AssetTypeId(*id.as_bytes()))
            .map_err(|_| de::Error::invalid_value(de::Unexpected::Str(s), &self))
    }
}

#[cfg(feature = "serde-1")]
impl<'de> Deserialize<'de> for AssetTypeId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            deserializer.deserialize_string(AssetTypeIdVisitor)
        } else {
            Ok(AssetTypeId(<[u8; 16]>::deserialize(deserializer)?))
        }
    }
}

/// A potentially unresolved reference to an asset
#[derive(Debug, Hash, PartialEq, Eq, Clone, Ord, PartialOrd)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum AssetRef {
    Uuid(AssetUuid),
    Path(std::path::PathBuf),
}
impl AssetRef {
    pub fn expect_uuid(&self) -> &AssetUuid {
        if let AssetRef::Uuid(uuid) = self {
            uuid
        } else {
            panic!("Expected AssetRef::Uuid, got {:?}", self)
        }
    }

    pub fn is_path(&self) -> bool {
        matches!(self, AssetRef::Path(_))
    }

    pub fn is_uuid(&self) -> bool {
        matches!(self, AssetRef::Uuid(_))
    }
}

#[derive(Debug, Hash, PartialEq, Copy, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum CompressionType {
    None,
    Lz4,
}

impl Default for CompressionType {
    fn default() -> Self {
        Self::None
    }
}

/// Serializable metadata for an asset.
/// Stored in .meta files and metadata DB.
#[derive(Debug, Clone, Hash, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct AssetMetadata {
    /// UUID for the asset to uniquely identify it
    pub id: AssetUuid,
    /// Search tags are used by asset tooling to search for the imported asset
    pub search_tags: Vec<(String, Option<String>)>,
    /// The referenced build pipeline is invoked when a build artifact is requested for the imported asset
    pub build_pipeline: Option<AssetUuid>,
    /// The latest artifact produced when importing this asset
    pub artifact: Option<ArtifactMetadata>,
}

/// 64-bit hash of the inputs that would produce a given asset artifact
#[derive(Debug, Copy, Clone, Hash, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct ArtifactId(pub u64);

/// Serializable metadata for an artifact.
/// Stored in .meta files and metadata DB.
#[derive(Debug, Clone, Hash, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ArtifactMetadata {
    /// Hash that identifies this artifact
    pub id: ArtifactId,
    /// UUID for this artifact's asset
    pub asset_id: AssetUuid,
    /// Build dependencies will be included in the Builder arguments when building an asset
    pub build_deps: Vec<AssetRef>,
    /// Load dependencies are guaranteed to load before this asset by the Loader
    pub load_deps: Vec<AssetRef>,
    /// Type of compression used to compress this artifact
    pub compression: CompressionType,
    /// Size of this artifact in bytes when compressed
    pub compressed_size: Option<u64>,
    /// Size of this artifact in bytes when serialized and uncompressed
    pub uncompressed_size: Option<u64>,
    /// The UUID of the artifact's Rust type
    pub type_id: AssetTypeId,
}

/// Provides a unique 16-byte ID for a value's type.
pub trait TypeUuidDynamic {
    fn uuid(&self) -> [u8; 16];
}

#[cfg(feature = "type_uuid")]
impl<T: type_uuid::TypeUuidDynamic> TypeUuidDynamic for T {
    fn uuid(&self) -> [u8; 16] {
        <Self as type_uuid::TypeUuidDynamic>::uuid(self)
    }
}

#[cfg(feature = "type_uuid")]
pub use type_uuid;
