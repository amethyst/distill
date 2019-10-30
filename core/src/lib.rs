use proc_macro_hack::proc_macro_hack;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid;

use std::str::FromStr;
use std::{cmp, fmt};

#[proc_macro_hack]
pub use asset_uuid::asset_uuid;
pub mod utils;

/// A universally unique identifier for an asset.
/// An asset can be an instance of any Rust type that implements
/// [type_uuid::TypeUuid] + [serde::Serialize] + [Send].
///
/// If using a human-readable format, serializes to a hyphenated UUID format and deserializes from
/// any format supported by the `uuid` crate. Otherwise, serializes to and from a `[u8; 16]`.
#[derive(PartialEq, Eq, Debug, Clone, Copy, Default, Hash)]
pub struct AssetUuid(pub [u8; 16]);

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

impl PartialOrd for AssetUuid {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl fmt::Display for AssetUuid {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        uuid::Uuid::from_bytes(self.0).fmt(f)
    }
}

impl Serialize for AssetUuid {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_string())
        } else {
            self.0.serialize(serializer)
        }
    }
}

struct AssetUuidVisitor;

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

impl<'de> Deserialize<'de> for AssetUuid {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            deserializer.deserialize_string(AssetUuidVisitor)
        } else {
            Ok(AssetUuid(<[u8; 16]>::deserialize(deserializer)?))
        }
    }
}

/// UUID of an asset's Rust type. Produced by [type_uuid::TypeUuid::UUID].
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

impl Serialize for AssetTypeId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_string())
        } else {
            self.0.serialize(serializer)
        }
    }
}

struct AssetTypeIdVisitor;

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
#[derive(Debug, Hash, PartialEq, Eq, Clone)]
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
        if let AssetRef::Path(_) = self {
            true
        } else {
            false
        }
    }
    pub fn is_uuid(&self) -> bool {
        if let AssetRef::Uuid(_) = self {
            true
        } else {
            false
        }
    }
}
