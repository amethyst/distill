use proc_macro_hack::proc_macro_hack;
use serde::{Serialize, Deserialize, Serializer, Deserializer};
use serde::de::{self, Visitor};
use uuid;

use std::{cmp, fmt};
use std::str::FromStr;

#[proc_macro_hack]
pub use asset_uuid::asset_uuid;
pub mod utils;

/// A universally unique identifier for an asset.
/// An asset can be an instance of any Rust type that implements
/// [type_uuid::TypeUuid] + [serde::Serialize] + [Send].
///
/// Serializes to a hyphenated UUID format and deserializes from any format supported by the `uuid`
/// crate.
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

impl Serialize for AssetUuid {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&uuid::Uuid::from_bytes(self.0).to_string())
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
        deserializer.deserialize_string(AssetUuidVisitor)
    }
}

/// UUID of an asset's Rust type. Produced by [type_uuid::TypeUuid::UUID].
///
/// Serializes to a hyphenated UUID format and deserializes from any format supported by the `uuid`
/// crate.
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

impl Serialize for AssetTypeId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&uuid::Uuid::from_bytes(self.0).to_string())
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
        deserializer.deserialize_string(AssetTypeIdVisitor)
    }
}
