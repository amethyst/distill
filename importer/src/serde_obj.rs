use atelier_core::TypeUuidDynamic;
use erased_serde::*;
use std::any::Any;

/// A trait for serializing any struct with a TypeUuid
pub trait SerdeObj: Any + Serialize + TypeUuidDynamic + Send {
    fn any(&self) -> &dyn Any;
    fn any_mut(&mut self) -> &mut dyn Any;
}
impl<T: Serialize + TypeUuidDynamic + Send + 'static> SerdeObj for T {
    fn any(&self) -> &dyn Any {
        self
    }
    fn any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub trait IntoSerdeObj {
    fn into_serde_obj(self: Box<Self>) -> Box<dyn SerdeObj>
    where
        Self: 'static;
}

impl<T: SerdeObj> IntoSerdeObj for T {
    fn into_serde_obj(self: Box<Self>) -> Box<dyn SerdeObj>
    where
        Self: 'static,
    {
        self
    }
}

#[cfg(feature = "serde_importers")]
#[typetag::serde]
pub trait SerdeImportable: SerdeObj + IntoSerdeObj {}

#[cfg(feature = "serde_importers")]
#[doc(hidden)]
pub use serde_importable_derive::*;

#[doc(hidden)]
#[cfg(feature = "serde_importers")]
pub use typetag;

serialize_trait_object!(SerdeObj);
