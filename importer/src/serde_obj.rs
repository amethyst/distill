use downcast::*;
use erased_serde::*;
use type_uuid::TypeUuidDynamic;

/// A trait for serializing any struct with a TypeUuid
pub trait SerdeObj: Any + Serialize + TypeUuidDynamic + Send {}
impl<T: Serialize + TypeUuidDynamic + Send + 'static> SerdeObj for T {}

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

serialize_trait_object!(SerdeObj);
downcast!(dyn SerdeObj);
