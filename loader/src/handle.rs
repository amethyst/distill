use std::{
    marker::PhantomData,
    sync::{Arc, Weak},
};

/// A handle to an asset. This is usually what the
/// user deals with, the actual asset (`A`) is stored
/// in an `AssetStorage`.
#[derive(derivative::Derivative)]
#[derivative(
    Clone(bound = ""),
    Eq(bound = ""),
    Hash(bound = ""),
    PartialEq(bound = ""),
    Debug(bound = "")
)]
pub struct Handle<A: ?Sized> {
    id: Arc<u32>,
    #[derivative(Debug = "ignore")]
    marker: PhantomData<A>,
}

impl<A> Handle<A> {
    /// Return the 32 bit id of this handle.
    pub fn id(&self) -> u32 {
        *self.id.as_ref()
    }

    /// Downgrades the handle and creates a `WeakHandle`.
    pub fn downgrade(&self) -> WeakHandle<A> {
        let id = Arc::downgrade(&self.id);

        WeakHandle {
            id,
            marker: PhantomData,
        }
    }

    /// Returns `true` if this is the only handle to the asset its pointing at.
    fn is_unique(&self) -> bool {
        Arc::strong_count(&self.id) == 1
    }
}
/// A weak handle, which is useful if you don't directly need the asset
/// like in caches. This way, the asset can still get dropped (if you want that).
#[derive(derivative::Derivative)]
#[derivative(Clone(bound = ""))]
pub struct WeakHandle<A> {
    id: Weak<u32>,
    marker: PhantomData<A>,
}

impl<A> WeakHandle<A> {
    /// Tries to upgrade to a `Handle`.
    #[inline]
    pub fn upgrade(&self) -> Option<Handle<A>> {
        self.id.upgrade().map(|id| Handle {
            id,
            marker: PhantomData,
        })
    }

    /// Returns `true` if the original handle is dead.
    #[inline]
    pub fn is_dead(&self) -> bool {
        self.upgrade().is_none()
    }
}
