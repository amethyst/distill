use futures_core::future::{BoxFuture, Future};
use futures_util::future::FutureExt;
use futures_util::stream::{FuturesUnordered, Stream};
use pin_project::{pin_project, pinned_drop};
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::task::JoinHandle;

/// A scope to allow controlled spawning of non 'static
/// futures. Futures can be spawned using `spawn` or
/// `spawn_cancellable` methods.
///
/// # Safety
///
/// This type uses `Drop` implementation to guarantee
/// safety. It is not safe to forget this object unless it
/// is driven to completion.
#[pin_project(PinnedDrop)]
pub struct Scope<'a, T> {
    done: bool,
    len: usize,
    remaining: usize,
    #[pin]
    futs: FuturesUnordered<JoinHandle<T>>,

    // Future proof against variance changes
    _marker: PhantomData<&'a mut &'a ()>,
}

impl<'a, T: Send + 'static> Scope<'a, T> {
    /// Create a Scope object.
    ///
    /// This function is unsafe as `futs` may hold futures
    /// which have to be manually driven to completion.
    pub unsafe fn create() -> Self {
        Scope {
            done: false,
            len: 0,
            remaining: 0,
            futs: FuturesUnordered::new(),
            _marker: PhantomData,
        }
    }

    /// Spawn a future with `async_std::task::spawn`. The
    /// future is expected to be driven to completion before
    /// 'a expires.
    pub fn spawn<F: Future<Output = T> + Send + 'a>(&mut self, f: F) {
        let handle =
            tokio::spawn(unsafe { std::mem::transmute::<_, BoxFuture<'static, T>>(f.boxed()) });
        self.futs.push(handle);
        self.len += 1;
        self.remaining += 1;
    }
}

impl<'a, T> Scope<'a, T> {
    /// Total number of futures spawned in this scope.
    #[inline]
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Number of futures remaining in this scope.
    #[inline]
    #[allow(dead_code)]
    pub fn remaining(&self) -> usize {
        self.remaining
    }

    /// A slighly optimized `collect` on the stream. Also
    /// useful when we can not move out of self.
    pub async fn collect(&mut self) -> Vec<T> {
        let mut proc_outputs = Vec::with_capacity(self.remaining);

        use futures_util::stream::StreamExt;
        while let Some(item) = self.next().await {
            proc_outputs.push(item);
        }

        proc_outputs
    }
}

impl<'a, T> Stream for Scope<'a, T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let poll = this.futs.poll_next(cx);
        if let Poll::Ready(None) = poll {
            *this.done = true;
        } else if poll.is_ready() {
            *this.remaining -= 1;
        }
        poll.map(|t| t.map(|t| t.expect("task not driven to completion")))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

#[pinned_drop]
impl<T> PinnedDrop for Scope<T> {
    fn drop(mut self: Pin<&mut Self>) {
        if !self.done {
            futures_executor::block_on(async {
                // self.cancel().await;
                self.collect().await;
            });
        }
    }
}

/// Creates a `Scope` to spawn non-'static futures. The
/// function is called with a block which takes an `&mut
/// Scope`. The `spawn` method on this arg. can be used to
/// spawn "local" futures.
///
/// # Returns
///
/// The function returns the created `Scope`, and the return
/// value of the block passed to it. The returned stream and
/// is expected to be driven completely before being
/// forgotten. Dropping this stream causes the stream to be
/// driven _while blocking the current thread_. The values
/// returned from the stream are the output of the futures
/// spawned.
///
/// # Safety
///
/// The returned stream is expected to be run to completion
/// before being forgotten. Dropping it is okay, but blocks
/// the current thread until all spawned futures complete.
pub unsafe fn scope<'a, T: Send + 'static, R, F: FnOnce(&mut Scope<'a, T>) -> R>(
    f: F,
) -> (Scope<'a, T>, R) {
    let mut scope = Scope::create();
    let op = f(&mut scope);
    (scope, op)
}

/// A function that creates a scope and immediately awaits,
/// _blocking the current thread_ for spawned futures to
/// complete. The outputs of the futures are collected as a
/// `Vec` and returned along with the output of the block.
///
/// # Safety
///
/// This function is safe to the best of our understanding
/// as it blocks the current thread until the stream is
/// driven to completion, implying that all the spawned
/// futures have completed too. However, care must be taken
/// to ensure a recursive usage of this function doesn't
/// lead to deadlocks.
///
/// When scope is used recursively, you may also use the
/// unsafe `scope_and_*` functions as long as this function
/// is used at the top level. In this case, either the
/// recursively spawned should have the same lifetime as the
/// top-level scope, or there should not be any spurious
/// future cancellations within the top level scope.
#[allow(dead_code)]
pub fn scope_and_block<'a, T: Send + 'static, R, F: FnOnce(&mut Scope<'a, T>) -> R>(
    f: F,
) -> (R, Vec<T>) {
    let (mut stream, block_output) = unsafe { scope(f) };
    let proc_outputs = futures_executor::block_on(stream.collect());
    (block_output, proc_outputs)
}

/// An asynchronous function that creates a scope and
/// immediately awaits the stream. The outputs of the
/// futures are collected as a `Vec` and returned along with
/// the output of the block.
///
/// # Safety
///
/// This function is _not completely safe_: please see
/// `cancellation_soundness` in [tests.rs][tests-src] for a
/// test-case that suggests how this can lead to invalid
/// memory access if not dealt with care.
///
/// The caller must ensure that the lifetime 'a is valid
/// until the returned future is fully driven. Dropping the
/// future is okay, but blocks the current thread until all
/// spawned futures complete.
///
/// [tests-src]: https://github.com/rmanoka/async-scoped/blob/master/src/tests.rs
#[allow(dead_code)]
pub async unsafe fn scope_and_collect<
    'a,
    T: Send + 'static,
    R,
    F: FnOnce(&mut Scope<'a, T>) -> R,
>(
    f: F,
) -> (R, Vec<T>) {
    let (mut stream, block_output) = scope(f);
    let proc_outputs = stream.collect().await;
    (block_output, proc_outputs)
}
