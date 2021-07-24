use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Thin wrapper around `futures_channel::oneshot` to match `tokio::sync::oneshot` interface.
pub fn oneshot<T>() -> (Sender<T>, Receiver<T>) {
    let (sender, receiver) = futures_channel::oneshot::channel();
    (Sender::new(sender), Receiver::new(receiver))
}

#[derive(Debug)]
pub struct Receiver<T> {
    inner: futures_channel::oneshot::Receiver<T>,
}

impl<T> Receiver<T> {
    #[inline(always)]
    pub(crate) fn new(inner: futures_channel::oneshot::Receiver<T>) -> Self {
        Receiver { inner }
    }

    #[inline]
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        match self.inner.try_recv() {
            Ok(Some(x)) => Ok(x),
            Ok(None) => Err(TryRecvError::Empty),
            Err(_canceled) => Err(TryRecvError::Closed),
        }
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<T, RecvError>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<T, RecvError>> {
        match self.try_recv() {
            Ok(value) => Poll::Ready(Ok(value)),
            Err(TryRecvError::Closed) => Poll::Ready(Err(RecvError { 0: () })),
            Err(TryRecvError::Empty) => Poll::Pending,
        }
    }
}

#[derive(Debug)]
pub struct Sender<T> {
    inner: futures_channel::oneshot::Sender<T>,
}

impl<T> Sender<T> {
    #[inline(always)]
    pub(crate) fn new(inner: futures_channel::oneshot::Sender<T>) -> Self {
        Sender { inner }
    }

    #[inline]
    pub fn send(self, value: T) -> Result<(), RecvError> {
        match self.inner.send(value) {
            Ok(_) => Ok(()),
            Err(_) => Err(RecvError { 0: () }),
        }
    }
}

use self::error::*;
pub mod error {
    use std::fmt;

    #[derive(Debug, Eq, PartialEq)]
    pub struct RecvError(pub(super) ());

    #[derive(Debug, Eq, PartialEq)]
    pub enum TryRecvError {
        Empty,
        Closed,
    }

    impl fmt::Display for TryRecvError {
        fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                TryRecvError::Empty => write!(fmt, "channel empty"),
                TryRecvError::Closed => write!(fmt, "channel closed"),
            }
        }
    }

    impl std::error::Error for TryRecvError {}
}
