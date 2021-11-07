//! `AsyncRead` and `AsyncWrite` implementations on top of `tungestenite`-websockets

use std::io;

use async_tungstenite::tungstenite::{Error, Message};
use futures::{future, Sink};
use futures::{AsyncRead, AsyncWrite, SinkExt, StreamExt, TryStreamExt};
use std::{pin::Pin, task::Poll};

pub async fn accept_websocket_stream<S>(
    stream: S,
) -> Result<(impl AsyncRead, impl AsyncWrite), Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let ws_stream = async_tungstenite::accept_async(stream).await?;

    let (sink, stream) = ws_stream.split();

    let stream = stream
        .and_then(|message| async {
            match message {
                Message::Binary(bytes) => Ok(bytes),
                Message::Close(_) => Err(Error::ConnectionClosed),
                other => Err(Error::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!(
                        "tungstenite-async-io can only handle binary messages, got {:?}",
                        other
                    ),
                ))),
            }
        })
        .take_while(|res| match res {
            Err(Error::ConnectionClosed) => future::ready(false),
            _ => future::ready(true),
        })
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e));

    let stream = Box::pin(stream);
    let async_read = stream.into_async_read();

    let sink = sink.with(|data: Vec<u8>| async { Ok::<_, Error>(Message::Binary(data)) });
    let sink = Box::pin(sink);
    let async_write = IntoAsyncWrite::new(sink);

    Ok((async_read, async_write))
}

struct IntoAsyncWrite<S: Sink<Vec<u8>> + Unpin> {
    sink: S,
    buffer: Vec<u8>,
}

impl<S: Sink<Vec<u8>> + Unpin> IntoAsyncWrite<S> {
    pub fn new(sink: S) -> Self {
        IntoAsyncWrite {
            sink,
            buffer: Vec::new(),
        }
    }
}

impl<S> AsyncWrite for IntoAsyncWrite<S>
where
    S: Sink<Vec<u8>> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        self.get_mut().buffer.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        let map_err = |e| io::Error::new(io::ErrorKind::Other, e);

        let sink = Pin::new(&mut self.sink);
        match sink.poll_ready(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => return Poll::Ready(Err(map_err(e))),
            Poll::Pending => return Poll::Pending,
        }

        let buffer = std::mem::take(&mut self.buffer);
        let sink = Pin::new(&mut self.sink);
        sink.start_send(buffer).map_err(map_err)?;

        let sink = Pin::new(&mut self.sink);
        sink.poll_flush(cx).map_err(map_err)
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        let map_err = |e| io::Error::new(io::ErrorKind::Other, e);

        Pin::new(&mut self.sink).poll_close(cx).map_err(map_err)
    }
}
