#[cfg(feature = "channeled")]
pub mod blocking {
    use std::io;

    pub struct ChannelWriter<T> {
        sender: T,
    }

    impl<T> ChannelWriter<T> {
        pub fn new(sender: T) -> Self {
            Self { sender }
        }
    }

    impl io::Write for ChannelWriter<tokio::sync::mpsc::Sender<Vec<u8>>> {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let size = buf.len();

            self.sender
                .blocking_send(buf.to_vec())
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "send failed"))?;

            Ok(size)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl io::Write for ChannelWriter<std::sync::mpsc::Sender<Vec<u8>>> {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let size = buf.len();

            self.sender
                .send(buf.to_vec())
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "send failed"))?;

            Ok(size)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
}

#[cfg(feature = "channeled")]
pub mod non_blocking {
    use rocket::futures::{AsyncWrite, FutureExt};
    use std::{io, pin::Pin, task::Context, task::Poll};
    use tokio::sync::mpsc::{self, error::SendError};

    pub struct State {
        size: usize,
        future: Pin<Box<dyn Future<Output = Result<(), SendError<Vec<u8>>>> + Send>>,
    }

    #[pin_project::pin_project]
    pub struct ChannelWriter {
        sender: Option<mpsc::Sender<Vec<u8>>>,
        state: Option<State>,
    }

    impl ChannelWriter {
        pub fn new(sender: mpsc::Sender<Vec<u8>>) -> Self {
            Self {
                sender: Some(sender),
                state: None,
            }
        }
    }

    impl AsyncWrite for ChannelWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            if buf.is_empty() {
                return Poll::Ready(Ok(0));
            }

            let this = self.project();

            if this.state.is_none() {
                let Some(sender) = this.sender.clone() else {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "channel writer sender has been closed",
                    )));
                };

                let data = buf.to_vec();

                *this.state = Some(State {
                    size: data.len(),
                    future: Box::pin(async move { sender.send(data).await }),
                });
            }

            let state = this.state.as_mut().unwrap();

            match state.future.poll_unpin(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(_)) => Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "send failed",
                ))),
                Poll::Ready(Ok(_)) => {
                    let size = state.size;
                    *this.state = None;

                    Poll::Ready(Ok(size))
                }
            }
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_close(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            let project = self.project();

            project.state.take();
            project.sender.take();

            Poll::Ready(Ok(()))
        }
    }
}
