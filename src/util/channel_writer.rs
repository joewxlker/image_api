#[cfg(feature = "channeled")]
pub mod blocking {
    use std::io;

    pub struct ChannelWriter {
        sender: tokio::sync::mpsc::Sender<Vec<u8>>,
    }

    impl ChannelWriter {
        pub fn new(sender: tokio::sync::mpsc::Sender<Vec<u8>>) -> Self {
            Self { sender }
        }
    }

    impl io::Write for ChannelWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if buf.is_empty() {
                return Ok(0);
            }

            let size = buf.len();

            self.sender
                .blocking_send(buf.to_vec())
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "send failed"))?;

            Ok(size)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[cfg(test)]
    mod test {
        use tokio::sync::mpsc::error::TryRecvError;

        use super::*;
        use std::io::Write;
        use std::time::Duration;

        #[tokio::test]
        async fn write_empty_buffer_returns_zero_and_sends_nothing() {
            let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
            let mut writer = ChannelWriter::new(sender);

            tokio::task::spawn_blocking(move || {
                let written = writer.write(b"").unwrap();

                assert_eq!(written, 0);
                matches!(receiver.try_recv(), Err(TryRecvError::Empty));
            })
            .await
            .unwrap();
        }

        #[tokio::test]
        async fn write_sends_buffer_and_returns_length() {
            let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
            let mut writer = ChannelWriter::new(sender);

            let written = tokio::task::spawn_blocking(move || writer.write(b"hello"))
                .await
                .expect("blocking task panicked")
                .unwrap();

            assert_eq!(written, 5);
            assert_eq!(receiver.recv().await.unwrap(), b"hello");
        }

        #[tokio::test]
        async fn write_copies_buffer_contents() {
            let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
            let mut writer = ChannelWriter::new(sender);

            let mut buf = b"hello".to_vec();

            let written = tokio::task::spawn_blocking(move || {
                let result = writer.write(&buf);
                buf.fill(b'x');
                result
            })
            .await
            .expect("blocking task panicked")
            .unwrap();

            assert_eq!(written, 5);
            assert_eq!(receiver.recv().await.unwrap(), b"hello");
        }

        #[tokio::test]
        async fn multiple_writes_preserve_order() {
            let (sender, mut receiver) = tokio::sync::mpsc::channel(4);
            let mut writer = ChannelWriter::new(sender);

            tokio::task::spawn_blocking(move || {
                writer.write_all(b"one")?;
                writer.write_all(b"two")?;
                writer.write_all(b"three")?;

                Ok::<(), std::io::Error>(())
            })
            .await
            .expect("blocking task panicked")
            .unwrap();

            assert_eq!(receiver.recv().await.unwrap(), b"one");
            assert_eq!(receiver.recv().await.unwrap(), b"two");
            assert_eq!(receiver.recv().await.unwrap(), b"three");
            assert!(receiver.recv().await.is_none());
        }

        #[tokio::test]
        async fn write_returns_broken_pipe_when_receiver_is_dropped() {
            let (sender, _) = tokio::sync::mpsc::channel(1);

            let mut writer = ChannelWriter::new(sender);

            let error = tokio::task::spawn_blocking(move || writer.write_all(b"hello"))
                .await
                .expect("blocking task panicked")
                .unwrap_err();

            assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
        }

        #[tokio::test]
        async fn flush_is_noop() {
            let (sender, _receiver) = tokio::sync::mpsc::channel(1);
            let mut writer = ChannelWriter::new(sender);

            tokio::task::spawn_blocking(move || writer.flush())
                .await
                .expect("blocking task panicked")
                .unwrap();
        }

        #[tokio::test]
        async fn write_blocks_until_capacity_is_available() {
            let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
            let mut writer = ChannelWriter::new(sender);

            let handle = tokio::task::spawn_blocking(move || {
                writer.write_all(b"first")?;
                writer.write_all(b"second")?;

                Ok::<(), std::io::Error>(())
            });

            // wait for first write to fill the buffer and second write to block
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(!handle.is_finished());

            assert_eq!(receiver.recv().await.unwrap(), b"first");

            handle.await.expect("blocking task panicked").unwrap();

            assert_eq!(receiver.recv().await.unwrap(), b"second");
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

    #[cfg(test)]
    mod test {
        use rocket::futures::AsyncWriteExt;
        use tokio::sync::mpsc::error::TryRecvError;

        use super::*;
        use std::time::Duration;

        #[tokio::test]
        async fn write_empty_buffer_returns_zero_and_sends_nothing() {
            let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
            let mut writer = ChannelWriter::new(sender);

            tokio::task::spawn(async move {
                let written = writer.write(b"").await.unwrap();

                assert_eq!(written, 0);
                matches!(receiver.try_recv(), Err(TryRecvError::Empty));
            })
            .await
            .unwrap();
        }

        #[tokio::test]
        async fn write_sends_buffer_and_returns_length() {
            let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
            let mut writer = ChannelWriter::new(sender);

            let written = tokio::task::spawn(async move { writer.write(b"hello").await })
                .await
                .expect("blocking task panicked")
                .unwrap();

            assert_eq!(written, 5);
            assert_eq!(receiver.recv().await.unwrap(), b"hello");
        }

        #[tokio::test]
        async fn write_copies_buffer_contents() {
            let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
            let mut writer = ChannelWriter::new(sender);

            let mut buf = b"hello".to_vec();

            let written = tokio::task::spawn(async move {
                let result = writer.write(&buf).await;
                buf.fill(b'x');
                result
            })
            .await
            .unwrap()
            .unwrap();

            assert_eq!(written, 5);
            assert_eq!(receiver.recv().await.unwrap(), b"hello");
        }

        #[tokio::test]
        async fn multiple_writes_preserve_order() {
            let (sender, mut receiver) = tokio::sync::mpsc::channel(4);
            let mut writer = ChannelWriter::new(sender);

            tokio::task::spawn(async move {
                writer.write_all(b"one").await?;
                writer.write_all(b"two").await?;
                writer.write_all(b"three").await?;

                Ok::<(), std::io::Error>(())
            })
            .await
            .expect("blocking task panicked")
            .unwrap();

            assert_eq!(receiver.recv().await.unwrap(), b"one");
            assert_eq!(receiver.recv().await.unwrap(), b"two");
            assert_eq!(receiver.recv().await.unwrap(), b"three");
            assert!(receiver.recv().await.is_none());
        }

        #[tokio::test]
        async fn write_returns_broken_pipe_when_receiver_is_dropped() {
            let (sender, _) = tokio::sync::mpsc::channel(1);

            let mut writer = ChannelWriter::new(sender);

            let error = tokio::task::spawn(async move { writer.write_all(b"hello").await })
                .await
                .expect("blocking task panicked")
                .unwrap_err();

            assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
        }

        #[tokio::test]
        async fn flush_is_noop() {
            let (sender, _receiver) = tokio::sync::mpsc::channel(1);
            let mut writer = ChannelWriter::new(sender);

            tokio::task::spawn(async move { writer.flush().await })
                .await
                .expect("blocking task panicked")
                .unwrap();
        }

        #[tokio::test]
        async fn write_blocks_until_capacity_is_available() {
            let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
            let mut writer = ChannelWriter::new(sender);

            let handle = tokio::task::spawn(async move {
                writer.write_all(b"first").await?;
                writer.write_all(b"second").await?;

                Ok::<(), std::io::Error>(())
            });

            // wait for first write to fill the buffer and second write to block
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(!handle.is_finished());

            assert_eq!(receiver.recv().await.unwrap(), b"first");

            handle.await.expect("blocking task panicked").unwrap();

            assert_eq!(receiver.recv().await.unwrap(), b"second");
        }
    }
}
