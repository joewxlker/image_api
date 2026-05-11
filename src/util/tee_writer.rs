#[cfg(feature = "channeled")]
pub mod non_blocking {
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    use rocket::futures;

    use tokio::io;

    struct State {
        buf: Vec<u8>,
        a_pos: usize,
        b_pos: usize,
    }

    #[pin_project::pin_project]
    pub struct TeeWriter<A, B> {
        #[pin]
        a: A,
        #[pin]
        b: B,
        state: Option<State>,
    }

    impl<A: futures::AsyncWrite + Unpin, B: futures::AsyncWrite + Unpin> TeeWriter<A, B> {
        pub fn new(a: A, b: B) -> Self {
            Self { a, b, state: None }
        }
    }

    impl<A: futures::AsyncWrite + Unpin, B: futures::AsyncWrite + Unpin> futures::AsyncWrite
        for TeeWriter<A, B>
    {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            if buf.is_empty() {
                return Poll::Ready(Ok(0));
            }

            let this = self.project();

            if this.state.is_none() {
                *this.state = Some(State {
                    buf: buf.to_vec(),
                    a_pos: 0,
                    b_pos: 0,
                });
            }

            let state = this.state.as_mut().unwrap();

            if state.a_pos < state.buf.len() {
                match this.a.poll_write(cx, &state.buf[state.a_pos..]) {
                    Poll::Ready(Ok(0)) => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "failed to write to tee target",
                        )));
                    }
                    Poll::Ready(Ok(n)) => state.a_pos += n,
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    _ => (),
                };
            }

            if state.b_pos < state.buf.len() {
                match this.b.poll_write(cx, &state.buf[state.b_pos..]) {
                    Poll::Ready(Ok(0)) => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "failed to write to tee target",
                        )));
                    }
                    Poll::Ready(Ok(n)) => state.b_pos += n,
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    _ => (),
                };
            }

            if state.b_pos == state.buf.len() && state.a_pos == state.buf.len() {
                let len = state.buf.len();
                *this.state = None;
                return Poll::Ready(Ok(len));
            }

            Poll::Pending
        }
        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            let this = self.project();

            match (this.a.poll_flush(cx), this.b.poll_flush(cx)) {
                (Poll::Ready(Ok(())), Poll::Ready(Ok(()))) => Poll::Ready(Ok(())),
                (Poll::Ready(Err(e)), _) | (_, Poll::Ready(Err(e))) => Poll::Ready(Err(e)),
                (Poll::Pending, _) | (_, Poll::Pending) => Poll::Pending,
            }
        }

        fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            let this = self.project();

            match (this.a.poll_close(cx), this.b.poll_close(cx)) {
                (Poll::Ready(Ok(())), Poll::Ready(Ok(()))) => Poll::Ready(Ok(())),
                (Poll::Ready(Err(e)), _) | (_, Poll::Ready(Err(e))) => Poll::Ready(Err(e)),
                (Poll::Pending, _) | (_, Poll::Pending) => Poll::Pending,
            }
        }
    }

    #[cfg(test)]
    mod test {
        use std::io::ErrorKind;

        use pin_project::pin_project;
        use rocket::futures::{AsyncWrite, AsyncWriteExt, io::BufWriter};

        use super::*;

        #[pin_project]
        struct MockedBackPressureWriter<W: AsyncWrite + Unpin> {
            #[pin]
            inner: W,
            write_count: usize,
            close_count: usize,
            flush_count: usize,
        }

        impl<W: AsyncWrite + Unpin> MockedBackPressureWriter<W> {
            pub fn new(inner: W) -> Self {
                Self {
                    write_count: 0,
                    flush_count: 0,
                    close_count: 0,
                    inner,
                }
            }
        }

        impl<W: AsyncWrite + Unpin> AsyncWrite for MockedBackPressureWriter<W> {
            fn poll_write(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<std::io::Result<usize>> {
                let this = self.project();
                *this.write_count += 1;

                // simulate backpressure for first four write attempts
                if *this.write_count < 5 {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    this.inner.poll_write(cx, buf)
                }
            }
            fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
                let this = self.project();
                *this.close_count += 1;

                // simulate backpressure for first four write attempts
                if *this.close_count < 5 {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    this.inner.poll_close(cx)
                }
            }
            fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
                let this = self.project();
                *this.flush_count += 1;

                // simulate backpressure for first four write attempts
                if *this.flush_count < 5 {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    this.inner.poll_flush(cx)
                }
            }
        }

        #[pin_project]
        struct MockedErrorWriter<W: AsyncWrite + Unpin> {
            #[pin]
            inner: W,
        }

        impl<W: AsyncWrite + Unpin> MockedErrorWriter<W> {
            pub fn new(inner: W) -> Self {
                Self { inner }
            }
        }

        impl<W: AsyncWrite + Unpin> AsyncWrite for MockedErrorWriter<W> {
            fn poll_write(
                self: Pin<&mut Self>,
                _: &mut Context<'_>,
                _: &[u8],
            ) -> Poll<std::io::Result<usize>> {
                Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "mock_error",
                )))
            }
            fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
                Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "mock_error",
                )))
            }
            fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
                Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "mock_error",
                )))
            }
        }

        // ===================== WRITE TESTS =======================

        #[tokio::test]
        async fn write_to_backpressured_writers_succeeds() {
            let mut a = vec![];
            let mut with_backpressure_a = MockedBackPressureWriter::new(&mut a);
            let mut b = vec![];
            let mut with_backpressure_b = MockedBackPressureWriter::new(&mut b);

            let mut writer = TeeWriter::new(&mut with_backpressure_a, &mut with_backpressure_b);

            let buf = [0; 100];

            let written = writer.write(&buf).await.unwrap();

            assert_eq!(written, 100);
            assert_eq!(a, b);
            assert_eq!(b, buf);
        }

        #[tokio::test]
        async fn empty_write_returns_zero_and_sends_nothing() {
            let mut a = vec![];
            let mut b = vec![];

            let mut writer = TeeWriter::new(&mut a, &mut b);

            let written = writer.write(b"").await.unwrap();

            assert_eq!(written, 0);
            assert!(a.is_empty());
            assert!(b.is_empty());
        }

        #[tokio::test]
        async fn write_sends_buffer_and_returns_length() {
            let mut a = vec![];
            let mut b = vec![];

            let mut writer = TeeWriter::new(&mut a, &mut b);

            let written = writer.write(b"hello").await.unwrap();

            assert_eq!(written, 5);
            assert_eq!(a, b"hello");
            assert_eq!(b, b"hello");
        }

        #[tokio::test]
        async fn write_copies_buffer_contents() {
            let mut a = vec![];
            let mut b = vec![];

            let mut writer = TeeWriter::new(&mut a, &mut b);

            let mut buf = b"hello".to_vec();
            let written = writer.write(&buf).await.unwrap();
            buf.fill(b'x');

            assert_eq!(written, 5);
            assert_eq!(a, b"hello");
            assert_eq!(b, b"hello");
        }

        #[tokio::test]
        async fn multiple_writes_preserve_order() {
            let mut a = vec![];
            let mut b = vec![];

            let mut writer = TeeWriter::new(&mut a, &mut b);

            let mut written = writer.write(b"one").await.unwrap();
            written += writer.write(b"two").await.unwrap();
            written += writer.write(b"three").await.unwrap();

            assert_eq!(written, 11);

            assert_eq!(a[0..3], *b"one");
            assert_eq!(a[3..6], *b"two");
            assert_eq!(a[6..11], *b"three");

            assert_eq!(a, b);
            assert_eq!(a, b);
            assert_eq!(a, b);
        }

        #[tokio::test]
        async fn failed_write_to_first_writer_propagates_error() {
            let mut a = vec![];
            let mut b = vec![];

            let mut error_writer = MockedErrorWriter::new(&mut a);

            let mut writer = TeeWriter::new(&mut error_writer, &mut b);

            let error = writer.write(b"hello").await.unwrap_err();

            assert!(matches!(error.kind(), ErrorKind::Other));
        }

        #[tokio::test]
        async fn failed_write_to_second_writer_propagates_error() {
            let mut a = vec![];
            let mut b = vec![];

            let mut error_writer = MockedErrorWriter::new(&mut a);

            let mut writer = TeeWriter::new(&mut b, &mut error_writer);

            let error = writer.write(b"hello").await.unwrap_err();

            assert!(matches!(error.kind(), ErrorKind::Other));
        }

        // ===================== FLUSH TESTS =======================

        #[tokio::test]
        async fn empty_flush_is_noop() {
            let mut a = vec![];
            let mut b = vec![];

            let mut writer = TeeWriter::new(&mut a, &mut b);

            writer.flush().await.unwrap();

            assert!(a.is_empty());
            assert!(b.is_empty());
        }

        #[tokio::test]
        async fn flush_flushes_inner_writers() {
            let mut a = vec![];
            let mut b = vec![];
            let mut buf_writer = BufWriter::with_capacity(64, &mut b);

            let mut writer = TeeWriter::new(&mut a, &mut buf_writer);

            writer.write_all(b"hello").await.unwrap();

            writer.flush().await.unwrap();

            assert_eq!(a, b"hello");
            assert_eq!(b, b"hello");
        }

        #[tokio::test]
        async fn flush_flushes_inner_writers_with_backpressure() {
            let mut a = vec![];
            let mut b = vec![];
            let mut buf_writer = BufWriter::with_capacity(64, &mut b);
            let mut with_backpressure = MockedBackPressureWriter::new(&mut buf_writer);

            let mut writer = TeeWriter::new(&mut a, &mut with_backpressure);

            writer.write_all(b"hello").await.unwrap();

            writer.flush().await.unwrap();

            assert_eq!(a, b"hello");
            assert_eq!(b, b"hello");
        }

        #[tokio::test]
        async fn failed_flush_to_first_writer_propagates_error() {
            let mut a = vec![];
            let mut b = vec![];

            let mut error_writer = MockedErrorWriter::new(&mut a);

            let mut writer = TeeWriter::new(&mut error_writer, &mut b);

            let error = writer.flush().await.unwrap_err();

            assert!(matches!(error.kind(), ErrorKind::Other));
        }

        #[tokio::test]
        async fn failed_flush_to_second_writer_propagates_error() {
            let mut a = vec![];
            let mut b = vec![];

            let mut error_writer = MockedErrorWriter::new(&mut a);

            let mut writer = TeeWriter::new(&mut b, &mut error_writer);

            let error = writer.flush().await.unwrap_err();

            assert!(matches!(error.kind(), ErrorKind::Other));
        }

        // ===================== CLOSE TESTS =======================
    }
}
