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
}
