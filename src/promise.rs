use futures::executor;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};
use std::sync::mpsc::{channel, Receiver, Sender, TryRecvError};

pub struct Promise<A> {
    result: Receiver<A>,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl<A> Promise<A> {
    pub fn new() -> (Self, impl FnOnce(A) -> ()) {
        let (snd, rcv) = channel::<A>();
        let promise: Promise<A> = Promise { result: rcv, waker: Arc::new(Mutex::new(None)) };
        let waker = promise.waker.clone();
        let callback = move |res: A| -> () {
            snd.send(res).expect("Failed to send result to promise");
            if let Some(waker) = waker.lock().unwrap().take() {
                waker.wake();
            }
        };
        (promise, callback)
    }
}

impl<A> Future for Promise<A> {
    type Output = A;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        match self.result.try_recv() {
            Ok(res) => Poll::Ready(res),
            Err(_) => {
                let _ = self.waker.lock().unwrap().insert(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;

    #[test]
    fn test_promise() {
        let (promise, callback) = Promise::<String>::new();
        let handle = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(100));
            callback("Hello, world!".to_string());
        });
        let res = block_on(promise);
        assert_eq!(res, "Hello, world!".to_string());
        handle.join().unwrap();
    }
}

