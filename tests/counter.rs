use futures::future::join_all;
use batcher::batcher::{Batched, BatchedOp, Batcher, WrappedOp};
use batcher::utils;

#[derive(Debug)]
struct Counter {
    value: i32,
}

#[derive(Debug)]
enum CounterOp { Get, Incr }

impl BatchedOp for CounterOp { type Res = Option<i32>; }

impl Batched for Counter {
    type Op = CounterOp;

    fn init() -> Self {
        Counter { value: 0 }
    }

    async fn run_batch(&mut self, ops: Vec<WrappedOp<CounterOp>>) {
        let vl = self.value;
        fn reduce(l: i32, r: i32) -> i32 { l + r }
        let map = move |op: WrappedOp<CounterOp>| -> i32 {
            match op.0 {
                CounterOp::Get => {
                    op.1(Some(vl));
                    0
                }
                CounterOp::Incr => {
                    op.1(None);
                    1
                }
            }
        };
        let res = utils::parallel_reduce(ops, reduce, map).await;
        self.value += res;
    }
}

#[tokio::test]
async fn test_counter() {
    let counter: Batcher<Counter> = Batcher::new();

    let tasks: Vec<_> = (1..1000).map(|i| {
        let counter_local = counter.clone();
        tokio::spawn(async move {
            if i % 10 == 0 {
                let res = counter_local.apply(CounterOp::Get).await;
                println!("[task {}]: result of getting counter was {:?}", i, res)
            } else {
                let res = counter_local.apply(CounterOp::Incr).await;
                println!("[task {}]: result of incr counter was {:?}", i, res)
            }
        })
    }).collect();

    let _ = join_all(tasks).await;
    println!("final counter value: {:?}", counter.apply(CounterOp::Get).await);
}
