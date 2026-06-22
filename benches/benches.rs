use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::thread;
use std::time::{Duration, Instant};

use lamellae::channel;

// const MESSAGE_COUNT: u64 = 1_000_000_000;
const MESSAGE_COUNT: u64 = 5_000_000;
type Message = u64;

fn bench_lamellae() -> Duration {
    let (mut tx, mut rx) = channel!(Message, 1024);

    let consumer_handle = thread::spawn(move || {
        let mut sum = 0;
        for _ in 0..MESSAGE_COUNT {
            sum += rx.recv();
        }
        sum
    });

    let start = Instant::now();

    for i in 0..MESSAGE_COUNT {
        tx.send(i);
    }

    while tx.flush().is_err() {}

    let elapsed = start.elapsed();
    let sum = consumer_handle.join().unwrap();

    let expected_sum = (MESSAGE_COUNT - 1) * MESSAGE_COUNT / 2;
    assert_eq!(sum, expected_sum);

    elapsed
}

fn bench_std() -> Duration {
    let (tx, rx) = std::sync::mpsc::sync_channel::<Message>(1024);

    let consumer_handle = thread::spawn(move || {
        let mut sum = 0;
        for _ in 0..MESSAGE_COUNT {
            sum += rx.recv().unwrap();
        }
        sum
    });

    let start = Instant::now();

    for i in 0..MESSAGE_COUNT {
        tx.send(i).unwrap();
    }

    let elapsed = start.elapsed();
    let sum = consumer_handle.join().unwrap();

    let expected_sum = (MESSAGE_COUNT - 1) * MESSAGE_COUNT / 2;
    assert_eq!(sum, expected_sum);

    elapsed
}

fn bench_rtrb() -> Duration {
    let (mut tx, mut rx) = rtrb::RingBuffer::<Message>::new(1024);

    let consumer_handle = thread::spawn(move || {
        let mut sum = 0;
        for _ in 0..MESSAGE_COUNT {
            let msg = loop {
                if let Ok(m) = rx.pop() {
                    break m;
                }
                std::thread::yield_now();
            };
            sum += msg;
        }
        sum
    });

    let start = Instant::now();

    for i in 0..MESSAGE_COUNT {
        while tx.push(i).is_err() {
            std::thread::yield_now();
        }
    }

    let sum = consumer_handle.join().unwrap();
    let elapsed = start.elapsed();

    let expected_sum = (MESSAGE_COUNT - 1) * MESSAGE_COUNT / 2;
    assert_eq!(sum, expected_sum);

    elapsed
}

fn criterion_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("Channels");
    group.measurement_time(Duration::from_secs(15));
    group.sample_size(20);

    let total_bytes = MESSAGE_COUNT * std::mem::size_of::<Message>() as u64;
    group.throughput(Throughput::Bytes(total_bytes));

    group.bench_function("Lamellae SPSC", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                total_duration += bench_lamellae();
            }
            total_duration
        });
    });

    group.bench_function("std MPSC sync", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                total_duration += bench_std();
            }
            total_duration
        });
    });

    group.bench_function("rtrb SPSC", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                total_duration += bench_rtrb();
            }
            total_duration
        });
    });

    group.finish();
}

// fn main() {
//     let elapsed = bench_lamellae();
//     println!("elapsed: {}", elapsed.as_millis());
// }

criterion_group!(benches, criterion_benchmarks);
criterion_main!(benches);
