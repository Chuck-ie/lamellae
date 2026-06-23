use std::thread;
use std::time::{Duration, Instant};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use lamellae::channel;

// const BATCH_COUNT: u64 = 25_000_000;
// const BATCH_SIZE: usize = 128;
const BATCH_COUNT: u64 = 40_000;
const BATCH_SIZE: usize = 128;
// const BATCH_COUNT: u64 = 20_000;
// const BATCH_SIZE: usize = 256;
// const BATCH_COUNT: u64 = 10_000;
// const BATCH_SIZE: usize = 512;
// const CAPACITY: usize = 16384;
const CAPACITY: usize = 4096;

type Message = u64;

fn bench_rtrb_batch() -> Duration {
    let (mut tx, mut rx) = rtrb::RingBuffer::<Message>::new(CAPACITY);
    let start = Instant::now();

    let consumer_handle = thread::spawn(move || {
        let mut sum = 0;
        for _ in 0..BATCH_COUNT {
            let chunk = loop {
                if let Ok(c) = rx.read_chunk(BATCH_SIZE) {
                    break c;
                }
                thread::yield_now();
            };

            for msg in chunk {
                sum += msg;
            }
        }
        sum
    });

    for batch in 0..BATCH_COUNT {
        let mut chunk = loop {
            if let Ok(c) = tx.write_chunk(BATCH_SIZE) {
                break c;
            }
            thread::yield_now();
        };

        let (first_slice, second_slice) = chunk.as_mut_slices();
        let mut val_counter = batch * BATCH_SIZE as u64;

        for val in first_slice {
            *val = val_counter;
            val_counter += 1;
        }
        for val in second_slice {
            *val = val_counter;
            val_counter += 1;
        }

        chunk.commit_all();
    }

    let sum = consumer_handle.join().unwrap();
    let n = BATCH_SIZE as u64 * BATCH_COUNT;
    assert_eq!(sum, (n * (n - 1)) / 2);
    start.elapsed()
}

fn bench_lamellae_batch() -> Duration {
    let (mut tx, mut rx) = channel!(Message, CAPACITY);

    let consumer_handle = thread::spawn(move || {
        let mut sum = 0;
        let mut recv_buf = [0; BATCH_SIZE];

        for _ in 0..BATCH_COUNT {
            while rx.try_recv_batch_exact(&mut recv_buf).is_err() {
                thread::yield_now();
            }

            for msg in &recv_buf {
                sum += msg;
            }
        }
        sum
    });

    let start = Instant::now();

    for batch in 0..BATCH_COUNT {
        let mut local_buf = [0u64; BATCH_SIZE];

        for (j, val) in local_buf.iter_mut().enumerate() {
            *val = batch * BATCH_SIZE as u64 + j as u64;
        }

        {
            while tx.try_send_batch_exact(&local_buf).is_err() {
                thread::yield_now();
            }
        }
    }

    while tx.flush().is_err() {}
    let sum = consumer_handle.join().unwrap();
    let n = BATCH_SIZE as u64 * BATCH_COUNT;
    assert_eq!(sum, (n * (n - 1)) / 2);
    start.elapsed()
}

fn bench_lamellae_with() -> Duration {
    let (mut tx, mut rx) = channel!(Message, CAPACITY);

    let consumer_handle = thread::spawn(move || {
        let mut sum = 0;

        for _ in 0..BATCH_COUNT {
            while rx
                .try_recv_exact_with(BATCH_SIZE, |s1, s2| {
                    for i in s1 {
                        sum += *i;
                    }

                    for i in s2 {
                        sum += *i;
                    }
                })
                .is_err()
            {
                thread::yield_now();
            }
        }

        sum
    });

    let start = Instant::now();

    for batch in 0..BATCH_COUNT {
        while tx
            .try_send_exact_with(BATCH_SIZE, |s1, s2| {
                let mut sent_count: u64 = 0;

                for val in s1 {
                    *val = batch * BATCH_SIZE as u64 + sent_count;
                    sent_count += 1;
                }

                for val in s2 {
                    *val = batch * BATCH_SIZE as u64 + sent_count;
                    sent_count += 1;
                }
            })
            .is_err()
        {
            thread::yield_now();
        }
    }

    while tx.flush().is_err() {}
    let sum = consumer_handle.join().unwrap();
    let n = BATCH_SIZE as u64 * BATCH_COUNT;
    assert_eq!(sum, (n * (n - 1)) / 2);
    start.elapsed()
}

fn criterion_batched_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("Batched Operations");

    group.measurement_time(Duration::from_secs(15));
    group.sample_size(20);

    let total_bytes = BATCH_COUNT * BATCH_SIZE as u64 * std::mem::size_of::<Message>() as u64;
    group.throughput(Throughput::Bytes(total_bytes));

    group.bench_function("rtrb Batched", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                total_duration += bench_rtrb_batch();
            }
            total_duration
        });
    });

    group.bench_function("Lamellae Batched", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                total_duration += bench_lamellae_batch();
            }
            total_duration
        });
    });

    group.bench_function("Lamellae Batch With", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                total_duration += bench_lamellae_with();
            }
            total_duration
        });
    });

    group.finish();
}

// fn main() {
//     // let elapsed = bench_lamellae_batch();
//     let elapsed = bench_lamellae_with();
//     println!("elapsed: {}", elapsed.as_millis());
// }

criterion_group!(benches, criterion_batched_benchmarks);
criterion_main!(benches);
