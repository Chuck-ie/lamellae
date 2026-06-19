use std::thread;
use std::time::{Duration, Instant};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use lamellae::channel;

// const BATCH_COUNT: u64 = 40_000;
// const BATCH_SIZE: usize = 128;
// const BATCH_COUNT: u64 = 20_000;
// const BATCH_SIZE: usize = 256;
const BATCH_COUNT: u64 = 25_000_000;
const BATCH_SIZE: usize = 256;

type Message = u64;

fn bench_rtrb_batch() -> Duration {
    let (mut tx, mut rx) = rtrb::RingBuffer::<Message>::new(2048);
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

    let _sum = consumer_handle.join().unwrap();
    start.elapsed()
}

// fn bench_lamellae_reservation() -> Duration {
//     let (mut tx, mut rx) = channel!(Message, 2048);
//
//     let consumer_handle = thread::spawn(move || {
//         let mut sum = 0;
//         let mut read_buf = [0u64; BATCH_SIZE];
//
//         for _ in 0..BATCH_COUNT {
//             let mut reservation = loop {
//                 if let Ok(res) = rx.try_reserve_exact(BATCH_SIZE) {
//                     break res;
//                 }
//                 thread::yield_now();
//             };
//
//             reservation.recv_slice(&mut read_buf);
//
//             for msg in &read_buf {
//                 sum += msg;
//             }
//         }
//         sum
//     });
//
//     let start = Instant::now();
//
//     for batch in 0..BATCH_COUNT {
//         let mut local_buf = [0u64; BATCH_SIZE];
//
//         for (j, val) in local_buf.iter_mut().enumerate() {
//             *val = batch * BATCH_SIZE as u64 + j as u64;
//         }
//
//         {
//             let mut reservation = loop {
//                 if let Ok(res) = tx.try_reserve_exact(BATCH_SIZE) {
//                     break res;
//                 }
//                 thread::yield_now();
//             };
//
//             reservation.send_slice(&local_buf);
//         }
//
//         while tx.flush().is_err() {}
//     }
//
//     let _sum = consumer_handle.join().unwrap();
//     start.elapsed()
// }

// fn bench_lamellae_reservation() -> Duration {
//     let (mut tx, mut rx) = channel!(Message, 2048);
//
//     let consumer_handle = thread::spawn(move || {
//         let mut sum = 0;
//
//         for _ in 0..BATCH_COUNT {
//             let mut reservation = loop {
//                 if let Ok(res) = rx.try_reserve_exact(BATCH_SIZE) {
//                     break res;
//                 }
//                 thread::yield_now();
//             };
//
//             while let Some(msg) = reservation.recv() {
//                 sum += msg;
//             }
//         }
//         sum
//     });
//
//     let start = Instant::now();
//
//     for batch in 0..BATCH_COUNT {
//         let mut local_buf = [0u64; BATCH_SIZE];
//
//         for (j, val) in local_buf.iter_mut().enumerate() {
//             *val = batch * BATCH_SIZE as u64 + j as u64;
//         }
//
//         let mut reservation = loop {
//             if let Ok(res) = tx.try_reserve_exact(BATCH_SIZE) {
//                 break res;
//             }
//             thread::yield_now();
//         };
//
//         for j in local_buf {
//             reservation.send(j);
//         }
//     }
//
//     while tx.flush().is_err() {}
//
//     let _sum = consumer_handle.join().unwrap();
//     start.elapsed()
// }

fn bench_lamellae_reservation() -> Duration {
    let (mut tx, mut rx) = channel!(Message, 2048);

    let consumer_handle = thread::spawn(move || {
        let mut sum = 0;
        let mut recv_buf = [0; BATCH_SIZE];

        for _ in 0..BATCH_COUNT {
            let mut reservation = loop {
                if let Ok(res) = rx.try_reserve_exact(BATCH_SIZE) {
                    break res;
                }
                thread::yield_now();
            };

            reservation.recv_slice(&mut recv_buf);

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
            let mut reservation = loop {
                if let Ok(res) = tx.try_reserve_exact(BATCH_SIZE) {
                    break res;
                }
                thread::yield_now();
            };

            reservation.send_slice(&local_buf);
        }
    }

    while tx.flush().is_err() {}
    let _sum = consumer_handle.join().unwrap();
    start.elapsed()
}

fn criterion_batched_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("Batched Operations");

    group.measurement_time(Duration::from_secs(15));
    group.sample_size(20);

    let total_bytes = BATCH_COUNT * BATCH_SIZE as u64 * std::mem::size_of::<Message>() as u64;
    group.throughput(Throughput::Bytes(total_bytes));

    group.bench_function("Lamellae Zero-Copy Reservation", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                total_duration += bench_lamellae_reservation();
            }
            total_duration
        });
    });

    group.bench_function("rtrb Chunk Slices", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                total_duration += bench_rtrb_batch();
            }
            total_duration
        });
    });

    group.finish();
}

fn main() {
    let elapsed = bench_lamellae_reservation();
    println!("elapsed: {}", elapsed.as_millis());
}

// criterion_group!(benches, criterion_batched_benchmarks);
// criterion_main!(benches);
