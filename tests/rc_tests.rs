use std::thread;

use lamellae::channel;

type TestMessage = usize;

fn threaded_send_recv_sequence(messages: usize) {
    const TOTAL_CAPACITY: usize = 1024;

    let (mut tx, mut rx) = channel!(TestMessage, TOTAL_CAPACITY);

    let producer = thread::spawn(move || {
        for i in 0..messages {
            tx.send(i);
        }

        while tx.flush().is_err() {
            std::hint::spin_loop();
        }
    });

    let consumer = thread::spawn(move || {
        for expected in 0..messages {
            assert_eq!(rx.recv(), expected);
        }
    });

    producer.join().expect("producer thread panicked");
    consumer.join().expect("consumer thread panicked");
}

#[test]
fn test_threaded_send_recv_sequence() {
    threaded_send_recv_sequence(1_000_000);
}

#[test]
#[ignore = "heavy stress test"]
fn test_threaded_send_recv_sequence_stress() {
    threaded_send_recv_sequence(10_000_000);
}

#[test]
#[ignore = "heavy repeated stress test"]
fn test_threaded_send_recv_sequence_repeated_stress() {
    for _ in 0..1_000 {
        threaded_send_recv_sequence(100_000);
    }
}
