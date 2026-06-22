use lamellae::{channel, consumer, producer};

/// the capacity of ``TestMessages`` per cache line
const CL_CAPACITY: usize = 8;
const TOTAL_CAPACITY: usize = 4 * CL_CAPACITY;
const MAX_WRITEABLE_SLOTS: usize = TOTAL_CAPACITY - CL_CAPACITY;

type TestMessage = usize;

const fn assert_send<T: Send>(_: &T) {}

#[test]
fn test_producer_is_send() {
    let (tx, _) = channel!(TestMessage, TOTAL_CAPACITY);
    assert_send(&tx);
}

#[test]
fn test_consumer_is_send() {
    let (_, rx) = channel!(TestMessage, TOTAL_CAPACITY);
    assert_send(&rx);
}

#[test]
fn test_try_send_recv_lazy_wrap() {
    let (mut tx, mut rx) = channel!(TestMessage, TOTAL_CAPACITY);

    for i in 0..CL_CAPACITY {
        assert!(tx.try_send(i).is_ok());
    }

    assert!(rx.try_recv().is_err());
    assert!(tx.try_send(0).is_ok());

    for i in 0..CL_CAPACITY {
        assert_eq!(Ok(i), rx.try_recv());
    }
}

#[test]
fn test_try_send_recv_flush() {
    const MESSAGE: usize = 49;
    let (mut tx, mut rx) = channel!(TestMessage, TOTAL_CAPACITY);

    assert!(tx.try_send(MESSAGE).is_ok());
    assert!(rx.try_recv().is_err());
    assert!(tx.flush().is_ok());
    assert_eq!(Ok(MESSAGE), rx.try_recv());
}

#[test]
fn test_try_send_recv_queue_full() {
    let (mut tx, mut rx) = channel!(TestMessage, TOTAL_CAPACITY);

    for i in 0..(TOTAL_CAPACITY - CL_CAPACITY) {
        assert!(tx.try_send(i).is_ok());
    }

    assert_eq!(Err((77, producer::Error::QueueFull)), tx.try_send(77));
    assert!(rx.try_recv().is_ok());
    assert!(tx.try_send(49).is_ok());
}

#[test]
fn test_try_send_recv_queue_empty() {
    let (_, mut rx) = channel!(TestMessage, TOTAL_CAPACITY);
    assert_eq!(Err(consumer::Error::QueueEmpty), rx.try_recv());
}

#[test]
fn test_try_send_recv_batch_one_recv() {
    const BATCH_SIZE: usize = CL_CAPACITY;
    let (mut tx, mut rx) = channel!(TestMessage, TOTAL_CAPACITY);
    let send_buf: [usize; BATCH_SIZE] = core::array::from_fn(|i| i);

    let send_res = tx.try_send_batch(&send_buf);
    assert_eq!(Ok(BATCH_SIZE), send_res);
    assert!(tx.flush().is_ok());

    let mut recv_buf = [0; BATCH_SIZE];
    assert_ne!(recv_buf, send_buf);

    let recv_res = rx.try_recv_batch(&mut recv_buf);
    assert_eq!(Ok(BATCH_SIZE), recv_res);

    assert_eq!(recv_buf, send_buf);
}

#[test]
fn test_try_send_recv_batch_mul_recv() {
    const BATCH_SIZE: usize = CL_CAPACITY;
    const HALF_BATCH: usize = BATCH_SIZE / 2;
    let (mut tx, mut rx) = channel!(TestMessage, TOTAL_CAPACITY);
    let send_buf: [usize; BATCH_SIZE] = core::array::from_fn(|i| i);

    let send_res = tx.try_send_batch(&send_buf);
    assert_eq!(Ok(BATCH_SIZE), send_res);
    assert!(tx.flush().is_ok());

    let mut recv_buf = [0; BATCH_SIZE];
    assert_ne!(recv_buf, send_buf);

    let recv_res_1 = rx.try_recv_batch(&mut recv_buf[0..HALF_BATCH]);
    assert_eq!(Ok(HALF_BATCH), recv_res_1);

    let recv_res_2 = rx.try_recv_batch(&mut recv_buf[HALF_BATCH..]);
    assert_eq!(Ok(HALF_BATCH), recv_res_2);

    assert_eq!(recv_buf, send_buf);
}

#[test]
fn test_try_send_recv_batch_partial() {
    let (mut tx, mut rx) = channel!(TestMessage, TOTAL_CAPACITY);
    let mut buf = [0; 2 * CL_CAPACITY];

    assert_eq!(Ok(2 * CL_CAPACITY), tx.try_send_batch(&buf));
    assert_eq!(Ok(CL_CAPACITY), rx.try_recv_batch(&mut buf));
    assert!(tx.flush().is_ok());
    assert_eq!(Ok(CL_CAPACITY), rx.try_recv_batch(&mut buf));
    assert!(rx.try_recv().is_err());
}

#[test]
fn test_try_send_recv_batch_queue_full() {
    let (mut tx, mut rx) = channel!(TestMessage, TOTAL_CAPACITY);

    for i in 0..MAX_WRITEABLE_SLOTS {
        assert!(tx.try_send(i).is_ok());
    }

    assert_eq!(
        Err(producer::Error::QueueFull),
        tx.try_send_batch(&[0; TOTAL_CAPACITY])
    );
    assert!(rx.try_recv().is_ok());
    assert!(tx.try_send_batch(&[0]).is_ok());
}

#[test]
fn test_try_send_recv_batch_queue_empty() {
    let (_, mut rx) = channel!(TestMessage, TOTAL_CAPACITY);

    assert_eq!(
        Err(consumer::Error::QueueEmpty),
        rx.try_recv_batch(&mut [0])
    );
}

#[test]
fn test_try_send_recv_batch_exact_one_recv() {
    const BATCH_SIZE: usize = 8;
    let (mut tx, mut rx) = channel!(TestMessage, TOTAL_CAPACITY);
    let send_buf: [usize; BATCH_SIZE] = core::array::from_fn(|i| i);

    let send_res = tx.try_send_batch_exact(&send_buf);
    assert_eq!(Ok(BATCH_SIZE), send_res);
    assert!(tx.flush().is_ok());

    let mut recv_buf = [0; BATCH_SIZE];
    assert_ne!(recv_buf, send_buf);

    let recv_res = rx.try_recv_batch_exact(&mut recv_buf);
    assert_eq!(Ok(BATCH_SIZE), recv_res);

    assert_eq!(recv_buf, send_buf);
}

#[test]
fn test_try_send_recv_batch_exact_mul_recv() {
    const BATCH_SIZE: usize = 8;
    const HALF_BATCH: usize = BATCH_SIZE / 2;
    let (mut tx, mut rx) = channel!(TestMessage, TOTAL_CAPACITY);
    let send_buf: [usize; BATCH_SIZE] = core::array::from_fn(|i| i);

    let send_res = tx.try_send_batch_exact(&send_buf);
    assert_eq!(Ok(BATCH_SIZE), send_res);
    assert!(tx.flush().is_ok());

    let mut recv_buf = [0; BATCH_SIZE];
    assert_ne!(recv_buf, send_buf);

    let recv_res_1 = rx.try_recv_batch_exact(&mut recv_buf[0..HALF_BATCH]);
    assert_eq!(Ok(HALF_BATCH), recv_res_1);

    let recv_res_2 = rx.try_recv_batch_exact(&mut recv_buf[HALF_BATCH..]);
    assert_eq!(Ok(HALF_BATCH), recv_res_2);

    assert_eq!(recv_buf, send_buf);
}

#[test]
fn test_try_send_recv_batch_exact_queue_full() {
    let (mut tx, mut rx) = channel!(TestMessage, TOTAL_CAPACITY);

    for i in 0..MAX_WRITEABLE_SLOTS {
        assert!(tx.try_send(i).is_ok());
    }

    assert_eq!(
        Err(producer::Error::QueueFull),
        tx.try_send_batch_exact(&[0])
    );

    assert!(rx.try_recv().is_ok());
    assert!(tx.try_send_batch_exact(&[0]).is_ok());
}

#[test]
fn test_try_send_recv_batch_exact_queue_empty() {
    let (_, mut rx) = channel!(TestMessage, TOTAL_CAPACITY);

    assert_eq!(
        Err(consumer::Error::QueueEmpty),
        rx.try_recv_batch_exact(&mut [0])
    );
}
