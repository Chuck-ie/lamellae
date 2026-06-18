use lamellae::channel;

const fn assert_send<T: Send>(_: &T) {}

#[test]
fn test_consumer_is_send() {
    let (_, rx) = channel!(u64, 32);
    assert_send(&rx);
}

#[test]
fn test_producer_is_send() {
    let (tx, _) = channel!(u64, 32);
    assert_send(&tx);
}

#[test]
fn test_try_send_recv_lazy_wrap() {
    const MESSAGES: u64 = 8;
    let (mut tx, mut rx) = channel!(u64, 32);

    for i in 0..MESSAGES {
        assert!(tx.try_send(i).is_ok());
    }

    assert!(rx.try_recv().is_err());
    assert!(tx.try_send(MESSAGES).is_ok());

    for i in 0..MESSAGES {
        assert_eq!(Ok(i), rx.try_recv());
    }
}

#[test]
fn test_try_send_recv_flush() {
    const MESSAGE: u64 = 49;
    let (mut tx, mut rx) = channel!(u64, 32);

    assert!(tx.try_send(MESSAGE).is_ok());
    assert!(rx.try_recv().is_err());
    assert!(tx.flush().is_ok());
    assert_eq!(Ok(MESSAGE), rx.try_recv());
}

#[test]
fn test_try_send_recv_batch_partial_success() {
    const BATCH_SIZE: usize = 8;
    let (mut tx, mut rx) = channel!(u64, 32);
    let send_buf = [0, 1, 2, 3, 4, 5, 6, 7];
    assert_eq!(send_buf.len(), BATCH_SIZE);

    let send_res = tx.try_send_batch(&send_buf);
    assert!(send_res.is_ok());
    assert_eq!(Ok(BATCH_SIZE), send_res);

    let mut recv_buf = [0; BATCH_SIZE];
    assert_ne!(recv_buf, send_buf);

    let recv_res = rx.try_recv_batch(&mut recv_buf);
    assert!(recv_res.is_ok());
    assert_eq!(Ok(BATCH_SIZE), recv_res);
    assert_eq!(recv_buf, send_buf);
}

#[test]
fn test_try_send_recv_batch_partial_fail() {
    const BATCH_SIZE: usize = 32;
    let (mut tx, mut rx) = channel!(u64, BATCH_SIZE);
    let send_buf = [0; BATCH_SIZE];

    let send_res = tx.try_send_batch(&send_buf);
    assert_eq!(Ok(BATCH_SIZE / 2), send_res);
}
