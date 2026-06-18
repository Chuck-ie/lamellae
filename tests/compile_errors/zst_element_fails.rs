use lamellae::channel;

fn main() {
    struct ZST();
    let _ = channel!(ZST, 5);
}
