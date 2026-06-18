use lamellae::channel;

#[cfg(any())]
mod compile_errors;

#[test]
fn test_compile_errors() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_errors/*.rs");
}

#[test]
fn test_init_success() {
    let _ = channel!(u64, 32);
}
