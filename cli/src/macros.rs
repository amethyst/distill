macro_rules! async_write {
    ($dst:expr, $($arg:tt)*) => (
        $dst.write_all(format!( $($arg)* ).as_bytes())
    )
}