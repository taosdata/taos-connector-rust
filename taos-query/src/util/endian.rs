#[inline]
pub fn is_big_endian() -> bool {
    cfg!(target_endian = "big")
}

#[cfg(test)]
mod tests {
    use super::is_big_endian;

    #[test]
    fn test_is_big_endian() {
        #[cfg(target_endian = "little")]
        assert!(!is_big_endian());

        #[cfg(target_endian = "big")]
        assert!(is_big_endian());
    }
}
