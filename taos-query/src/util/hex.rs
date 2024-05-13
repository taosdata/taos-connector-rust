use bytes::Bytes;

pub fn hex_string_to_bytes(s: &str) -> Bytes {
    let b: Vec<u8> = s
        .as_bytes()
        .chunks(2)
        .map(|chunk| std::str::from_utf8(chunk).unwrap())
        .map(|chunk| u8::from_str_radix(chunk, 16).unwrap())
        .collect();
    Bytes::from(b)
}

#[test]
fn test_inline_lines() {
    assert_eq!(
        Bytes::from(vec![0x12, 0x34, 0x56]),
        hex_string_to_bytes("123456")
    );
}
