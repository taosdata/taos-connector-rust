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

pub fn bytes_to_hex_string(bytes: Bytes) -> String {
    bytes
        .iter()
        .map(|byte| format!("{:02x}", byte))
        .collect::<Vec<String>>()
        .join("")
}

#[test]
fn test_inline_lines() {
    assert_eq!(
        Bytes::from(vec![0x12, 0x34, 0x56]),
        hex_string_to_bytes("123456")
    );
}

#[test]
fn test_bytes_to_hex_and_back() {
    let original_bytes = Bytes::from(vec![0xde, 0xad, 0xbe, 0xef]);
    let hex_string = bytes_to_hex_string(original_bytes.clone());
    let converted_bytes = hex_string_to_bytes(&hex_string);

    assert_eq!(original_bytes, converted_bytes);
}
