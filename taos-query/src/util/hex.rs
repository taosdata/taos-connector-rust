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
    const HEX_CHARS: &[u8] = b"0123456789abcdef";

    let mut res = String::with_capacity(bytes.len() * 2);
    for &byte in bytes.iter() {
        res.push(HEX_CHARS[(byte >> 4) as usize] as char);
        res.push(HEX_CHARS[(byte & 0xf) as usize] as char);
    }
    res
}

pub fn bytes_to_hex_string_upper(bytes: Bytes) -> String {
    const HEX_CHARS: &[u8] = b"0123456789ABCDEF";

    let mut res = String::with_capacity(bytes.len() * 2);
    for &byte in bytes.iter() {
        res.push(HEX_CHARS[(byte >> 4) as usize] as char);
        res.push(HEX_CHARS[(byte & 0xf) as usize] as char);
    }
    res
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn test_hex_string_to_bytes() {
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

    #[test]
    fn test_bytes_to_hex_string() {
        let bytes = Bytes::from(vec![
            0x00, 0x01, 0x10, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xFF,
        ]);
        let hex_string = bytes_to_hex_string(bytes);
        assert_eq!(hex_string, "000110123456789abcdeff");
    }

    #[test]
    fn test_bytes_to_hex_string_upper() {
        let bytes = Bytes::from(vec![
            0x00, 0x01, 0x10, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xFF,
        ]);
        let hex_string = bytes_to_hex_string_upper(bytes);
        assert_eq!(hex_string, "000110123456789ABCDEFF");
    }
}
