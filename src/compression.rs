use anyhow::Result;
use std::io::Cursor;

pub fn compress_json_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    Ok(zstd::stream::encode_all(Cursor::new(bytes), 3)?)
}

pub fn decompress_json_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    Ok(zstd::stream::decode_all(Cursor::new(bytes))?)
}
