use std::borrow::Cow;
use std::ffi::c_void;

use bytes::Bytes;

use crate::util::{Inlinable, InlinableRead};

const RAW_PTR_OFFSET: usize = std::mem::size_of::<u32>() + std::mem::size_of::<u16>();

/// C-struct for raw data, just a data view from native library.
///
/// It can be copy/cloned, but should not use it outbound away a offset lifetime.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct raw_data_t {
    pub raw: *const c_void,
    pub raw_len: u32,
    pub raw_type: u16,
}

unsafe impl Send for raw_data_t {}

impl raw_data_t {
    pub fn to_bytes(&self) -> Bytes {
        let cap = // raw data len
            self.raw_len as usize +
            // self.raw_len
            std::mem::size_of::<u32>() +
            // self.raw_type
            std::mem::size_of::<u16>();
        let mut data = Vec::with_capacity(cap);

        // first 4 bytes: raw_len
        data.extend(self.raw_len.to_le_bytes());

        // next 2 bytes: raw_type
        data.extend(self.raw_type.to_le_bytes());

        unsafe {
            let ptr = data.as_mut_ptr().add(RAW_PTR_OFFSET);
            std::ptr::copy_nonoverlapping(self.raw, ptr as _, self.raw_len as _);
            data.set_len(cap);
        }
        Bytes::from(data)
    }
}

#[derive(Debug, Clone)]
pub struct RawData(Bytes);

unsafe impl Send for RawData {}
unsafe impl Sync for RawData {}

impl From<&raw_data_t> for RawData {
    fn from(raw: &raw_data_t) -> Self {
        RawData(raw.to_bytes())
    }
}

impl<T: Into<Bytes>> From<T> for RawData {
    fn from(bytes: T) -> Self {
        RawData(bytes.into())
    }
}

impl RawData {
    pub fn new(raw: Bytes) -> Self {
        raw.into()
    }
    pub fn raw(&self) -> *const c_void {
        unsafe { self.0.as_ptr().add(RAW_PTR_OFFSET) as _ }
    }
    pub fn raw_len(&self) -> u32 {
        unsafe { *(self.0.as_ptr() as *const u32) }
    }
    pub fn raw_type(&self) -> u16 {
        unsafe { *(self.0.as_ptr().add(std::mem::size_of::<u32>()) as *const u16) }
    }

    pub fn as_raw_data_t(&self) -> raw_data_t {
        raw_data_t {
            raw: self.raw(),
            raw_len: self.raw_len(),
            raw_type: self.raw_type(),
        }
    }

    pub fn as_bytes(&self) -> Cow<Bytes> {
        Cow::Borrowed(&self.0)
    }
}

impl Inlinable for RawData {
    fn read_inlined<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        let mut data = Vec::new();

        let len = reader.read_u32()?;
        data.extend(len.to_le_bytes());

        let meta_type = reader.read_u16()?;
        data.extend(meta_type.to_le_bytes());

        data.resize(data.len() + len as usize, 0);

        let buf = &mut data[RAW_PTR_OFFSET..];

        reader.read_exact(buf)?;
        Ok(data.into())
    }

    fn write_inlined<W: std::io::Write>(&self, wtr: &mut W) -> std::io::Result<usize> {
        let bytes = self.as_bytes();
        wtr.write_all(&bytes)?;
        Ok(bytes.len())
    }
}

#[async_trait::async_trait]
impl crate::util::AsyncInlinable for RawData {
    async fn read_inlined<R: tokio::io::AsyncRead + Send + Unpin>(
        reader: &mut R,
    ) -> std::io::Result<Self> {
        use tokio::io::*;
        let mut data = Vec::new();

        let len = reader.read_u32_le().await?;
        data.extend(len.to_le_bytes());

        let meta_type = reader.read_u16_le().await?;
        data.extend(meta_type.to_le_bytes());

        data.resize(data.len() + len as usize, 0);

        let buf = &mut data[RAW_PTR_OFFSET..];

        reader.read_exact(buf).await?;
        Ok(data.into())
    }

    async fn write_inlined<W: tokio::io::AsyncWrite + Send + Unpin>(
        &self,
        wtr: &mut W,
    ) -> std::io::Result<usize> {
        use tokio::io::*;
        let bytes = self.as_bytes();
        wtr.write_all(&bytes).await?;
        Ok(bytes.len())
    }
}
