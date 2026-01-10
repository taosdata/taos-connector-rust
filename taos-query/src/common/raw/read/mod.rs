use std::{
    borrow::Cow,
    cell::{Cell, LazyCell, RefCell},
    collections::VecDeque,
    ffi::c_void,
    fs::File,
    io::{BufRead, BufReader, Cursor, Read},
    ops::Deref,
    sync::LazyLock,
};

use byteorder::{ReadBytesExt, LE};
use bytes::{Buf, Bytes};
use faststr::FastStr;
use zerocopy::{ByteSlice, FromBytes, FromZeros, Immutable, KnownLayout, Unaligned};

use crate::{
    common::{meta, Field, FieldMore, MetaUnit, MultiBlockCursor, Precision, Timestamp, Ty, Value},
    util::Inlinable,
    RawBlock,
};

const RAW_PTR_OFFSET: usize = std::mem::size_of::<u32>() + std::mem::size_of::<u16>();

macro_rules! zigzag_decode {
    ($value:expr) => {
        ($value >> 1) ^ (-($value & 1))
    };
}

macro_rules! impl_read_var_int_unsigned {
    ($($ty:ident),*) => {
        paste::paste! {
            $(
                #[doc = concat!("Reads a variable-length unsigned integer (", stringify!($ty), ")")]
                fn [<read_ $ty v>](self: &mut Self) -> Result<$ty, std::io::Error> {
                    let mut v = 0;
                    let mut i = 0;
                    const ENCODE_LIMIT: u8 = 1 << 7; // 128, 0x80
                    loop {
                        let byte = self.read_u8()?;
                        if byte < ENCODE_LIMIT {
                            v |= (byte as $ty) << (7 * i);
                            break;
                        } else {
                            v |= ((byte as $ty & (ENCODE_LIMIT - 1) as $ty)) << (7 * i);
                        }
                        i += 1;
                    }

                    Ok(v)
                }
            )*
        }
    };
}
#[inline(always)]
pub const fn zigzag_decode_16_const(v: u16) -> i16 {
    ((v >> 1) as i16) ^ -((v & 1) as i16)
}
#[inline(always)]
pub const fn zigzag_decode_64_const(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}
#[inline(always)]
pub const fn zigzag_decode_32_const(v: u32) -> i32 {
    ((v >> 1) as i32) ^ -((v & 1) as i32)
}

macro_rules! impl_read_var_int_signed {
    ($($ut:ident >> $ty:ident),*) => {
        paste::paste! {
            $(
                #[doc = concat!("Reads a variable-length signed integer [", stringify!($ty), "]")]
                fn [<read_ $ty v>](self: &mut Self) -> Result<$ty, std::io::Error> {
                    let v = self.[<read_ $ut v>]()?;
                    Ok(((v >> 1) as $ty) ^ (-((v & 1) as $ty)))
                }
            )*
        }
    };
}

pub trait PrimitiveSlice: ByteSlice {
    #[doc = "Works like a `[i32]` slice .get_unchecked()`."]
    unsafe fn get_at_unchecked<T: Copy>(&self, offset: usize) -> T {
        self.as_ptr()
            .add(offset * std::mem::size_of::<T>())
            .cast::<T>()
            .read_unaligned()
    }
}

impl<T: ByteSlice> PrimitiveSlice for T {}
pub trait LazyFromBytes {
    /// Creates a new instance from the given bytes.
    fn lazy_from_bytes(bytes: &Bytes) -> Self;
}

struct LazyFromBytesImpl<T: LazyFromBytes, F> {
    _bytes: Bytes,
    _value: LazyLock<T, F>,
}
impl<T: LazyFromBytes, F> Deref for LazyFromBytesImpl<T, F>
where
    F: FnOnce() -> T,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self._value.deref()
    }
}
pub fn lazy_from<T: LazyFromBytes>(bytes: Bytes) -> LazyFromBytesImpl<T, impl FnOnce() -> T> {
    let lazy = bytes.clone();
    let _value = LazyLock::new(move || T::lazy_from_bytes(&lazy));
    LazyFromBytesImpl {
        _bytes: bytes,
        _value,
    }
}

pub trait ReadExactSizeBytesExt: Read {
    /// Read the exact `size` of bytes as [Bytes].
    fn read_exact_size_bytes(&mut self, size: usize) -> Result<Bytes, std::io::Error> {
        let mut buf = vec![0; size];
        self.read_exact(&mut buf)?;
        Ok(buf.into())
    }
}

impl ReadExactSizeBytesExt for File {}
impl<T: Read> ReadExactSizeBytesExt for BufReader<T> {}
impl ReadExactSizeBytesExt for Cursor<&[u8]> {}
impl ReadExactSizeBytesExt for Cursor<&mut [u8]> {}
impl ReadExactSizeBytesExt for Cursor<Vec<u8>> {}
impl ReadExactSizeBytesExt for &[u8] {}
impl ReadExactSizeBytesExt for Cursor<Bytes> {
    fn read_exact_size_bytes(&mut self, size: usize) -> Result<Bytes, std::io::Error> {
        let pos = self.position() as usize;
        let end = (pos + size).min(self.get_ref().len());
        let buf = self.get_ref().slice(pos..end);
        self.consume(size);
        Ok(buf)
    }
}
impl ReadExactSizeBytesExt for Cursor<&Bytes> {
    fn read_exact_size_bytes(&mut self, size: usize) -> Result<Bytes, std::io::Error> {
        let pos = self.position() as usize;
        let end = (pos + size).min(self.get_ref().len());
        let buf = self.get_ref().slice(pos..end);
        self.consume(size);
        Ok(buf)
    }
}

/// Extension trait for reading Taos-specific binary formats from a buffer.
///
/// This trait provides a set of methods for decoding variable-length integers,
/// zero-copy structs, and various Taos protocol structures from a buffer implementing
/// [`Buf`] and [`ReadExactSizeBytesExt`]. It is designed to facilitate efficient
/// parsing of Taos raw data blocks, schemas, compression options, column references,
/// and metadata, as well as handling Taos-specific encoding schemes such as zigzag
/// variable-length integers and null-terminated UTF-8 strings.
///
/// # Required Traits
/// - [`ReadExactSizeBytesExt`]: For reading an exact number of bytes.
/// - [`Buf`]: For buffer operations.
///
/// # Provided Methods
/// - Variable-length unsigned and signed integer decoding.
/// - Reading length-prefixed byte arrays and UTF-8 strings.
/// - Zero-copy reading of structs using [`zerocopy`] traits.
/// - Parsing Taos field and schema definitions.
/// - Reading compression options and column references.
/// - Skipping bytes or protocol headers as required by Taos formats.
/// - Parsing raw data blocks and associated metadata, including support for
///   different Taos protocol versions and table types.
///
/// # Safety
/// Some methods use `unsafe` code for zero-copy struct reading and pointer arithmetic.
/// These are carefully encapsulated and should only be used with trusted input data.
///
/// # Errors
/// All methods return [`std::io::Error`] on failure, including invalid data,
/// unexpected end of buffer, or protocol violations.
///
/// # Example
/// ```ignore
/// use taos_query::common::raw::read::ReadTaosExt;
/// let mut buf: &[u8] = ...;
/// let field = buf.read_field()?;
/// ```
///
/// # See Also
/// - [`ReadExactSizeBytesExt`]
/// - [`Buf`]
/// - [`zerocopy`]
/// - [`Bytes`]
/// - [`FastStr`]
pub trait ReadTaosExt: ReadExactSizeBytesExt + Buf {
    // Variable-length unsigned integers.
    impl_read_var_int_unsigned!(u64, u32, u16);

    // Zigzag decoded variable-length signed integers.
    impl_read_var_int_signed!(u64 >> i64, u32 >> i32, u16 >> i16);

    /// Read a variable-length u32 and that sized [Bytes].
    fn read_u32v_sized_bytes(&mut self) -> Result<Bytes, std::io::Error> {
        let len = self.read_u32v()? as usize;
        self.read_exact_size_bytes(len)
    }
    /// Read a variable-length u64 and that sized [Bytes].
    fn read_u64v_sized_bytes(&mut self) -> Result<Bytes, std::io::Error> {
        let len = self.read_u64v()? as usize;
        self.read_exact_size_bytes(len)
    }

    /// Read a variable-length u32 and that sized bytes to a [FastStr].
    ///
    /// Where the bytes are expected to be valid UTF-8, and the last byte is expected to be a null terminator.
    ///
    /// # Errors
    ///
    /// If the bytes are not valid UTF-8, it will return an error with [std::io::ErrorKind::InvalidData].
    ///
    fn read_to_faststr(&mut self) -> Result<FastStr, std::io::Error> {
        let bytes = self.read_u32v_sized_bytes()?;
        if bytes.len() <= 1 {
            return Ok(FastStr::empty());
        }

        Ok(
            FastStr::from_bytes(bytes.slice(0..bytes.len() - 1)).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid UTF-8 string")
            })?,
        )
    }

    /// Read remaining bytes to a [Bytes].
    fn read_bytes_to_end(&mut self) -> Result<Option<Bytes>, std::io::Error> {
        let size = self.remaining();
        if size == 0 {
            Ok(None)
        } else {
            self.read_exact_size_bytes(size).map(Some)
        }
    }

    /// Read a zero-copy struct from the buffer.
    fn read_zerocopy<T: zerocopy::FromBytes + KnownLayout + Immutable + Clone>(
        &mut self,
    ) -> Result<Cow<T>, std::io::Error> {
        let mut t = T::new_zeroed();
        let slice = unsafe {
            std::slice::from_raw_parts_mut(&mut t as *mut T as *mut u8, std::mem::size_of::<T>())
        };
        self.read_exact(slice)?;
        Ok(Cow::Owned(t))
    }

    /// C: `tDecodeSSchemaWrapper`
    ///
    /// | i8 | i8 | i32v | i16v | cstr |
    /// |----|----|------|------|------|
    /// | ty | flags | bytes | col_id | name |
    ///
    /// flags is a bitmask:
    ///
    /// ```text
    /// 0|is_compressed|is_indexed|set_null|-|is_primary_key|index_on|sma_on|set_null
    /// ```
    fn read_field(&mut self) -> Result<Field, std::io::Error> {
        let ty = self.read_i8()?;
        let ty = Ty::from(ty);
        // dbg!(ty);
        let _flags = self.read_i8()?;
        let bytes = self.read_i32v()?;
        let _col_id = self.read_i16v()?;
        let name = self.read_to_faststr()?;
        // dbg!(&ty, &name, flags, bytes, col_id);
        let field = Field::new(name, ty, bytes as u32);
        Ok(field)
    }

    fn read_schemas(&mut self) -> Result<Vec<Field>, std::io::Error> {
        let ncols = self.read_i32v()?;
        let _version = self.read_i32v()?;
        dbg!(ncols);
        let mut fields = Vec::with_capacity(ncols as usize);
        for _ in 0..ncols {
            let field = self.read_field()?;
            fields.push(field);
        }
        Ok(fields)
    }

    /// Read number of columns for compression options, and then read each column's compression options.
    fn read_compression_vec(&mut self) -> Result<(), std::io::Error> {
        let cursor = self;
        if let Ok(ncols) = cursor.read_i32v() {
            dbg!(ncols);
            let version = cursor.read_i32v()?;
            for i in 0..ncols {
                let id = cursor.read_i16v()?;
                let algo = cursor.read_u32::<LE>()?;

                /// Mask 0xFF000000 for encode, 0x00FFFF00 for compress algo, 0x000000FF for level.
                #[derive(Debug, Clone, Copy)]
                #[repr(C, packed(1))]
                struct CompressOptions {
                    level: CompressLevel,
                    compress: CompressAlgo,
                    encode: CompressEncode,
                }

                #[derive(Debug, Clone, Copy)]
                #[repr(u8)]
                enum CompressEncode {
                    NoChange = 0,
                    Simple8B = 1,
                    Xor = 2,
                    Rle = 3,
                    DeltaD = 4,
                    Disabled = 0xFF,
                }
                #[derive(Debug, Clone, Copy)]
                #[repr(u16)]
                enum CompressAlgo {
                    NoChange = 0,
                    Lz4 = 1,
                    Zlib = 2,
                    Zstd = 3,
                    TsZ = 4,
                    Xz = 5,
                    Disabled = 0xFF,
                }
                #[derive(Debug, Clone, Copy)]
                #[repr(u8)]
                enum CompressLevel {
                    NoChange = 0,
                    Low = 1,
                    Medium = 2,
                    High = 3,
                }
                let (encode, compress, level) = (algo >> 24, (algo >> 8) & 0xFFFF, algo & 0xFF);

                dbg!(encode, compress, level);
                let options = unsafe { std::mem::transmute::<u32, CompressOptions>(algo) };
                dbg!(&options);
                dbg!(i, id, algo);
            }
        }
        Ok(())
    }

    fn read_col_ref_vec(&mut self) -> Result<Vec<ColRef>, std::io::Error> {
        let ncols = self.read_i32v()?;
        let _version = self.read_i32v()?;

        let mut cols = Vec::with_capacity(ncols as usize);
        for _ in 0..ncols {
            let col = self.read_i16v()?;
            let has_ref = self.read_i8()? != 0;

            let col_ref = if has_ref {
                let ref_db = self.read_to_faststr()?;
                let ref_table = self.read_to_faststr()?;
                let ref_col = self.read_to_faststr()?;
                ColRef {
                    has_ref,
                    cold_id: col,
                    ref_db: Some(ref_db),
                    ref_table: Some(ref_table),
                    ref_col: Some(ref_col),
                }
            } else {
                ColRef {
                    has_ref,
                    cold_id: col,
                    ref_db: None,
                    ref_table: None,
                    ref_col: None,
                }
            };
            cols.push(col_ref);
        }
        Ok(cols)
    }

    /// Read the type modifier with specified number of columns if it exists.
    fn read_type_mod(&mut self, ncols: usize) -> Result<Option<Vec<i32>>, std::io::Error> {
        let has_type_mod = self.read_i8()? != 0;
        if has_type_mod {
            let mut vec = Vec::with_capacity(ncols);
            for _ in 0..ncols {
                let type_mod = self.read_i32v()?;
                vec.push(type_mod);
            }
            return Ok(Some(vec));
        } else {
            return Ok(None);
        }
    }

    fn skip_by_size(&mut self, size: usize) {
        thread_local! {
            static SKIP_BUF: RefCell<[u8;4096]> = RefCell::new([0; 4096]);
        }
        SKIP_BUF.with_borrow_mut(|buf| {
            let mut remaining = size;
            while remaining > 0 {
                let to_read = remaining.min(buf.len());
                if let Err(_) = self.read_exact(&mut buf[..to_read]) {
                    break;
                }
                remaining -= to_read;
            }
        });
    }

    fn skip_raw_head(&mut self) -> Result<(), std::io::Error> {
        let version: u8 = self.read_u8()?;
        if version >= 100 {
            let skip: u32 = self.read_u32::<LE>()?;
            self.skip_by_size(skip as _);
        } else {
            const fn get_type_skip(t: u8) -> i32 {
                match t {
                    1 => 8,
                    2 | 3 => 16,
                    _ => unreachable!(),
                }
            }
            let skip = get_type_skip(version);
            self.skip_by_size(skip as _);

            let version = self.read_u8()?;
            let skip = get_type_skip(version);
            self.skip_by_size(skip as _);
        }

        Ok(())
    }

    fn read_tag(&mut self) -> Result<(), std::io::Error> {
        #[derive(
            Debug,
            Clone,
            Copy,
            zerocopy::FromBytes,
            zerocopy::KnownLayout,
            zerocopy::Immutable,
            zerocopy::Unaligned,
        )]
        #[repr(C, packed(1))]
        struct STag {
            flags: i8,
            len: i16,
            n_tag: i16,
            ver: i32,
            idx: [i8; 0],
        }
        let tag = *self.read_zerocopy::<STag>()?;

        const TD_TAG_LARGE: i8 = 0x20;
        const TD_TAG_JSON: i8 = 0x40;
        // let mut values = Vec::with_capacity(tag.n_tag as usize);
        let is_large = tag.flags & TD_TAG_LARGE != 0;
        let is_json = tag.flags & TD_TAG_JSON != 0;
        let len = if is_large {
            std::mem::size_of::<u16>() * tag.n_tag as usize
        } else {
            std::mem::size_of::<u8>() * tag.n_tag as usize
        };
        let n_tags = tag.n_tag as usize;
        // drop(tag);
        let bytes = { self.read_exact_size_bytes(len)? };

        let mut tag_values = Vec::with_capacity(n_tags);
        let mut last_offset = 0;
        for i in 0..n_tags {
            let offset = if is_large {
                unsafe {
                    (bytes.as_ptr() as *const u16)
                        .add(i as usize)
                        .read_unaligned() as usize
                }
            } else {
                unsafe {
                    (bytes.as_ptr() as *const u8)
                        .add(i as usize)
                        .read_unaligned() as usize
                }
            };
            let bytes = self.read_exact_size_bytes(offset - last_offset)?;
            tag_values.push(bytes);
            last_offset = offset;
        }
        // let p = unsafe {
        //     tag.idx.as_ptr().add(if is_large {
        //         std::mem::size_of::<u16>() * tag.n_tag as usize
        //     } else {
        //         std::mem::size_of::<u8>() * tag.n_tag as usize
        //     }) as *const u8
        // };

        #[derive(Debug)]
        struct STagVal {
            flags: i8,
            len: i16,
            val: [u8; 0],
        }
        fn tag_to_values(tag: &STag) -> Result<Vec<STagVal>, i32> {
            const TD_TAG_LARGE: i8 = 0x20;
            const TD_TAG_JSON: i8 = 0x40;
            let is_json = tag.flags & TD_TAG_JSON != 0;
            let mut values = Vec::with_capacity(tag.n_tag as usize);
            let is_large = tag.flags & TD_TAG_LARGE != 0;
            let p = unsafe {
                tag.idx.as_ptr().add(if is_large {
                    std::mem::size_of::<u16>() * tag.n_tag as usize
                } else {
                    std::mem::size_of::<u8>() * tag.n_tag as usize
                }) as *const u8
            };
            for i in 0..tag.n_tag {
                let offset = if is_large {
                    unsafe { *(tag.idx.as_ptr() as *const u16).add(i as usize) as usize }
                } else {
                    unsafe { *(tag.idx.as_ptr() as *const u8).add(i as usize) as usize }
                };
                let ptr = unsafe { p.add(offset) };

                unsafe fn t_get_u32v(p: *const u8) -> (u32, usize) {
                    let mut n = 0usize;
                    let mut v = 0u32;

                    loop {
                        if unsafe { *p.add(n) } <= 0x7f {
                            v |= (unsafe { *p.add(n) } as u32) << (7 * n);
                            n += 1;
                            break;
                        }
                        v |= ((unsafe { *p.add(n) } & 0x7f) as u32) << (7 * n);
                        n += 1;
                    }

                    (v, n)
                }

                if is_json {
                    // JSON tag value
                    let (len, n) = unsafe { t_get_u32v(ptr) };
                    dbg!(len, n);
                    let key = unsafe {
                        std::str::from_utf8_unchecked(std::slice::from_raw_parts(
                            ptr.add(n) as _,
                            len as usize,
                        ))
                    };
                    dbg!(key);

                    let t = unsafe { *(ptr.add(n + len as usize) as *const u8) };
                    dbg!(t);
                    let ty = crate::common::Ty::from(t);
                    dbg!(ty);
                    if ty.is_var_type() {
                        let (len, n) =
                            unsafe { t_get_u32v(ptr.add(n + len as usize + 1) as *const u8) };
                        dbg!(len, n);
                        let val = unsafe {
                            std::slice::from_raw_parts(
                                ptr.add(n + len as usize + 1 + n) as *const u8,
                                len as usize,
                            )
                        };
                        dbg!(val);
                    } else {
                        let val = unsafe { ptr.add(n + len as usize + 1) };
                        let value = match ty {
                            crate::common::Ty::Null => Value::Null(ty),
                            crate::common::Ty::Bool => {
                                dbg!(unsafe { *val } != 0);
                                Value::Bool(unsafe { *val } != 0)
                            }
                            crate::common::Ty::TinyInt => {
                                let val = unsafe { *(val as *const i8) };
                                dbg!(val);
                                Value::TinyInt(val)
                            }
                            crate::common::Ty::SmallInt => {
                                let val = unsafe { *(val as *const i16) };
                                dbg!(val);
                                Value::SmallInt(val)
                            }
                            crate::common::Ty::Int => {
                                let val = unsafe { *(val as *const i32) };
                                dbg!(val);
                                Value::Int(val)
                            }
                            crate::common::Ty::BigInt => {
                                let val = unsafe { *(val as *const i64) };
                                dbg!(val);
                                Value::BigInt(val)
                            }
                            crate::common::Ty::UTinyInt => {
                                let val = unsafe { *(val as *const u8) };
                                dbg!(val);
                                Value::UTinyInt(val)
                            }
                            crate::common::Ty::USmallInt => {
                                let val = unsafe { *(val as *const u16) };
                                dbg!(val);
                                Value::USmallInt(val)
                            }
                            crate::common::Ty::UInt => {
                                let val = unsafe { *(val as *const u32) };
                                dbg!(val);
                                Value::UInt(val)
                            }
                            crate::common::Ty::UBigInt => {
                                let val = unsafe { *(val as *const u64) };
                                dbg!(val);
                                Value::UBigInt(val)
                            }
                            crate::common::Ty::Float => {
                                let val = unsafe { *(val as *const f32) };
                                dbg!(val);
                                Value::Float(val)
                            }
                            crate::common::Ty::Double => {
                                let val = unsafe { (val as *const f64).read_unaligned() };
                                dbg!(val);
                                Value::Double(val)
                            }
                            crate::common::Ty::Timestamp => {
                                let val = unsafe { (val as *const i64).read_unaligned() };
                                dbg!(val);
                                Value::Timestamp(Timestamp::Milliseconds(val))
                            }
                            _ => {
                                unreachable!("{ty} is not supported in json tag value")
                            }
                        };
                    }
                } else {
                    // normal tag value
                    let len = unsafe { *(ptr as *const u8) } as usize;
                    let val = unsafe { std::slice::from_raw_parts(ptr.add(1), len) };
                    values.push(STagVal {
                        flags: tag.flags,
                        len: len as i16,
                        val: val.to_vec().try_into().unwrap_or_default(),
                    });
                }
            }
            Ok(values)
        }

        // if let Ok(v) = tag_to_values(&tag) {
        //     dbg!(&v);
        // }
        // eprintln!("Child table's tag bytes: {:?}", tag);
        Ok(())
    }

    /// C: `tDecodeSVCreateTbReq`
    fn parse_create_table(&mut self) -> Result<MetaCreateTable, std::io::Error> {
        let len = self.read_i32::<LE>()?;
        let flags = self.read_i32v()?;
        let name = self.read_to_faststr()?;
        let uid = self.read_i64::<LE>()?;
        let btime = self.read_i64::<LE>()?;
        // btime is the block time, not the create time
        let ttl = self.read_i32::<LE>()?;
        // ttl is the time to live, not the create time
        // if ttl is negative, it means no ttl
        if ttl <= 0 {
            eprintln!("Warning: create table {:?} has no ttl", name);
        }
        let t = self.read_i8()?;
        let comment_len = self.read_i32::<LE>()?;
        let comment = if comment_len > 0 {
            eprintln!("Warning: create table {:?} has comment length {}, which is not supported in raw data.", name, comment_len);
            let comment = self.read_to_faststr()?;
            Some(comment)
        } else {
            None
        };

        #[repr(i8)]
        enum ETableType {
            TSDB_SUPER_TABLE = 1,  // super table
            TSDB_CHILD_TABLE = 2,  // table created from super table
            TSDB_NORMAL_TABLE = 3, // ordinary table
            TSDB_TEMP_TABLE = 4,   // temp table created by nest query
            TSDB_SYSTEM_TABLE = 5,
            TSDB_TSMA_TABLE = 6, // time-range-wise sma
            TSDB_VIEW_TABLE = 7,
            TSDB_VIRTUAL_NORMAL_TABLE = 8,
            TSDB_VIRTUAL_CHILD_TABLE = 9,
            TSDB_TABLE_MAX = 10,
        }

        let table_type: ETableType = unsafe { std::mem::transmute_copy(&t) };
        let mut ncols = 0;

        match table_type {
            ETableType::TSDB_SUPER_TABLE => {
                eprintln!(
                            "Warning: create table {:?} is a super table, which is not supported in raw data.",
                            name
                        );
                eprintln!(
                    "Super table uid: {}, btime: {}, ttl: {}, comment: {:?}",
                    uid, btime, ttl, comment
                );
                let stable_name = self.read_to_faststr()?;
                eprintln!("Child table's super table name: {:?}", stable_name);

                let tag_num = self.read_u8()?;
                eprintln!("Child table's tag num: {}", tag_num);
                let suid = self.read_i64::<LE>()?;

                eprintln!("Child table's super table uid: {}", suid);
            }
            ETableType::TSDB_CHILD_TABLE | ETableType::TSDB_VIRTUAL_CHILD_TABLE => {
                eprintln!(
                    "Child child table uid: {}, btime: {}, ttl: {}, comment: {:?}",
                    uid, btime, ttl, comment
                );
                let stable_name = self.read_to_faststr()?;
                eprintln!("Child table's super table name: {}", stable_name);

                let tag_num = self.read_u8()?;
                eprintln!("Child table's tag num: {}", tag_num);
                let suid = self.read_i64::<LE>()?;

                eprintln!("Child table's super table uid: {}", suid);
                for _ in 0..tag_num {
                    // let tag = self.read_zerocopy::<STag>()?;
                    let tag_bytes = self.read_u32v_sized_bytes()?;
                    eprintln!("Child table's tag bytes: {:?}", tag_bytes);

                    #[derive(
                        Debug,
                        Clone,
                        Copy,
                        zerocopy::FromBytes,
                        zerocopy::KnownLayout,
                        zerocopy::Immutable,
                        zerocopy::Unaligned,
                    )]
                    #[repr(C, packed(1))]
                    struct STag {
                        flags: i8,
                        len: i16,
                        nTag: i16,
                        ver: i32,
                        idx: [u8; 0],
                    };
                    let (tag, slice): (&STag, _) =
                        zerocopy::FromBytes::ref_from_prefix(tag_bytes.as_ref())
                            .expect("Invalid tag bytes");

                    #[derive(Debug)]
                    struct STagVal {
                        flags: i8,
                        len: i16,
                        val: [u8; 0],
                    }
                    fn tag_to_values(tag: &STag) -> Result<Vec<STagVal>, i32> {
                        const TD_TAG_LARGE: i8 = 0x20;
                        const TD_TAG_JSON: i8 = 0x40;
                        let mut values = Vec::with_capacity(tag.nTag as usize);
                        let is_large = tag.flags & TD_TAG_LARGE != 0;
                        let p = unsafe {
                            tag.idx.as_ptr().add(if is_large {
                                std::mem::size_of::<u16>() * tag.nTag as usize
                            } else {
                                std::mem::size_of::<u8>() * tag.nTag as usize
                            }) as *const u8
                        };
                        for i in 0..tag.nTag {
                            let offset = if is_large {
                                unsafe {
                                    *(tag.idx.as_ptr() as *const u16).add(i as usize) as usize
                                }
                            } else {
                                unsafe { *(tag.idx.as_ptr() as *const u8).add(i as usize) as usize }
                            };
                            let ptr = unsafe { p.add(offset) };
                            let is_json = tag.flags & TD_TAG_JSON != 0;

                            unsafe fn t_get_u32v(p: *const u8) -> (u32, usize) {
                                let mut n = 0usize;
                                let mut v = 0u32;

                                loop {
                                    if unsafe { *p.add(n) } <= 0x7f {
                                        v |= (unsafe { *p.add(n) } as u32) << (7 * n);
                                        n += 1;
                                        break;
                                    }
                                    v |= ((unsafe { *p.add(n) } & 0x7f) as u32) << (7 * n);
                                    n += 1;
                                }

                                (v, n)
                            }

                            if is_json {
                                // JSON tag value
                                let (len, n) = unsafe { t_get_u32v(ptr) };
                                dbg!(len, n);
                                let key = unsafe {
                                    std::str::from_utf8_unchecked(std::slice::from_raw_parts(
                                        ptr.add(n) as _,
                                        len as usize,
                                    ))
                                };
                                dbg!(key);

                                let t = unsafe { *(ptr.add(n + len as usize) as *const u8) };
                                dbg!(t);
                                let ty = crate::common::Ty::from(t);
                                dbg!(ty);
                                if ty.is_var_type() {
                                    let (len, n) = unsafe {
                                        t_get_u32v(ptr.add(n + len as usize + 1) as *const u8)
                                    };
                                    dbg!(len, n);
                                    let val = unsafe {
                                        std::slice::from_raw_parts(
                                            ptr.add(n + len as usize + 1 + n) as *const u8,
                                            len as usize,
                                        )
                                    };
                                    dbg!(val);
                                } else {
                                    let val = unsafe { ptr.add(n + len as usize + 1) };
                                    let value = match ty {
                                        crate::common::Ty::Null => Value::Null(ty),
                                        crate::common::Ty::Bool => {
                                            dbg!(unsafe { *val } != 0);
                                            Value::Bool(unsafe { *val } != 0)
                                        }
                                        crate::common::Ty::TinyInt => {
                                            let val = unsafe { *(val as *const i8) };
                                            dbg!(val);
                                            Value::TinyInt(val)
                                        }
                                        crate::common::Ty::SmallInt => {
                                            let val = unsafe { *(val as *const i16) };
                                            dbg!(val);
                                            Value::SmallInt(val)
                                        }
                                        crate::common::Ty::Int => {
                                            let val = unsafe { *(val as *const i32) };
                                            dbg!(val);
                                            Value::Int(val)
                                        }
                                        crate::common::Ty::BigInt => {
                                            let val = unsafe { *(val as *const i64) };
                                            dbg!(val);
                                            Value::BigInt(val)
                                        }
                                        crate::common::Ty::UTinyInt => {
                                            let val = unsafe { *(val as *const u8) };
                                            dbg!(val);
                                            Value::UTinyInt(val)
                                        }
                                        crate::common::Ty::USmallInt => {
                                            let val = unsafe { *(val as *const u16) };
                                            dbg!(val);
                                            Value::USmallInt(val)
                                        }
                                        crate::common::Ty::UInt => {
                                            let val = unsafe { *(val as *const u32) };
                                            dbg!(val);
                                            Value::UInt(val)
                                        }
                                        crate::common::Ty::UBigInt => {
                                            let val = unsafe { *(val as *const u64) };
                                            dbg!(val);
                                            Value::UBigInt(val)
                                        }
                                        crate::common::Ty::Float => {
                                            let val = unsafe { *(val as *const f32) };
                                            dbg!(val);
                                            Value::Float(val)
                                        }
                                        crate::common::Ty::Double => {
                                            let val =
                                                unsafe { (val as *const f64).read_unaligned() };
                                            dbg!(val);
                                            Value::Double(val)
                                        }
                                        crate::common::Ty::Timestamp => {
                                            let val =
                                                unsafe { (val as *const i64).read_unaligned() };
                                            dbg!(val);
                                            Value::Timestamp(Timestamp::Milliseconds(val))
                                        }
                                        _ => {
                                            unreachable!("{ty} is not supported in json tag value")
                                        }
                                    };
                                }
                            } else {
                                // normal tag value
                                let len = unsafe { *(ptr as *const u8) } as usize;
                                let val = unsafe { std::slice::from_raw_parts(ptr.add(1), len) };
                                values.push(STagVal {
                                    flags: tag.flags,
                                    len: len as i16,
                                    val: val.to_vec().try_into().unwrap_or_default(),
                                });
                            }
                        }
                        Ok(values)
                    }

                    if let Ok(v) = tag_to_values(&tag) {
                        dbg!(&v);
                    }
                    eprintln!("Child table's tag bytes: {:?}", tag);
                }
                let len = self.read_i32::<LE>()?;
                for i in 0..len {
                    let tag_name = self.read_to_faststr()?;
                    eprintln!("Child table's tag[{}] name: {}", i, tag_name);
                }
            }
            ETableType::TSDB_NORMAL_TABLE | ETableType::TSDB_VIRTUAL_NORMAL_TABLE => {
                // normal table
                let schemas = self.read_schemas()?;
                dbg!(&schemas);
                ncols = schemas.len();
            }
            _ => (),
        }
        if self.has_remaining() {
            let sql_len = self.read_i32::<LE>()?;
            if sql_len > 0 {
                let sql_bytes = self.read_u32v_sized_bytes()?;
                dbg!(&sql_bytes);
            }
            if matches!(
                table_type,
                ETableType::TSDB_NORMAL_TABLE | ETableType::TSDB_SUPER_TABLE
            ) {
                if self.has_remaining() {
                    self.read_compression_vec()?;
                }
            } else if matches!(
                table_type,
                ETableType::TSDB_VIRTUAL_NORMAL_TABLE | ETableType::TSDB_VIRTUAL_CHILD_TABLE
            ) {
                // virtual table does not have compression options
                if self.has_remaining() {
                    eprintln!(
                        "Warning: virtual table {:?} does not have compression options",
                        name
                    );
                    let refs = self.read_col_ref_vec()?;
                    dbg!(&refs);
                }
            }
            if self.has_remaining() {
                // read column references
                let refs = self.read_type_mod(ncols)?;
                dbg!(&refs);
            }
        }
        Ok(MetaCreateTable {
            name,
            uid,
            btime,
            ttl,
            comment,
        })
    }

    fn parse_raw_block_with_meta(
        &mut self,
        with_table_name: bool,
    ) -> Result<RawBlock, std::io::Error> {
        let block_total_len = self.read_u32v()?;
        // dbg!(block_total_len);
        let version = self.read_u64::<LE>()?;
        const RETRIEVE_TABLE_RSP_VERSION: u64 = 0;
        const RETRIEVE_TABLE_RSP_TMQ_VERSION: u64 = 1;
        const RETRIEVE_TABLE_RSP_TMQ_RAW_VERSION: u64 = 2;

        #[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
        #[repr(C, packed(1))]
        struct SRetrieveTableRspForTmq {
            rows: i64,
            compressed: i8,
            precision: i8,
        }
        let precision = match version {
            RETRIEVE_TABLE_RSP_VERSION => {
                unimplemented!("Raw block version 0 is not supported yet");
            }
            RETRIEVE_TABLE_RSP_TMQ_VERSION | RETRIEVE_TABLE_RSP_TMQ_RAW_VERSION => {
                let header = self.read_zerocopy::<SRetrieveTableRspForTmq>()?;
                // dbg!(&header);
                header.precision
            }
            v => {
                eprintln!("Unsupported raw block version: {}", v);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Unsupported raw block version",
                ));
            }
        };
        // dbg!(precision);

        // parse block data
        let data = self.read_exact_size_bytes((block_total_len - 18) as usize)?;
        // dbg!(&data);
        let mut raw_block = RawBlock::parse_from_raw_block(data, precision.into());

        // parse block schema
        let cols = self.read_i32v()?;
        let _version = self.read_i32v()?; // skip version

        let mut fields = Vec::new();
        for _ in 0..cols {
            let field = self.read_field()?;
            fields.push(field.name().to_string());
        }
        raw_block.with_field_names(fields.iter());

        if with_table_name {
            raw_block.with_table_name(self.read_to_faststr()?);
        }
        Ok(raw_block)
    }

    fn parse_raw_data_blocks(&mut self) -> Result<VecDeque<RawBlock>, std::io::Error> {
        // skip header
        self.skip_raw_head()?;

        let block_num = self.read_i32::<LE>()?;
        let with_table_name: bool = self.read_u8()? != 0; // withTableName
        self.read_u8()?; // skip withSchema, always true

        let mut raw_block_queue = VecDeque::with_capacity(block_num as usize);
        for _ in 0..block_num {
            let raw_block = self.parse_raw_block_with_meta(with_table_name)?;
            raw_block_queue.push_back(raw_block);
        }
        return Ok(raw_block_queue);
    }

    /// Read blocks
    fn parse_raw_data_with_type(
        &mut self,
        raw_type: u16,
    ) -> Result<VecDeque<RawBlock>, std::io::Error> {
        // enum {
        //   RES_TYPE__QUERY = 1,
        //   RES_TYPE__TMQ,
        //   RES_TYPE__TMQ_META,
        //   RES_TYPE__TMQ_METADATA,
        //   RES_TYPE__TMQ_BATCH_META,
        //   RES_TYPE__TMQ_RAWDATA,
        // };

        match raw_type {
            // Only type 2/4 contains raw blocks.
            2 => self.parse_raw_data_blocks(),
            4 => {
                let blocks = self.parse_raw_data_blocks()?;

                // let mut meta_vec = Vec::new();

                let create_table_num = self.read_i32::<LE>()?;
                if create_table_num <= 0 {
                    // return Ok(meta_vec);
                    panic!("Raw data type 4 should have meta data, but got meta_len <= 0");
                }
                for i in 0..create_table_num {
                    let create_table_bytes = self.read_u64v_sized_bytes()?;
                    let mut table_cursor = Cursor::new(create_table_bytes);
                    let c = table_cursor.parse_create_table()?;
                    dbg!(c);
                }
                let mut others = Vec::new();
                if self.has_remaining() {
                    self.read_to_end(&mut others)?;
                    if !others.is_empty() {
                        eprintln!(
                            "Warning: {} bytes of other data found after raw blocks, they are not supported in raw data: {:?}",
                            others.len(), Bytes::from(others),
                        );
                    } else {
                        eprintln!("No other data found after raw blocks.");
                    }
                }
                return Ok(blocks);
            }
            rt => {
                eprintln!("Unsupported raw data type: {}", rt);
                return Ok(VecDeque::new());
            }
        }
    }

    fn to_meta_vec(&mut self, raw_type: u16) -> Result<Vec<MetaView>, std::io::Error> {
        let mut meta_vec = Vec::new();
        match raw_type {
            2 => {
                return Ok(meta_vec); // no meta in raw data type 2
            }
            3 | 4 => {
                // type 3/5 contains meta data
                let meta_len = self.read_i32::<LE>()?;
                if meta_len <= 0 {
                    return Ok(meta_vec);
                }
            }
            // Batched meta
            5 => {
                let meta_len = self.read_i32::<LE>()?;
                if meta_len <= 0 {
                    return Ok(meta_vec);
                }
                dbg!(&meta_len);
                for _ in 0..meta_len {
                    let meta_bytes = self.read_u32v_sized_bytes()?;
                    dbg!(&meta_bytes);
                    meta_vec.push(MetaView { inner: meta_bytes });
                }
            }
            rt => {
                eprintln!("Unsupported raw data type: {}", rt);
                return Ok(meta_vec);
            }
        }

        if self.has_remaining() {
            let mut buf = Vec::new();
            self.read_to_end(&mut buf)?;

            dbg!(Bytes::from(buf));
        }

        Ok(meta_vec)
    }
}

impl ReadTaosExt for Cursor<&[u8]> {
    fn read_zerocopy<T: zerocopy::FromBytes + KnownLayout + Immutable + Clone>(
        &mut self,
    ) -> Result<Cow<T>, std::io::Error> {
        let val = unsafe {
            let ptr = self.get_ref().as_ptr().offset(self.position() as isize);
            (ptr as *const T).as_ref().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid data")
            })?
        };
        self.consume(std::mem::size_of::<T>());
        Ok(Cow::Borrowed(val))
    }
}
impl ReadTaosExt for Cursor<Bytes> {
    fn read_zerocopy<T: zerocopy::FromBytes + KnownLayout + Immutable + Clone>(
        &mut self,
    ) -> Result<Cow<T>, std::io::Error> {
        let ptr = unsafe {
            let ptr = self.get_ref().as_ptr().offset(self.position() as isize);
            (ptr as *const T).as_ref().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid data")
            })?
        };
        self.consume(std::mem::size_of::<T>());
        Ok(Cow::Borrowed(ptr))
    }
    fn read_u32v_sized_bytes(&mut self) -> Result<Bytes, std::io::Error> {
        let len = self.read_u64v()? as usize;
        let pos = self.position() as usize;
        self.consume(len);
        Ok(self.get_ref().slice(pos..pos + len))
    }
    // fn read_exact_size_bytes(&mut self, size: usize) -> Result<Bytes, std::io::Error> {
    //     let pos = self.position() as usize;
    //     let end = (pos + size).min(self.get_ref().len());
    //     let buf = self.get_ref().slice(pos..end);
    //     self.consume(size);
    //     Ok(buf)
    // }
}
impl ReadTaosExt for Cursor<&Bytes> {
    fn read_zerocopy<T: zerocopy::FromBytes + KnownLayout + Immutable + Clone>(
        &mut self,
    ) -> Result<Cow<T>, std::io::Error> {
        let ptr = unsafe {
            let ptr = self.get_ref().as_ptr().offset(self.position() as isize);
            (ptr as *const T).as_ref().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid data")
            })?
        };
        self.consume(std::mem::size_of::<T>());
        Ok(Cow::Borrowed(ptr))
    }
    fn read_u32v_sized_bytes(&mut self) -> Result<Bytes, std::io::Error> {
        let len = self.read_u64v()? as usize;
        let pos = self.position() as usize;
        self.consume(len);
        Ok(self.get_ref().slice(pos..pos + len))
    }
    // fn read_exact_size_bytes(&mut self, size: usize) -> Result<Bytes, std::io::Error> {
    //     let pos = self.position() as usize;
    //     let end = (pos + size).min(self.get_ref().len());
    //     let buf = self.get_ref().slice(pos..end);
    //     self.consume(size);
    //     Ok(buf)
    // }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColRef {
    has_ref: bool,
    cold_id: i16,
    ref_db: Option<FastStr>,
    ref_table: Option<FastStr>,
    ref_col: Option<FastStr>,
}
#[derive(Debug)]
pub struct MetaCreateTable {
    name: FastStr,
    uid: i64,
    btime: i64,
    ttl: i32,
    comment: Option<FastStr>,
}

impl MetaCreateTable {
    pub fn parse(table_cursor: &mut Cursor<&Bytes>) -> std::io::Result<Self> {
        let len = table_cursor.read_i32::<LE>()?;
        let flags = table_cursor.read_i32v()?;
        dbg!(&flags);
        let name = table_cursor.read_to_faststr()?;
        dbg!(&name);
        let uid = table_cursor.read_i64::<LE>()?;
        dbg!(&uid);
        let btime = table_cursor.read_i64::<LE>()?;
        dbg!(&btime);
        // btime is the block time, not the create time
        let ttl = table_cursor.read_i32::<LE>()?;
        dbg!(&ttl);
        // ttl is the time to live, not the create time
        // if ttl is 0, it means no ttl
        if ttl < 0 {
            eprintln!("Warning: create table {:?} has negative ttl {}, which is not supported in raw data.", name, ttl);
        }
        // if ttl is negative, it means no ttl
        if ttl == 0 {
            eprintln!("Warning: create table {:?} has no ttl", name);
        }
        let t = table_cursor.read_i8()?;
        dbg!(&t);
        let comment_len = table_cursor.read_i32::<LE>()?;
        dbg!(&comment_len);
        let comment = if comment_len > 0 {
            eprintln!("Warning: create table {:?} has comment length {}, which is not supported in raw data.", name, comment_len);
            let comment = table_cursor.read_to_faststr()?;
            Some(comment)
        } else {
            None
        };

        #[repr(i8)]
        enum ETableType {
            TSDB_SUPER_TABLE = 1,  // super table
            TSDB_CHILD_TABLE = 2,  // table created from super table
            TSDB_NORMAL_TABLE = 3, // ordinary table
            TSDB_TEMP_TABLE = 4,   // temp table created by nest query
            TSDB_SYSTEM_TABLE = 5,
            TSDB_TSMA_TABLE = 6, // time-range-wise sma
            TSDB_VIEW_TABLE = 7,
            TSDB_VIRTUAL_NORMAL_TABLE = 8,
            TSDB_VIRTUAL_CHILD_TABLE = 9,
            TSDB_TABLE_MAX = 10,
        }

        let table_type: ETableType = unsafe { std::mem::transmute_copy(&t) };
        let mut ncols = 0;

        match table_type {
            ETableType::TSDB_SUPER_TABLE => {
                eprintln!(
                            "Warning: create table {:?} is a super table, which is not supported in raw data.",
                            name
                        );
                eprintln!(
                    "Super table uid: {}, btime: {}, ttl: {}, comment: {:?}",
                    uid, btime, ttl, comment
                );
                let stable_name = table_cursor.read_to_faststr()?;
                eprintln!("Child table's super table name: {:?}", stable_name);

                let tag_num = table_cursor.read_u8()?;
                eprintln!("Child table's tag num: {}", tag_num);
                let suid = table_cursor.read_i64::<LE>()?;

                eprintln!("Child table's super table uid: {}", suid);
            }
            ETableType::TSDB_CHILD_TABLE | ETableType::TSDB_VIRTUAL_CHILD_TABLE => {
                eprintln!(
                    "Child child table uid: {}, btime: {}, ttl: {}, comment: {:?}",
                    uid, btime, ttl, comment
                );
                let stable_name = table_cursor.read_to_faststr()?;
                eprintln!("Child table's super table name: {}", stable_name);

                let tag_num = table_cursor.read_u8()?;
                eprintln!("Child table's tag num: {}", tag_num);
                let suid = table_cursor.read_i64::<LE>()?;

                eprintln!("Child table's super table uid: {}", suid);
                for _ in 0..tag_num {
                    // let tag = table_cursor.read_zerocopy::<STag>()?;
                    let tag_bytes = table_cursor.read_u32v_sized_bytes()?;
                    eprintln!("Child table's tag bytes: {:?}", tag_bytes);

                    #[derive(
                        Debug,
                        Clone,
                        Copy,
                        zerocopy::FromBytes,
                        zerocopy::KnownLayout,
                        zerocopy::Immutable,
                        zerocopy::Unaligned,
                    )]
                    #[repr(C, packed(1))]
                    struct STag {
                        flags: i8,
                        len: i16,
                        n_tag: i16,
                        ver: i32,
                        idx: [u8; 0],
                    };
                    let (tag, slice): (&STag, _) =
                        zerocopy::FromBytes::ref_from_prefix(tag_bytes.as_ref())
                            .expect("Invalid tag bytes");

                    #[derive(Debug)]
                    struct STagVal {
                        flags: i8,
                        len: i16,
                        val: [u8; 0],
                    }
                    fn tag_to_values(tag: &STag) -> Result<Vec<STagVal>, i32> {
                        const TD_TAG_LARGE: i8 = 0x20;
                        const TD_TAG_JSON: i8 = 0x40;
                        let mut values = Vec::with_capacity(tag.n_tag as usize);
                        let is_large = tag.flags & TD_TAG_LARGE != 0;
                        let p = unsafe {
                            tag.idx.as_ptr().add(if is_large {
                                std::mem::size_of::<u16>() * tag.n_tag as usize
                            } else {
                                std::mem::size_of::<u8>() * tag.n_tag as usize
                            }) as *const u8
                        };
                        for i in 0..tag.n_tag {
                            let offset = if is_large {
                                unsafe {
                                    *(tag.idx.as_ptr() as *const u16).add(i as usize) as usize
                                }
                            } else {
                                unsafe { *(tag.idx.as_ptr() as *const u8).add(i as usize) as usize }
                            };
                            let ptr = unsafe { p.add(offset) };
                            let is_json = tag.flags & TD_TAG_JSON != 0;

                            unsafe fn t_get_u32v(p: *const u8) -> (u32, usize) {
                                let mut n = 0usize;
                                let mut v = 0u32;

                                loop {
                                    if unsafe { *p.add(n) } <= 0x7f {
                                        v |= (unsafe { *p.add(n) } as u32) << (7 * n);
                                        n += 1;
                                        break;
                                    }
                                    v |= ((unsafe { *p.add(n) } & 0x7f) as u32) << (7 * n);
                                    n += 1;
                                }

                                (v, n)
                            }

                            if is_json {
                                // JSON tag value
                                let (len, n) = unsafe { t_get_u32v(ptr) };
                                dbg!(len, n);
                                let key = unsafe {
                                    std::str::from_utf8_unchecked(std::slice::from_raw_parts(
                                        ptr.add(n) as _,
                                        len as usize,
                                    ))
                                };
                                dbg!(key);

                                let t = unsafe { *(ptr.add(n + len as usize) as *const u8) };
                                dbg!(t);
                                let ty = crate::common::Ty::from(t);
                                dbg!(ty);
                                if ty.is_var_type() {
                                    let (len, n) = unsafe {
                                        t_get_u32v(ptr.add(n + len as usize + 1) as *const u8)
                                    };
                                    dbg!(len, n);
                                    let val = unsafe {
                                        std::slice::from_raw_parts(
                                            ptr.add(n + len as usize + 1 + n) as *const u8,
                                            len as usize,
                                        )
                                    };
                                    dbg!(val);
                                } else {
                                    let val = unsafe { ptr.add(n + len as usize + 1) };
                                    match ty {
                                        crate::common::Ty::Null => {}
                                        crate::common::Ty::Bool => {
                                            dbg!(unsafe { *val } != 0);
                                        }
                                        crate::common::Ty::TinyInt => {
                                            let val = unsafe { *(val as *const i8) };
                                            dbg!(val);
                                        }
                                        crate::common::Ty::SmallInt => {
                                            let val = unsafe { *(val as *const i16) };
                                            dbg!(val);
                                        }
                                        crate::common::Ty::Int => {
                                            let val = unsafe { *(val as *const i32) };
                                            dbg!(val);
                                        }
                                        crate::common::Ty::BigInt => {
                                            let val = unsafe { *(val as *const i64) };
                                            dbg!(val);
                                        }
                                        crate::common::Ty::UTinyInt => {
                                            let val = unsafe { *(val as *const u8) };
                                            dbg!(val);
                                        }
                                        crate::common::Ty::USmallInt => {
                                            let val = unsafe { *(val as *const u16) };
                                            dbg!(val);
                                        }
                                        crate::common::Ty::UInt => {
                                            let val = unsafe { *(val as *const u32) };
                                            dbg!(val);
                                        }
                                        crate::common::Ty::UBigInt => {
                                            let val = unsafe { *(val as *const u64) };
                                            dbg!(val);
                                        }
                                        crate::common::Ty::Float => {
                                            let val = unsafe { *(val as *const f32) };
                                            dbg!(val);
                                        }
                                        crate::common::Ty::Double => {
                                            let val =
                                                unsafe { (val as *const f64).read_unaligned() };
                                            dbg!(val);
                                        }
                                        crate::common::Ty::Timestamp => {
                                            let val =
                                                unsafe { (val as *const i64).read_unaligned() };
                                            dbg!(val);
                                        }
                                        _ => {
                                            unreachable!("{ty} is not supported in json tag value")
                                        }
                                    }
                                }
                            } else {
                                // normal tag value
                                let len = unsafe { *(ptr as *const u8) } as usize;
                                let val = unsafe { std::slice::from_raw_parts(ptr.add(1), len) };
                                values.push(STagVal {
                                    flags: tag.flags,
                                    len: len as i16,
                                    val: val.to_vec().try_into().unwrap_or_default(),
                                });
                            }
                        }
                        Ok(values)
                    }

                    if let Ok(v) = tag_to_values(&tag) {
                        dbg!(&v);
                    }
                    eprintln!("Child table's tag bytes: {:?}", tag);
                }
                let len = table_cursor.read_i32::<LE>()?;
                for i in 0..len {
                    let tag_name = table_cursor.read_to_faststr()?;
                    eprintln!("Child table's tag[{}] name: {}", i, tag_name);
                }
            }
            ETableType::TSDB_NORMAL_TABLE | ETableType::TSDB_VIRTUAL_NORMAL_TABLE => {
                // normal table
                let schemas = table_cursor.read_schemas()?;
                dbg!(&schemas);
                ncols = schemas.len();
            }
            _ => (),
        }
        if table_cursor.has_remaining() {
            let sql_len = table_cursor.read_i32::<LE>()?;
            if sql_len > 0 {
                let sql_bytes = table_cursor.read_u32v_sized_bytes()?;
                dbg!(&sql_bytes);
            }
            if matches!(
                table_type,
                ETableType::TSDB_NORMAL_TABLE | ETableType::TSDB_SUPER_TABLE
            ) {
                if table_cursor.has_remaining() {
                    table_cursor.read_compression_vec()?;
                }
            } else if matches!(
                table_type,
                ETableType::TSDB_VIRTUAL_NORMAL_TABLE | ETableType::TSDB_VIRTUAL_CHILD_TABLE
            ) {
                // virtual table does not have compression options
                if table_cursor.has_remaining() {
                    eprintln!(
                        "Warning: virtual table {:?} does not have compression options",
                        name
                    );
                    let refs = table_cursor.read_col_ref_vec()?;
                    dbg!(&refs);
                }
            }
            if table_cursor.has_remaining() {
                // read column references
                let refs = table_cursor.read_type_mod(ncols)?;
                dbg!(&refs);
            }
        }
        Ok(MetaCreateTable {
            name,
            uid,
            btime,
            ttl,
            comment,
        })
    }
}

#[derive(Debug)]
struct MetaView {
    inner: Bytes,
}

impl MetaView {
    /// Decode meta.
    ///
    /// ```c
    /// int32_t tDecodeMqMetaRsp(SDecoder *pDecoder, SMqMetaRsp *pRsp) {
    ///   TAOS_CHECK_RETURN(tDecodeSTqOffsetVal(pDecoder, &pRsp->rspOffset));
    ///   TAOS_CHECK_RETURN(tDecodeI16(pDecoder, &pRsp->resMsgType));
    ///   TAOS_CHECK_RETURN(tDecodeBinaryAlloc(pDecoder, &pRsp->metaRsp, (uint64_t *)&pRsp->metaRspLen));
    ///   return 0;
    /// }
    /// ```
    pub fn parse(&self) -> std::io::Result<()> {
        let mut cursor = Cursor::new(&self.inner);
        #[derive(Debug, KnownLayout, Immutable, FromBytes, Unaligned, Clone, Copy)]
        #[repr(C, packed(1))]
        struct SMqRspHead {
            mq_msg_type: i8,
            code: i32,
            epoch: i32,
            consumer_id: i64,
            walsver: i64,
            walever: i64,
        }
        let head = cursor.read_zerocopy::<SMqRspHead>()?;
        dbg!(&head, std::mem::size_of::<SMqRspHead>());
        let pos = cursor.position();
        dbg!(pos);

        // 1st. tDecodeSTqOffsetVal
        let mut type_id = cursor.read_i8()?;
        let mut offset_version = 0i8;
        if type_id > 0 {
            offset_version = type_id >> 4; // upper 4 bits for version
            type_id = type_id & 0x0F; // lower 4 bits for type
        }
        const TMQ_OFFSET__LOG: i8 = 1;
        const TMQ_OFFSET__SNAPSHOT_DATA: i8 = 0x02;
        const TMQ_OFFSET__SNAPSHOT_META: i8 = 0x03;
        const TQ_OFFSET_VERSION: i8 = 1;
        match type_id {
            TMQ_OFFSET__SNAPSHOT_DATA | TMQ_OFFSET__SNAPSHOT_META => {
                //
                let uid = cursor.read_i64::<LE>()?;
                let ts = cursor.read_i64::<LE>()?;
                dbg!(uid, ts);
                if offset_version > TQ_OFFSET_VERSION {
                    let primary_key_type = cursor.read_i8()?;
                    let ty = crate::common::Ty::from(primary_key_type);
                    if ty.is_var_type() {
                        let len = cursor.read_u64v()? as usize;
                        let primary_key = cursor.read_u32v_sized_bytes()?;
                        dbg!(primary_key_type, ty, len, primary_key);
                    } else {
                        let primary_key = cursor.read_i64::<LE>()?;
                        dbg!(primary_key_type, ty, primary_key);
                    }
                }
            }
            TMQ_OFFSET__LOG => {
                //
                let version = cursor.read_i64::<LE>()?;
                dbg!(version);
            }
            _ => {
                // do nothing
            }
        }

        // 2. tDecodeI16(pDecoder, &pRsp->resMsgType)
        let res_msg_type = cursor.read_i16::<LE>()?;
        dbg!(res_msg_type);

        // 3. tDecodeBinaryAlloc(pDecoder, &pRsp->metaRsp, (uint64_t *)&pRsp->metaRspLen)
        let bytes = cursor.read_u32v_sized_bytes()?;
        dbg!(&bytes);

        // Meta unit types
        const TDMT_VND_CREATE_STB: i16 = 531;
        const TDMT_VND_ALTER_STB: i16 = 533;
        const TDMT_VND_DROP_STB: i16 = 535;
        const TDMT_VND_CREATE_TABLE: i16 = 515;
        const TDMT_VND_ALTER_TABLE: i16 = 517;
        const TDMT_VND_DROP_TABLE: i16 = 519;
        const TDMT_VND_DELETE: i16 = 579;

        // Alter types
        const TSDB_ALTER_TABLE_ADD_TAG: i8 = 1;
        const TSDB_ALTER_TABLE_DROP_TAG: i8 = 2;
        const TSDB_ALTER_TABLE_UPDATE_TAG_NAME: i8 = 3;
        const TSDB_ALTER_TABLE_UPDATE_TAG_VAL: i8 = 4;
        const TSDB_ALTER_TABLE_ADD_COLUMN: i8 = 5;
        const TSDB_ALTER_TABLE_DROP_COLUMN: i8 = 6;
        const TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES: i8 = 7;
        const TSDB_ALTER_TABLE_UPDATE_TAG_BYTES: i8 = 8;
        const TSDB_ALTER_TABLE_UPDATE_OPTIONS: i8 = 9;
        const TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME: i8 = 10;
        const TSDB_ALTER_TABLE_ADD_TAG_INDEX: i8 = 11;
        const TSDB_ALTER_TABLE_DROP_TAG_INDEX: i8 = 12;
        const TSDB_ALTER_TABLE_UPDATE_COLUMN_COMPRESS: i8 = 13;
        const TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION: i8 = 14;
        const TSDB_ALTER_TABLE_UPDATE_MULTI_TAG_VAL: i8 = 15;
        const TSDB_ALTER_TABLE_ALTER_COLUMN_REF: i8 = 16;
        const TSDB_ALTER_TABLE_REMOVE_COLUMN_REF: i8 = 17;
        const TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COLUMN_REF: i8 = 18;
        #[derive(Debug, KnownLayout, Immutable, FromBytes, Unaligned, Clone, Copy)]
        #[repr(C, packed(1))]
        struct SMsgHead {
            cont_len: i32,
            vg_id: i32,
        }
        match res_msg_type {
            TDMT_VND_CREATE_STB | TDMT_VND_ALTER_STB => {
                // Create super table
                let mut cursor = Cursor::new(bytes);
                let head = cursor.read_zerocopy::<SMsgHead>()?;
                dbg!(&head);

                let len = cursor.read_i32::<LE>()?;
                dbg!(&len);
                let start_pos = cursor.position();
                dbg!(start_pos, cursor.position() - start_pos);

                let name = cursor.read_to_faststr()?;
                dbg!(&name, cursor.position() - start_pos);
                let suid = cursor.read_i64::<LE>()?;
                dbg!(suid, cursor.position() - start_pos);
                let rollup = cursor.read_i8()?;
                dbg!(rollup, cursor.position() - start_pos);

                // start columns
                let ncols = cursor.read_i32v()?;
                dbg!(ncols, cursor.position() - start_pos);
                let version = cursor.read_i32v()?;
                dbg!(version, cursor.position() - start_pos);
                for i in 0..ncols {
                    // decode schema
                    let f = cursor.read_field()?;
                    dbg!(&f);
                }
                dbg!("columns read at", cursor.position() - start_pos);
                // start tags
                let ntags = cursor.read_i32v()?;
                let version = cursor.read_i32v()?;
                for i in 0..ntags {
                    // decode schema
                    let f = cursor.read_field()?;
                    dbg!(&f);
                }
                dbg!("tags read at", cursor.position() - start_pos);

                if rollup > 0 {
                    // sma options
                    for i in 0..2 {
                        let max_delay = cursor.read_i64v()?;
                        let watermark = cursor.read_i64v()?;
                        let len = cursor.read_i32v()?;
                        if len > 0 {
                            let bytes = cursor.read_u32v_sized_bytes()?;
                            dbg!(&bytes);
                        }
                        dbg!(i, max_delay, watermark, len);
                    }
                }
                let alter_len = cursor.read_i32::<LE>()?;
                dbg!(alter_len, cursor.position() - start_pos);
                if alter_len > 0 {
                    let alter_ori_data = cursor.read_u32v_sized_bytes()?;
                    dbg!(&alter_ori_data, cursor.position() - start_pos);

                    let mut alter_cursor = Cursor::new(alter_ori_data);
                    let alter_len = alter_cursor.read_i32::<LE>()?;
                    let name = alter_cursor.read_to_faststr()?;
                    let alter_type = alter_cursor.read_i8()?;
                    let num = alter_cursor.read_i32::<LE>()?;
                    for i in 0..num {
                        if alter_type == TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION {
                            let ty = alter_cursor.read_i8()?;
                            let bytes = alter_cursor.read_i32::<LE>()?;
                            let name = alter_cursor.read_to_faststr()?;
                            let compress = alter_cursor.read_u32::<LE>()?;
                            dbg!(i, ty, bytes, name, compress);
                        } else {
                            let ty = alter_cursor.read_i8()?;
                            let bytes = alter_cursor.read_i32::<LE>()?;
                            let name = alter_cursor.read_to_faststr()?;
                            dbg!(i, ty, bytes, name);
                        }
                    }
                    // ttl
                    let ttl = alter_cursor.read_i32::<LE>()?;
                    let comment_len = alter_cursor.read_i32::<LE>()?;
                    if comment_len > 0 {
                        let comment = alter_cursor.read_to_faststr()?;
                        dbg!(comment_len, comment);
                    }
                    if let Ok(keep) = alter_cursor.read_i64::<LE>() {
                        dbg!(keep);
                    }
                    if let Ok(sql_len) = alter_cursor.read_i32::<LE>() {
                        if sql_len > 0 {
                            let sql = alter_cursor.read_u32v_sized_bytes()?;
                            dbg!(sql_len, sql);
                        }
                    }
                    if alter_cursor.has_remaining()
                        && matches!(
                            alter_type,
                            TSDB_ALTER_TABLE_ADD_COLUMN
                                | TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION
                        )
                    {
                        let has_type_mod = alter_cursor.read_i8()?;
                        if has_type_mod == 1 {
                            let type_mod = alter_cursor.read_i32v()?;
                            for i in 0..num {
                                // type mod is for decimal.
                                let type_mod = alter_cursor.read_i32v()?;
                                let precision = (type_mod >> 8) & 0xFF;
                                let scale = type_mod & 0xFF;
                                dbg!(precision, scale);
                            }
                        } else {
                            dbg!(has_type_mod);
                        }
                    }
                    if alter_cursor.has_remaining() {
                        let remaining = alter_cursor.read_bytes_to_end()?;
                        dbg!(remaining, alter_cursor.position() - start_pos);
                    }
                    // debug_assert!(!alter_cursor.has_remaining())
                }
                if let Ok(source) = cursor.read_i8() {
                    dbg!(source, cursor.position() - start_pos);
                }
                if let Ok(compressed) = cursor.read_i8() {
                    dbg!(compressed, cursor.position() - start_pos);
                }
                if cursor.has_remaining() {
                    let comp = cursor.read_compression_vec()?;
                    dbg!(comp, cursor.position() - start_pos);
                }
                if let Ok(keep) = cursor.read_i64::<LE>() {
                    dbg!(keep);
                }
                if let Ok(has_ext_schema) = cursor.read_i8() {
                    if has_ext_schema > 0 {
                        for i in 0..ncols {
                            // type mod is for decimal.
                            let type_mod = cursor.read_i32v()?;
                            let precision = (type_mod >> 8) & 0xFF;
                            let scale = type_mod & 0xFF;
                            dbg!(precision, scale);
                        }
                    }
                    //
                }
                if let Ok(is_virtual) = cursor.read_i8() {
                    dbg!(is_virtual);
                }
                assert!(!cursor.has_remaining());
            }

            TDMT_VND_DROP_STB => {
                // drop stable
                let mut cursor = Cursor::new(bytes);
                let head = cursor.read_zerocopy::<SMsgHead>()?;
                dbg!(&head);

                let len = cursor.read_i32::<LE>()?;
                dbg!(&len);
                let name = cursor.read_to_faststr()?;
                let suid = cursor.read_i64::<LE>()?;
                if cursor.has_remaining() {
                    dbg!(cursor.read_bytes_to_end()?);
                }
            }

            TDMT_VND_CREATE_TABLE => {
                // create table
                let mut cursor = Cursor::new(bytes);
                let head = cursor.read_zerocopy::<SMsgHead>()?;
                dbg!(&head);

                let len = cursor.read_i32::<LE>()?;
                let n = cursor.read_i32v()?;
                for i in 0..n {
                    let len = cursor.read_i32::<LE>()?;
                    let slice = cursor.get_ref().slice(
                        cursor.position() as usize..cursor.position() as usize + len as usize,
                    );
                    let name = cursor.read_to_faststr()?;
                }
            }
            TDMT_VND_ALTER_TABLE => {
                // alter table
                let mut cursor = Cursor::new(bytes);
                let head = cursor.read_zerocopy::<SMsgHead>()?;
                dbg!(&head);

                // **FROM**: tDecodeSVAlterTbReqCommon@tmsg.c
                let len = cursor.read_i32::<LE>()?;
                dbg!(&len);
                let name = cursor.read_to_faststr()?;
                let alter_type = cursor.read_i8()?;
                let col_id = cursor.read_i32::<LE>()?;
                match alter_type {
                    TSDB_ALTER_TABLE_ADD_COLUMN => {
                        let name = cursor.read_to_faststr()?;
                        let ty = cursor.read_i8()?;
                        let flags = cursor.read_i8()?;
                        let bytes = cursor.read_i32v()?;
                        dbg!(&name, ty, flags, bytes);
                    }
                    TSDB_ALTER_TABLE_DROP_COLUMN => {
                        let name = cursor.read_to_faststr()?;
                        dbg!(&name);
                    }
                    TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES => {
                        let name = cursor.read_to_faststr()?;
                        let col_mod_type = cursor.read_i8()?;
                        let bytes = cursor.read_i32v()?;
                        dbg!(&name, col_mod_type, bytes);
                    }
                    TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME => {
                        let name = cursor.read_to_faststr()?;
                        let new_name = cursor.read_to_faststr()?;
                        dbg!(&name, &new_name);
                    }
                    TSDB_ALTER_TABLE_UPDATE_TAG_VAL => {
                        let name = cursor.read_to_faststr()?;
                        let is_null = cursor.read_i8()?;
                        let tag_type = cursor.read_i8()?;
                        if is_null != 0 {
                            let val = cursor.read_u32v_sized_bytes()?;
                        }
                        dbg!(&name, is_null, tag_type);
                    }
                    TSDB_ALTER_TABLE_UPDATE_MULTI_TAG_VAL => {
                        let n = cursor.read_i32v()?;
                        for i in 0..n {
                            let col_id = cursor.read_i32v()?;
                            let tag_name = cursor.read_to_faststr()?;
                            let is_null = cursor.read_i8()?;
                            let tag_type = cursor.read_i8()?;
                            if is_null != 0 {
                                let val = cursor.read_u32v_sized_bytes()?;
                            }
                            let len = cursor.read_i32::<LE>()?; // length of the tag
                            if len <= 0 {
                                continue;
                            }
                            dbg!(i, col_id, &tag_name, is_null, tag_type, len);
                        }
                        dbg!(n);
                    }
                    TSDB_ALTER_TABLE_UPDATE_OPTIONS => {
                        let update_ttl = cursor.read_i8()?;
                        if update_ttl != 0 {
                            let ttl = cursor.read_i32v()?;
                            dbg!(ttl);
                        }
                        let new_comment_len = cursor.read_i32v()?;
                        if new_comment_len > 0 {
                            let new_comment = cursor.read_to_faststr()?;
                            dbg!(&new_comment);
                        }
                    }
                    TSDB_ALTER_TABLE_UPDATE_COLUMN_COMPRESS => {
                        let col_name = cursor.read_to_faststr()?;
                        let compress = cursor.read_u32::<LE>()?;
                        dbg!(&col_name, compress);
                    }
                    TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION => {
                        let col_name = cursor.read_to_faststr()?;
                        let ty = cursor.read_i8()?;
                        let flags = cursor.read_i8()?;
                        let bytes = cursor.read_i32v()?;
                        let compress = cursor.read_u32::<LE>()?;
                        dbg!(&col_name, ty, flags, bytes, compress);
                    }
                    TSDB_ALTER_TABLE_ALTER_COLUMN_REF => {
                        let col_name = cursor.read_to_faststr()?;
                        let ref_db_name = cursor.read_to_faststr()?;
                        let ref_tb_name = cursor.read_to_faststr()?;
                        let ref_col_name = cursor.read_to_faststr()?;
                        dbg!(&col_name, &ref_db_name, &ref_tb_name, &ref_col_name);
                    }
                    TSDB_ALTER_TABLE_REMOVE_COLUMN_REF => {
                        let col_name = cursor.read_to_faststr()?;
                        dbg!(&col_name);
                    }
                    TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COLUMN_REF => {
                        let col_name = cursor.read_to_faststr()?;
                        let ty = cursor.read_i8()?;
                        let flags = cursor.read_i8()?;
                        let bytes = cursor.read_i32v()?;
                        let ref_db_name = cursor.read_to_faststr()?;
                        let ref_tb_name = cursor.read_to_faststr()?;
                        let ref_col_name = cursor.read_to_faststr()?;
                        dbg!(&col_name, &ref_db_name, &ref_tb_name, &ref_col_name);
                    }
                    TSDB_ALTER_TABLE_UPDATE_OPTIONS => {}
                    _ => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Unsupported alter type: {}", alter_type),
                        ));
                    }
                }
                let c_time_ms = cursor.read_i64::<LE>().ok();
                let source = cursor.read_i8().ok();
                if alter_type == TSDB_ALTER_TABLE_ADD_COLUMN
                    || alter_type == TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION
                {
                    let type_mod = cursor.read_i32::<LE>().ok();
                }
                if alter_type == TSDB_ALTER_TABLE_UPDATE_OPTIONS {
                    // TODO: DO NOTHING WHEN WRITING THE UPDATE_OPTIONS META.
                }
            }
            TDMT_VND_DROP_TABLE => {
                // drop table
                let mut cursor = Cursor::new(bytes);
                let head = cursor.read_zerocopy::<SMsgHead>()?;
                dbg!(&head);
                let len = cursor.read_i32::<LE>()?;
                let n = cursor.read_i32v()?;
                for i in 0..n {
                    let len = cursor.read_i32::<LE>()?;
                    let name = cursor.read_to_faststr()?;
                    let suid = cursor.read_u64::<LE>()?;
                    let uid = cursor.read_i64::<LE>()?;
                    let ig_not_exists = cursor.read_i8()?;
                    let is_virtual = cursor.read_i8().unwrap_or(0);
                    dbg!(i, len, &name, suid, uid, ig_not_exists, is_virtual);
                }
            }
            TDMT_VND_DELETE => {
                // delete from table

                let mut cursor = Cursor::new(bytes);
                let head = cursor.read_zerocopy::<SMsgHead>()?;
                dbg!(&head);
                let suid = cursor.read_u64::<LE>()?;
                let n = cursor.read_i32v()?;
                for i in 0..n {
                    let tuid = cursor.read_u64::<LE>()?;
                }
                let skey = cursor.read_i64::<LE>()?;
                let ekey = cursor.read_i64::<LE>()?;
                let affected_rows = cursor.read_i64v()?;
                let table_name = cursor.read_to_faststr()?;
                let col_name = cursor.read_to_faststr()?;

                if let Ok(ctime_ms) = cursor.read_i64::<LE>() {
                    let ts = Timestamp::new(ctime_ms, crate::common::Precision::Millisecond);
                    dbg!(ts);
                }
                if let Ok(source) = cursor.read_i8() {
                    dbg!(source);
                }
            }
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Unsupported res_msg_type: {}", res_msg_type),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use byteorder::WriteBytesExt;

    use super::super::RawData;
    use super::*;
    use std::io::{Cursor, Write};
    #[test]
    fn test_raw_data_extract() {
        let bytes = include_bytes!("../../../../tests/dump/raw_4_metadata.bin");
        let mut multi_bytes = Vec::new();
        multi_bytes.write_i32::<LE>(0).unwrap();
        multi_bytes.write_u32::<LE>(0).unwrap();
        multi_bytes.write_u64::<LE>(0).unwrap();
        multi_bytes.write_u16::<LE>(0).unwrap();
        let mut cursor = Cursor::new(bytes);
        let raw: RawData = Inlinable::read_inlined(&mut cursor).unwrap();
        multi_bytes.write_u32::<LE>(raw.raw_len() as _).unwrap();
        multi_bytes.extend_from_slice(raw.raw_slice());
        let v = RawBlock::parse_from_multi_raw_block(multi_bytes).unwrap();
        for b in v {
            println!("Block: {}", b.pretty_format());
        }
        // let blocks = raw.to_blocks().unwrap();
        let cursor = raw.raw_slice();
        let mut cursor = Cursor::new(cursor);
        let blocks = cursor.parse_raw_data_with_type(raw.raw_type()).unwrap();
        assert_eq!(blocks.len(), 1);
        for block in blocks {
            println!("Block: {}", block.pretty_format());
        }

        let bytes = include_bytes!("../../../../tests/dump/raw_4_metadata.bin");
        let mut cursor = Cursor::new(bytes);
        let raw: RawData = Inlinable::read_inlined(&mut cursor).unwrap();
        // let blocks = raw.to_blocks().unwrap();
        let cursor = raw.raw_slice();
        let mut cursor = Cursor::new(cursor);
        let blocks = cursor.parse_raw_data_with_type(raw.raw_type()).unwrap();
        assert_eq!(blocks.len(), 1);
        for block in blocks {
            println!("Block: {}", block.pretty_format());
        }

        let bytes = include_bytes!("../../../../tests/dump/raw_5_data.bin");
        let mut cursor = Cursor::new(bytes);
        let raw: RawData = Inlinable::read_inlined(&mut cursor).unwrap();
        let cursor = raw.raw_slice();
        let mut cursor = Cursor::new(cursor);
        let blocks = cursor.parse_raw_data_with_type(raw.raw_type()).unwrap();
        assert_eq!(blocks.len(), 1);
        for block in blocks {
            println!("Block: {}", block.pretty_format());
        }
    }

    #[test]
    fn test_raw_data_create_stable() {
        // 0. stable
        let bytes = include_bytes!("../../../../tests/dump/raw_0_meta.bin");
        let mut cursor = Cursor::new(bytes);
        let raw: RawData = Inlinable::read_inlined(&mut cursor).unwrap();
        let cursor = raw.raw_slice();
        let mut cursor = Cursor::new(cursor);
        // let blocks = cursor.parse_raw_data_with_type(raw.raw_type()).unwrap();
        let metas = cursor.to_meta_vec(raw.raw_type()).unwrap();
        for meta in metas {
            println!("Block: {:?}", meta);
            meta.parse().unwrap();
        }
        //  1. stable with primary key
        let bytes = include_bytes!("../../../../tests/dump/raw_1_meta.bin");
        let mut cursor = Cursor::new(bytes);
        let raw: RawData = Inlinable::read_inlined(&mut cursor).unwrap();
        let cursor = raw.raw_slice();
        let mut cursor = Cursor::new(cursor);
        // let blocks = cursor.parse_raw_data_with_type(raw.raw_type()).unwrap();
        let metas = cursor.to_meta_vec(raw.raw_type()).unwrap();
        for meta in metas {
            println!("Block: {:?}", meta);
            meta.parse().unwrap();
        }
    }

    #[test]
    fn test_raw_parse_alter_table() {
        println!("alter table st1 add column v4 int level 'high' compress 'zstd'");
        //  6. stable with primary key
        let bytes = include_bytes!("../../../../tests/dump/raw_6_meta.bin");
        let mut cursor = Cursor::new(bytes);
        let raw: RawData = Inlinable::read_inlined(&mut cursor).unwrap();
        let cursor = raw.raw_slice();
        let mut cursor = Cursor::new(cursor);
        let metas = cursor.to_meta_vec(raw.raw_type()).unwrap();
        for meta in metas {
            println!("Block: {:?}", meta);
            meta.parse().unwrap();
        }
    }
    #[test]
    fn test_raw_data_create_table() {
        let files = std::fs::read_dir("tests/dump")
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.file_name().to_str().unwrap().starts_with("raw_"))
            .collect::<Vec<_>>();
        for f in &files {
            let bytes = std::fs::read(f.path()).unwrap();
            let mut cursor = Cursor::new(bytes);
            let raw: RawData = Inlinable::read_inlined(&mut cursor).unwrap();
            let cursor = raw.raw_slice();
            let mut cursor = Cursor::new(cursor);
            let metas = cursor.to_meta_vec(raw.raw_type()).unwrap();
            for meta in metas {
                println!("Block: {:?}", meta);
                meta.parse().unwrap();
            }
        }
    }
}
