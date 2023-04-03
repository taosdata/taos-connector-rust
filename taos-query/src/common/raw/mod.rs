use crate::common::{BorrowedValue, Field, Precision, Ty, Value};

use bytes::Bytes;
use itertools::Itertools;

use serde::Deserialize;

use std::{
    cell::{Cell, RefCell, UnsafeCell},
    ffi::c_void,
    fmt::Debug,
    fmt::Display,
    ops::Deref,
    ptr::NonNull,
    sync::Arc,
};

pub mod layout;
pub mod meta;

mod data;

use layout::Layout;

pub mod views;

pub use views::ColumnView;
use views::*;

pub use data::*;
pub use meta::*;

mod de;
mod rows;
pub use rows::*;

#[derive(Debug, Clone, Copy)]
#[repr(C, packed(1))]
struct Header {
    version: u32,
    length: u32,
    nrows: u32,
    ncols: u32,
    flag: u32,
    group_id: u64,
}

impl Default for Header {
    fn default() -> Self {
        Self {
            version: 1,
            length: Default::default(),
            nrows: Default::default(),
            ncols: Default::default(),
            flag: u32::MAX,
            group_id: Default::default(),
        }
    }
}

impl Header {
    fn as_bytes(&self) -> &[u8] {
        unsafe {
            let ptr = self as *const Self;
            let len = std::mem::size_of::<Self>();
            std::slice::from_raw_parts(ptr as *const u8, len)
        }
    }

    fn len(&self) -> usize {
        self.length as _
    }

    fn nrows(&self) -> usize {
        self.nrows as _
    }
    fn ncols(&self) -> usize {
        self.ncols as _
    }
}

/// Raw data block format (B for bytes):
///
/// ```text,ignore
/// +-----+----------+---------------+-----------+-----------------------+-----------------+
/// | header | col_schema... | length... | (bitmap or offsets    | col data)   ... |
/// | 28 B   | (1+4)B * cols | 4B * cols | (row+7)/8 or 4 * rows | length[col] ... |
/// +-----+----------+---------------+-----------+-----------------------+-----------------+
/// ```
///
/// The length of bitmap is decided by number of rows of this data block, and the length of each column data is
/// recorded in the first segment, next to the struct header
// #[derive(Debug)]
pub struct RawBlock {
    /// Layout is auto detected.
    layout: Arc<RefCell<Layout>>,
    /// Raw bytes version, may be v2 or v3.
    version: Version,
    /// Data is required, which could be v2 websocket block or a v3 raw block.
    data: Cell<Bytes>,
    /// Number of rows in current data block.
    rows: usize,
    /// Number of columns (or fields) in current data block.
    cols: usize,
    /// Timestamp precision in current data block.
    precision: Precision,
    /// Database name, if is tmq message.
    database: Option<String>,
    /// Table name of current data block.
    table: Option<String>,
    /// Field names of current data block.
    fields: Vec<String>,
    /// Group id in current data block, it always be 0 in v2 block, and be meaningful in v3.
    group_id: u64,
    /// Column schemas of current data block, contains only data type and the length defined in `create table`.
    schemas: Schemas,
    /// Data lengths collection for all columns.
    lengths: Lengths,
    /// A vector of [ColumnView] that represent column of values efficiently.
    columns: Vec<ColumnView>,
}

unsafe impl Send for RawBlock {}
unsafe impl Sync for RawBlock {}

impl Debug for RawBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // todo: more helpful debug impl.
        f.debug_struct("Raw")
            .field("layout", &self.layout)
            .field("version", &self.version)
            .field("data", &"...")
            .field("rows", &self.rows)
            .field("cols", &self.cols)
            .field("precision", &self.precision)
            .field("table", &self.table)
            .field("fields", &self.fields)
            // .field("raw_fields", &self.raw_fields)
            .field("group_id", &self.group_id)
            .field("schemas", &self.schemas)
            .field("lengths", &self.lengths)
            .field("columns", &self.columns)
            .finish()
    }
}

impl RawBlock {
    pub unsafe fn parse_from_ptr(ptr: *mut c_void, precision: Precision) -> Self {
        let header = &*(ptr as *const Header);
        let len = header.length as usize;
        let bytes = std::slice::from_raw_parts(ptr as *const u8, len);
        let bytes = Bytes::from(bytes.to_vec());
        Self::parse_from_raw_block(bytes, precision).with_layout(Layout::default())
    }

    pub fn parse_from_ptr_v2(
        ptr: *const *const c_void,
        fields: &[Field],
        lengths: &[u32],
        rows: usize,
        precision: Precision,
    ) -> Self {
        let mut bytes = Vec::new();
        for i in 0..fields.len() {
            unsafe {
                let slice = ptr.offset(i as _).read();
                bytes.extend_from_slice(std::slice::from_raw_parts(
                    slice as *const u8,
                    lengths[i] as usize * rows,
                ));
            }
        }
        Self::parse_from_raw_block_v2(bytes, fields, lengths, rows, precision)
    }

    pub fn parse_from_raw_block_v2(
        bytes: impl Into<Bytes>,
        fields: &[Field],
        lengths: &[u32],
        rows: usize,
        precision: Precision,
    ) -> Self {
        use bytes::BufMut;
        debug_assert_eq!(fields.len(), lengths.len());

        #[inline(always)]
        fn bool_is_null(v: *const bool) -> bool {
            unsafe { (v as *const u8).read_unaligned() == 0x02 }
        }
        #[inline(always)]
        fn tiny_int_is_null(v: *const i8) -> bool {
            unsafe { (v as *const u8).read_unaligned() == 0x80 }
        }
        #[inline(always)]
        fn small_int_is_null(v: *const i16) -> bool {
            unsafe { (v as *const u16).read_unaligned() == 0x8000 }
        }
        #[inline(always)]
        fn int_is_null(v: *const i32) -> bool {
            unsafe { (v as *const u32).read_unaligned() == 0x80000000 }
        }
        #[inline(always)]
        fn big_int_is_null(v: *const i64) -> bool {
            unsafe { (v as *const u64).read_unaligned() == 0x8000000000000000 }
        }
        #[inline(always)]
        fn u_tiny_int_is_null(v: *const u8) -> bool {
            unsafe { (v as *const u8).read_unaligned() == 0xFF }
        }
        #[inline(always)]
        fn u_small_int_is_null(v: *const u16) -> bool {
            unsafe { (v as *const u16).read_unaligned() == 0xFFFF }
        }
        #[inline(always)]
        fn u_int_is_null(v: *const u32) -> bool {
            unsafe { (v as *const u32).read_unaligned() == 0xFFFFFFFF }
        }
        #[inline(always)]
        fn u_big_int_is_null(v: *const u64) -> bool {
            unsafe { (v as *const u64).read_unaligned() == 0xFFFFFFFFFFFFFFFF }
        }
        #[inline(always)]
        fn float_is_null(v: *const f32) -> bool {
            unsafe { (v as *const u32).read_unaligned() == 0x7FF00000 }
        }
        #[inline(always)]
        fn double_is_null(v: *const f64) -> bool {
            unsafe { (v as *const u64).read_unaligned() == 0x7FFFFF0000000000 }
        }

        // const BOOL_NULL: u8 = 0x2;
        // const TINY_INT_NULL: i8 = i8::MIN;
        // const SMALL_INT_NULL: i16 = i16::MIN;
        // const INT_NULL: i32 = i32::MIN;
        // const BIG_INT_NULL: i64 = i64::MIN;
        // const FLOAT_NULL: f32 = 0x7FF00000i32 as f32;
        // const DOUBLE_NULL: f64 = 0x7FFFFF0000000000i64 as f64;
        // const U_TINY_INT_NULL: u8 = u8::MAX;
        // const U_SMALL_INT_NULL: u16 = u16::MAX;
        // const U_INT_NULL: u32 = u32::MAX;
        // const U_BIG_INT_NULL: u64 = u64::MAX;

        let layout = Arc::new(RefCell::new(
            Layout::INLINE_DEFAULT.with_schema_changed().into(),
        ));

        let bytes = bytes.into();
        let cols = fields.len();

        let mut schemas_bytes =
            bytes::BytesMut::with_capacity(rows * std::mem::size_of::<ColSchema>());
        fields
            .iter()
            .for_each(|f| schemas_bytes.put(f.to_column_schema().as_bytes()));
        let schemas = Schemas::from(schemas_bytes);

        let mut data_lengths = LengthsMut::new(cols);

        let mut columns = Vec::new();

        let mut offset = 0;

        for (i, (field, length)) in fields.into_iter().zip(&*lengths).enumerate() {
            macro_rules! _primitive_view {
                ($ty:ident, $prim:ty) => {{
                    debug_assert_eq!(field.bytes(), *length);
                    // column start
                    let start = offset;
                    // column end
                    offset += rows * std::mem::size_of::<$prim>() as usize;
                    // byte slice from start to end: `[start, end)`.
                    let data = bytes.slice(start..offset);
                    let nulls = NullBits::from_iter((0..rows).map(|row| unsafe {
                        paste::paste!{ [<$ty:snake _is_null>] (
                            data
                                .as_ptr()
                                .offset(row as isize * std::mem::size_of::<$prim>() as isize)
                                as *const $prim,
                        ) }
                    }));
                    // value as target type
                    // let value_slice = unsafe {
                    //     std::slice::from_raw_parts(
                    //         transmute::<*const u8, *const $prim>(data.as_ptr()),
                    //         rows,
                    //     )
                    // };
                    // Set data lengths for v3-compatible block.
                    data_lengths[i] = data.len() as u32;

                    // generate nulls bitmap.
                    // let nulls = NullsMut::from_bools(
                    //     value_slice
                    //         .iter()
                    //         .map(|v| paste::paste!{ [<$ty:snake _is_null>](v as _) })
                    //         // .map(|b| *b as u64 == paste::paste! { [<$ty:snake:upper _NULL>] }),
                    // )
                    // .into_nulls();
                    // build column view
                    let column = paste::paste! { ColumnView::$ty([<$ty View>] { nulls, data }) };
                    columns.push(column);
                }};
            }

            match field.ty() {
                Ty::Null => unreachable!(),

                // Signed integers columns.
                Ty::Bool => _primitive_view!(Bool, bool),
                Ty::TinyInt => _primitive_view!(TinyInt, i8),
                Ty::SmallInt => _primitive_view!(SmallInt, i16),
                Ty::Int => _primitive_view!(Int, i32),
                Ty::BigInt => _primitive_view!(BigInt, i64),
                // Unsigned integers columns.
                Ty::UTinyInt => _primitive_view!(UTinyInt, u8),
                Ty::USmallInt => _primitive_view!(USmallInt, u16),
                Ty::UInt => _primitive_view!(UInt, u32),
                Ty::UBigInt => _primitive_view!(UBigInt, u64),
                // Float columns.
                Ty::Float => _primitive_view!(Float, f32),
                Ty::Double => _primitive_view!(Double, f64),
                Ty::VarChar => {
                    let start = offset;
                    offset += *length as usize * rows;
                    let data = bytes.slice(start..offset);
                    let data_ptr = data.as_ptr();

                    let offsets = Offsets::from_offsets((0..rows).into_iter().map(|row| unsafe {
                        let offset = row as i32 * *length as i32;
                        let ptr = data_ptr.offset(offset as isize);
                        let len = (ptr as *const u16).read_unaligned();
                        if len == 1 && *ptr.offset(2) == 0xFF {
                            -1
                        } else {
                            offset
                        }
                    }));

                    columns.push(ColumnView::VarChar(VarCharView { offsets, data }));

                    data_lengths[i] = *length as u32 * rows as u32;
                }
                Ty::Timestamp => {
                    // column start
                    let start = offset;
                    // column end
                    offset += rows * std::mem::size_of::<i64>() as usize;
                    // byte slice from start to end: `[start, end)`.
                    let data = bytes.slice(start..offset);
                    let nulls = NullBits::from_iter((0..rows).map(|row| unsafe {
                        big_int_is_null(
                            &(data
                                .as_ptr()
                                .offset(row as isize * std::mem::size_of::<i64>() as isize)
                                as *const i64)
                                .read_unaligned() as _,
                        )
                    }));
                    // Set data lengths for v3-compatible block.
                    data_lengths[i] = data.len() as u32;

                    // build column view
                    let column = ColumnView::Timestamp(TimestampView {
                        nulls,
                        data,
                        precision,
                    });
                    columns.push(column);
                }
                Ty::NChar => {
                    let start = offset;
                    offset += *length as usize * rows;
                    let data = bytes.slice(start..offset);
                    let data_ptr = data.as_ptr();

                    let offsets = Offsets::from_offsets((0..rows).into_iter().map(|row| unsafe {
                        let offset = row as i32 * *length as i32;
                        let ptr = data_ptr.offset(offset as isize);
                        let len = (ptr as *const u16).read_unaligned();
                        if len == 4 && (ptr.offset(2) as *const u32).read_unaligned() == 0xFFFFFFFF
                        {
                            -1
                        } else {
                            offset
                        }
                    }));

                    columns.push(ColumnView::NChar(NCharView {
                        offsets,
                        data,
                        is_chars: UnsafeCell::new(false),
                        version: Version::V2,
                        layout: layout.clone(),
                    }));

                    data_lengths[i] = *length as u32 * rows as u32;
                }
                Ty::Json => {
                    let start = offset;
                    offset += *length as usize * rows;
                    let data = bytes.slice(start..offset);
                    let data_ptr = data.as_ptr();

                    let offsets = Offsets::from_offsets((0..rows).into_iter().map(|row| unsafe {
                        let offset = row as i32 * *length as i32;
                        let ptr = data_ptr.offset(offset as isize);
                        let len = (ptr as *const u16).read_unaligned();
                        if len == 4 && (ptr.offset(2) as *const u32).read_unaligned() == 0xFFFFFFFF
                        {
                            -1
                        } else {
                            offset
                        }
                    }));

                    columns.push(ColumnView::Json(JsonView { offsets, data }));

                    data_lengths[i] = *length as u32 * rows as u32;
                }
                Ty::VarBinary => todo!(),
                Ty::Decimal => todo!(),
                Ty::Blob => todo!(),
                Ty::MediumBlob => todo!(),
            }
        }

        Self {
            layout,
            version: Version::V2,
            data: Cell::new(bytes),
            rows,
            cols,
            schemas,
            lengths: data_lengths.into_lengths(),
            precision,
            database: None,
            table: None,
            fields: fields.iter().map(|s| s.name().to_string()).collect(),
            columns,
            group_id: 0,
            // raw_fields: Vec::new(),
        }
    }

    pub fn parse_from_raw_block(bytes: impl Into<Bytes>, precision: Precision) -> Self {
        let schema_start: usize = std::mem::size_of::<Header>();

        let layout = Arc::new(RefCell::new(Layout::INLINE_DEFAULT.into()));

        let bytes = bytes.into();
        let ptr = bytes.as_ptr();

        let header = unsafe { &*(ptr as *const Header) };

        let rows = header.nrows();
        let cols = header.ncols();
        let len = header.len();
        debug_assert_eq!(bytes.len(), len);
        let group_id = header.group_id as u64;

        let schema_end = schema_start + cols * std::mem::size_of::<ColSchema>();
        let schemas = Schemas::from(bytes.slice(schema_start..schema_end));
        // dbg!(&schemas);
        let lengths_end = schema_end + std::mem::size_of::<u32>() * cols;
        let lengths = Lengths::from(bytes.slice(schema_end..lengths_end));
        // dbg!(&lengths);
        let mut data_offset = lengths_end;
        let mut columns = Vec::with_capacity(cols);
        for col in 0..cols {
            // go for each column
            let length = unsafe { lengths.get_unchecked(col) } as usize;
            let schema = unsafe { schemas.get_unchecked(col) };

            macro_rules! _primitive_value {
                ($ty:ident, $prim:ty) => {{
                    let o1 = data_offset;
                    let o2 = data_offset + ((rows + 7) >> 3); // null bitmap len.
                    data_offset = o2 + rows * std::mem::size_of::<$prim>();
                    let nulls = bytes.slice(o1..o2);
                    let data = bytes.slice(o2..data_offset);
                    ColumnView::$ty(paste::paste! {[<$ty View>] {
                        nulls: NullBits(nulls),
                        data,
                    }})
                }};
            }

            let column = match schema.ty {
                Ty::Null => unreachable!("raw block does not contains type NULL"),
                Ty::Bool => _primitive_value!(Bool, i8),
                Ty::TinyInt => _primitive_value!(TinyInt, i8),
                Ty::SmallInt => _primitive_value!(SmallInt, i16),
                Ty::Int => _primitive_value!(Int, i32),
                Ty::BigInt => _primitive_value!(BigInt, i64),
                Ty::Float => _primitive_value!(Float, f32),
                Ty::Double => _primitive_value!(Double, f64),
                Ty::VarChar => {
                    let o1 = data_offset;
                    let o2 = data_offset + std::mem::size_of::<i32>() * rows;
                    data_offset = o2 + length;

                    let offsets = Offsets::from(bytes.slice(o1..o2));
                    let data = bytes.slice(o2..data_offset);

                    ColumnView::VarChar(VarCharView { offsets, data })
                }
                Ty::Timestamp => {
                    let o1 = data_offset;
                    let o2 = data_offset + ((rows + 7) >> 3);
                    data_offset = o2 + rows * std::mem::size_of::<i64>();
                    let nulls = bytes.slice(o1..o2);
                    let data = bytes.slice(o2..data_offset);
                    ColumnView::Timestamp(TimestampView {
                        nulls: NullBits(nulls),
                        data,
                        precision: precision,
                    })
                }
                Ty::NChar => {
                    let o1 = data_offset;
                    let o2 = data_offset + std::mem::size_of::<i32>() * rows;
                    data_offset = o2 + length;

                    let offsets = Offsets::from(bytes.slice(o1..o2));
                    let data = bytes.slice(o2..data_offset);

                    ColumnView::NChar(NCharView {
                        offsets,
                        data,
                        is_chars: UnsafeCell::new(true),
                        version: Version::V3,
                        layout: layout.clone(),
                    })
                }
                Ty::UTinyInt => _primitive_value!(UTinyInt, u8),
                Ty::USmallInt => _primitive_value!(USmallInt, u16),
                Ty::UInt => _primitive_value!(UInt, u32),
                Ty::UBigInt => _primitive_value!(UBigInt, u64),
                Ty::Json => {
                    let o1 = data_offset;
                    let o2 = data_offset + std::mem::size_of::<i32>() * rows;
                    data_offset = o2 + length;

                    let offsets = Offsets::from(bytes.slice(o1..o2));
                    let data = bytes.slice(o2..data_offset);

                    ColumnView::Json(JsonView { offsets, data })
                }
                ty => {
                    unreachable!("unsupported type: {ty}")
                }
            };
            columns.push(column);
            debug_assert!(data_offset <= len);
        }
        RawBlock {
            layout,
            version: Version::V3,
            data: Cell::new(bytes),
            rows,
            cols,
            precision,
            group_id,
            schemas,
            lengths,
            database: None,
            table: None,
            fields: Vec::new(),
            columns,
        }
    }

    /// Set table name of the block
    pub fn with_database_name(&mut self, name: impl Into<String>) -> &mut Self {
        self.database = Some(name.into());
        self
    }
    /// Set table name of the block
    pub fn with_table_name(&mut self, name: impl Into<String>) -> &mut Self {
        self.table = Some(name.into());
        self.layout.borrow_mut().with_table_name();

        self
    }

    /// Set field names of the block
    pub fn with_field_names<S: Into<String>, I: IntoIterator<Item = S>>(
        &mut self,
        names: I,
    ) -> &mut Self {
        self.fields = names.into_iter().map(|name| name.into()).collect();
        self.layout.borrow_mut().with_field_names();
        self
    }

    fn with_layout(mut self, layout: Layout) -> Self {
        self.layout = Arc::new(RefCell::new(layout));
        self
    }

    /// Number of columns
    #[inline]
    pub fn ncols(&self) -> usize {
        self.columns.len()
    }

    /// Number of rows
    #[inline]
    pub fn nrows(&self) -> usize {
        self.rows
    }

    /// Precision for current block.
    #[inline]
    pub const fn precision(&self) -> Precision {
        self.precision
    }

    #[inline]
    pub const fn group_id(&self) -> u64 {
        self.group_id
    }

    #[inline]
    pub fn table_name(&self) -> Option<&str> {
        self.table.as_ref().map(|s| s.as_str())
    }

    // todo: db name?
    #[inline]
    pub fn tmq_db_name(&self) -> Option<&str> {
        self.database.as_ref().map(|s| s.as_str())
    }

    #[inline]
    pub fn schemas(&self) -> &[ColSchema] {
        &self.schemas
    }

    /// Get field names.
    pub fn field_names(&self) -> &[String] {
        &self.fields
    }

    /// Data view in columns.
    #[inline]
    pub fn columns(&self) -> std::slice::Iter<ColumnView> {
        self.columns.iter()
    }

    pub fn column_views(&self) -> &[ColumnView] {
        &self.columns
    }

    /// Data view in rows.
    #[inline]
    pub fn rows<'a>(&self) -> RowsIter<'a> {
        RowsIter {
            raw: NonNull::new(self as *const Self as *mut Self).unwrap(),
            row: 0,
            _marker: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn into_rows<'a>(self) -> IntoRowsIter<'a>
    where
        Self: 'a,
    {
        IntoRowsIter {
            raw: self,
            row: 0,
            _marker: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn deserialize<'de, 'a: 'de, T>(
        &'a self,
    ) -> std::iter::Map<rows::RowsIter<'_>, fn(RowView<'a>) -> Result<T, DeError>>
    where
        T: Deserialize<'de>,
    {
        self.rows().map(|mut row| T::deserialize(&mut row))
    }

    pub fn as_raw_bytes(&self) -> &[u8] {
        if self.layout.borrow().schema_changed() {
            let bytes = views_to_raw_block(&self.columns);
            let bytes = bytes.into();
            self.data.replace(bytes);
            self.layout.borrow_mut().set_schema_changed(false);
            unsafe { &*self.data.as_ptr() }
        } else {
            unsafe { &(*self.data.as_ptr()) }
        }
    }

    pub fn is_null(&self, row: usize, col: usize) -> bool {
        if row >= self.nrows() || col >= self.ncols() {
            return true;
        }
        unsafe { self.columns.get_unchecked(col).is_null_unchecked(row) }
    }

    #[inline]
    /// Get one value at `(row, col)` of the block.
    pub unsafe fn get_raw_value_unchecked(
        &self,
        row: usize,
        col: usize,
    ) -> (Ty, u32, *const c_void) {
        let view = self.columns.get_unchecked(col);
        view.get_raw_value_unchecked(row)
    }

    pub fn get_ref(&self, row: usize, col: usize) -> Option<BorrowedValue> {
        if row >= self.nrows() || col >= self.ncols() {
            return None;
        }
        Some(unsafe { self.get_ref_unchecked(row, col) })
    }

    #[inline]
    /// Get one value at `(row, col)` of the block.
    pub unsafe fn get_ref_unchecked(&self, row: usize, col: usize) -> BorrowedValue {
        self.columns.get_unchecked(col).get_ref_unchecked(row)
    }

    // unsafe fn get_col_unchecked(&self, col: usize) -> &ColumnView {
    //     self.columns.get_unchecked(col)
    // }

    pub fn to_values(&self) -> Vec<Vec<Value>> {
        self.rows().map(|row| row.into_values()).collect_vec()
    }

    pub fn write<W: std::io::Write>(&self, _wtr: W) -> std::io::Result<usize> {
        todo!()
    }

    fn layout(&self) -> Layout {
        Layout::from_bits(self.layout.borrow().as_inner()).unwrap()
    }

    pub fn fields(&self) -> Vec<Field> {
        self.schemas()
            .iter()
            .zip(self.field_names())
            .map(|(schema, name)| Field::new(name, schema.ty, schema.len))
            .collect_vec()
    }

    pub fn pretty_format(&self) -> PrettyBlock {
        PrettyBlock::new(self)
    }

    // pub fn fields_iter(&self) -> impl Iterator<Item = Field> + '_ {
    //     self.schemas()
    //         .iter()
    //         .zip(self.field_names())
    //         .map(|(schema, name)| Field::new(name, schema.ty, schema.len))
    // }

    pub fn to_create(&self) -> Option<MetaCreate> {
        self.table_name().map(|table_name| MetaCreate::Normal {
            table_name: table_name.to_string(),
            columns: self.fields(),
        })
    }
}

pub struct PrettyBlock<'a> {
    raw: &'a RawBlock,
}

impl<'a> PrettyBlock<'a> {
    fn new(raw: &'a RawBlock) -> Self {
        Self { raw }
    }
}

impl<'a> Deref for PrettyBlock<'a> {
    type Target = RawBlock;
    fn deref(&self) -> &Self::Target {
        &self.raw
    }
}

impl<'a> Display for PrettyBlock<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use prettytable::{Row, Table};
        let mut table = Table::new();
        writeln!(
            f,
            "Table view with {} rows, {} columns",
            self.nrows(),
            self.ncols()
        )?;
        table.set_titles(Row::from_iter(self.field_names()));
        for row in self.raw.rows() {
            table.add_row(Row::from_iter(
                row.map(|s| s.1.to_string().unwrap_or_default()),
            ));
        }
        f.write_fmt(format_args!("{}", table))?;
        Ok(())
    }
}

struct InlineBlock(Bytes);

impl From<InlineBlock> for Bytes {
    fn from(raw: InlineBlock) -> Self {
        raw.0
    }
}

impl crate::prelude::sync::Inlinable for InlineBlock {
    fn read_inlined<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        use crate::prelude::sync::InlinableRead;
        let version = reader.read_u32()?;
        let len = reader.read_u32()?;
        let mut bytes = Vec::with_capacity(len as usize);
        bytes.resize(len as usize, 0);
        unsafe {
            std::ptr::copy_nonoverlapping(
                version.to_le_bytes().as_ptr(),
                bytes.as_mut_ptr(),
                std::mem::size_of::<u32>(),
            );
            std::ptr::copy_nonoverlapping(
                len.to_le_bytes().as_ptr(),
                bytes.as_mut_ptr().offset(4),
                std::mem::size_of::<u32>(),
            );
        }
        let buf = &mut bytes[8..];
        reader.read_exact(buf)?;
        Ok(Self(bytes.into()))
    }

    fn write_inlined<W: std::io::Write>(&self, wtr: &mut W) -> std::io::Result<usize> {
        wtr.write_all(self.0.as_ref())?;
        Ok(self.0.len())
    }
}
#[async_trait::async_trait]
impl crate::prelude::AsyncInlinable for InlineBlock {
    async fn read_inlined<R: tokio::io::AsyncRead + Send + Unpin>(
        reader: &mut R,
    ) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        use tokio::io::*;

        let version = reader.read_u32_le().await?;
        let len = reader.read_u32_le().await?;
        let mut bytes = Vec::with_capacity(len as usize);
        bytes.resize(len as usize, 0);
        unsafe {
            std::ptr::copy_nonoverlapping(
                version.to_le_bytes().as_ptr(),
                bytes.as_mut_ptr(),
                std::mem::size_of::<u32>(),
            );
            std::ptr::copy_nonoverlapping(
                len.to_le_bytes().as_ptr(),
                bytes.as_mut_ptr().offset(4),
                std::mem::size_of::<u32>(),
            );
        }
        let buf = &mut bytes[8..];
        reader.read_exact(buf).await?;
        Ok(Self(bytes.into()))
    }

    async fn write_inlined<W: tokio::io::AsyncWrite + Send + Unpin>(
        &self,
        wtr: &mut W,
    ) -> std::io::Result<usize> {
        use tokio::io::*;
        wtr.write_all(self.0.as_ref()).await?;
        Ok(self.0.len())
    }
}

impl crate::prelude::sync::Inlinable for RawBlock {
    fn read_inlined<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        use crate::prelude::sync::InlinableRead;
        let layout = reader.read_u32()?;
        let layout = Layout::from_bits(layout).expect("should be layout");

        let precision = layout.precision();
        let raw: InlineBlock = reader.read_inlinable()?;
        let mut raw = Self::parse_from_raw_block(raw.0, precision);
        let cols = raw.ncols();

        if layout.expect_table_name() {
            let name = reader.read_inlined_str::<2>()?;
            raw.with_table_name(name);
        }
        if layout.expect_field_names() {
            let names: Vec<_> = (0..cols as usize)
                .map(|_| reader.read_inlined_str::<1>())
                .try_collect()?;
            raw.with_field_names(names);
        }

        Ok(raw)
    }

    fn read_optional_inlined<R: std::io::Read>(reader: &mut R) -> std::io::Result<Option<Self>> {
        use crate::prelude::sync::InlinableRead;
        let layout = reader.read_u32()?;
        if layout == 0xFFFFFFFF {
            return Ok(None);
        }
        let layout = Layout::from_bits(layout).expect("should be layout");

        let precision = layout.precision();
        let raw: InlineBlock = reader.read_inlinable()?;
        let mut raw = Self::parse_from_raw_block(raw.0, precision);

        if layout.expect_table_name() {
            let name = reader.read_inlined_str::<2>()?;
            raw.with_table_name(name);
        }
        if layout.expect_field_names() {
            let names: Vec<_> = (0..raw.ncols() as usize)
                .map(|_| reader.read_inlined_str::<1>())
                .try_collect()?;
            raw.with_field_names(names);
        }
        log::trace!(
            "table name: {}, cols: {}, rows: {}",
            &raw.table_name().unwrap_or("(?)"),
            raw.ncols(),
            raw.nrows()
        );

        Ok(Some(raw))
    }

    fn write_inlined<W: std::io::Write>(&self, wtr: &mut W) -> std::io::Result<usize> {
        use crate::prelude::sync::InlinableWrite;
        let layout = self.layout();
        let mut l = wtr.write_u32_le(layout.as_inner())?;

        let raw = self.as_raw_bytes();
        wtr.write_all(raw)?;
        l += raw.len();

        if layout.expect_table_name() {
            let name = self.table_name().expect("table name should be known");
            l += wtr.write_inlined_bytes::<2>(name.as_bytes())?;
        }
        if layout.expect_field_names() {
            debug_assert_eq!(self.field_names().len(), self.ncols());
            for field in self.field_names() {
                l += wtr.write_inlined_str::<1>(field)?;
            }
        }
        Ok(l)
    }
}

#[async_trait::async_trait]
impl crate::prelude::AsyncInlinable for RawBlock {
    async fn read_optional_inlined<R: tokio::io::AsyncRead + Send + Unpin>(
        reader: &mut R,
    ) -> std::io::Result<Option<Self>> {
        use crate::util::AsyncInlinableRead;
        use tokio::io::*;
        let layout = reader.read_u32_le().await?;
        if layout == 0xFFFFFFFF {
            return Ok(None);
        }
        let layout = Layout::from_bits(layout).expect("invalid layout");

        let precision = layout.precision();

        let raw: InlineBlock =
            <InlineBlock as crate::prelude::AsyncInlinable>::read_inlined(reader).await?;
        let mut raw = Self::parse_from_raw_block(raw.0, precision);

        if layout.expect_table_name() {
            let name = reader.read_inlined_str::<2>().await?;
            raw.with_table_name(name);
        }
        if layout.expect_field_names() {
            let mut names = Vec::with_capacity(raw.ncols());
            for _ in 0..raw.ncols() {
                names.push(reader.read_inlined_str::<1>().await?);
            }
            raw.with_field_names(names);
        }

        Ok(Some(raw))
    }

    async fn read_inlined<R: tokio::io::AsyncRead + Send + Unpin>(
        reader: &mut R,
    ) -> std::io::Result<Self> {
        <Self as crate::prelude::AsyncInlinable>::read_optional_inlined(reader)
            .await?
            .ok_or(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid raw data format",
            ))
    }

    async fn write_inlined<W: tokio::io::AsyncWrite + Send + Unpin>(
        &self,
        wtr: &mut W,
    ) -> std::io::Result<usize> {
        use crate::util::AsyncInlinableWrite;
        use tokio::io::*;

        let layout = self.layout();
        wtr.write_u32_le(layout.as_inner()).await?;

        let raw = self.as_raw_bytes();
        wtr.write_all(raw).await?;

        let mut l = std::mem::size_of::<u32>() + raw.len();

        if layout.expect_table_name() {
            let name = self.table_name().expect("table name should be known");
            l += wtr.write_inlined_bytes::<2>(name.as_bytes()).await?;
        }
        if layout.expect_field_names() {
            debug_assert_eq!(self.field_names().len(), self.ncols());
            for field in self.field_names() {
                l += wtr.write_inlined_str::<1>(field).await?;
            }
        }

        Ok(l)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_raw_from_v2() {
    use crate::prelude::AsyncInlinable;
    use std::ops::Deref;
    // pretty_env_logger::formatted_builder()
    //     .filter_level(log::LevelFilter::Trace)
    //     .init();
    let bytes = b"\x10\x86\x1aA \xcc)AB\xc2\x14AZ],A\xa2\x8d$A\x87\xb9%A\xf5~\x0fA\x96\xf7,AY\xee\x17A1|\x15As\x00\x00\x00q\x00\x00\x00s\x00\x00\x00t\x00\x00\x00u\x00\x00\x00t\x00\x00\x00n\x00\x00\x00n\x00\x00\x00n\x00\x00\x00r\x00\x00\x00";

    let block = RawBlock::parse_from_raw_block_v2(
        bytes.as_slice(),
        &[Field::new("a", Ty::Float, 4), Field::new("b", Ty::Int, 4)],
        &[4, 4],
        10,
        Precision::Millisecond,
    );
    assert!(block.lengths.deref() == &[40, 40]);

    let bytes = include_bytes!("../../../tests/test.txt");

    let block = RawBlock::parse_from_raw_block_v2(
        bytes.as_slice(),
        &[
            Field::new("ts", Ty::Timestamp, 8),
            Field::new("current", Ty::Float, 4),
            Field::new("voltage", Ty::Int, 4),
            Field::new("phase", Ty::Float, 4),
            Field::new("group_id", Ty::Int, 4),
            Field::new("location", Ty::VarChar, 16),
        ],
        &[8, 4, 4, 4, 4, 18],
        10,
        Precision::Millisecond,
    );

    #[derive(Debug, serde::Deserialize)]
    #[allow(dead_code)]
    struct Record {
        ts: String,
        current: f32,
        voltage: i32,
        phase: f32,
        group_id: i32,
        location: String,
    }
    let rows: Vec<Record> = block.deserialize().try_collect().unwrap();
    dbg!(rows);
    // dbg!(block);
    let bytes = views_to_raw_block(&block.columns);
    let raw2 = RawBlock::parse_from_raw_block(bytes, block.precision);
    dbg!(&raw2);
    let inlined = raw2.inlined().await;
    dbg!(&inlined);
    let mut raw3 = RawBlock::read_inlined(&mut inlined.as_slice())
        .await
        .unwrap();
    raw3.with_field_names(["ts", "current", "voltage", "phase", "group_id", "location"])
        .with_table_name("meters");
    dbg!(&raw3);

    let raw4 = RawBlock::read_inlined(&mut raw3.inlined().await.as_slice())
        .await
        .unwrap();
    dbg!(&raw4);
    assert_eq!(raw3.table_name(), raw4.table_name());

    let raw5 = RawBlock::read_optional_inlined(&mut raw3.inlined().await.as_slice())
        .await
        .unwrap();
    dbg!(raw5);
}

#[test]
fn test_v2_full() {
    let bytes = include_bytes!("../../../tests/v2.block.gz");

    use flate2::read::GzDecoder;
    use std::io::prelude::*;
    let mut buf = Vec::new();
    let len = GzDecoder::new(&bytes[..]).read_to_end(&mut buf).unwrap();
    assert_eq!(len, 66716);
    let block = RawBlock::parse_from_raw_block_v2(
        buf,
        &[
            Field::new("ts", Ty::Timestamp, 8),
            Field::new("b1", Ty::Bool, 1),
            Field::new("c8i1", Ty::TinyInt, 1),
            Field::new("c16i1", Ty::SmallInt, 2),
            Field::new("c32i1", Ty::Int, 4),
            Field::new("c64i1", Ty::BigInt, 8),
            Field::new("c8u1", Ty::UTinyInt, 1),
            Field::new("c16u1", Ty::USmallInt, 2),
            Field::new("c32u1", Ty::UInt, 4),
            Field::new("c64u1", Ty::UBigInt, 8),
            Field::new("cb1", Ty::VarChar, 100),
            Field::new("cn1", Ty::NChar, 10),
            Field::new("b2", Ty::Bool, 1),
            Field::new("c8i2", Ty::TinyInt, 1),
            Field::new("c16i2", Ty::SmallInt, 2),
            Field::new("c32i2", Ty::Int, 4),
            Field::new("c64i2", Ty::BigInt, 8),
            Field::new("c8u2", Ty::UTinyInt, 1),
            Field::new("c16u2", Ty::USmallInt, 2),
            Field::new("c32u2", Ty::UInt, 4),
            Field::new("c64u2", Ty::UBigInt, 8),
            Field::new("cb2", Ty::VarChar, 100),
            Field::new("cn2", Ty::NChar, 10),
            Field::new("jt", Ty::Json, 4096),
        ],
        &[
            8, 1, 1, 2, 4, 8, 1, 2, 4, 8, 102, 42, 1, 1, 2, 4, 8, 1, 2, 4, 8, 12, 66, 16387,
        ],
        4,
        Precision::Millisecond,
    );
    let bytes = views_to_raw_block(&block.columns);
    let raw2 = RawBlock::parse_from_raw_block(bytes, block.precision);
    dbg!(raw2);
}

#[test]
fn test_v2_null() {
    let raw = RawBlock::parse_from_raw_block_v2(
        [0x2, 0].as_slice(),
        &[Field::new("b", Ty::Bool, 1)],
        &[1],
        2,
        Precision::Millisecond,
    );
    dbg!(&raw);
    let bytes = views_to_raw_block(&raw.columns);
    let raw2 = RawBlock::parse_from_raw_block(bytes, raw.precision);
    dbg!(raw2);
    let (_ty, _len, null) = unsafe { raw.get_raw_value_unchecked(0, 0) };
    assert!(null.is_null());
    let (_ty, _len, null) = unsafe { raw.get_raw_value_unchecked(1, 0) };
    assert!(!null.is_null());

    let raw = RawBlock::parse_from_raw_block_v2(
        [0x80].as_slice(),
        &[Field::new("b", Ty::TinyInt, 1)],
        &[1],
        1,
        Precision::Millisecond,
    );
    dbg!(&raw);
    let (_ty, _len, null) = unsafe { raw.get_raw_value_unchecked(0, 0) };
    assert!(null.is_null());

    let raw = RawBlock::parse_from_raw_block_v2(
        [0, 0, 0, 0x80].as_slice(),
        &[Field::new("a", Ty::Int, 4)],
        &[4],
        1,
        Precision::Millisecond,
    );
    dbg!(&raw);
    let (_ty, _len, null) = unsafe { raw.get_raw_value_unchecked(0, 0) };
    assert!(null.is_null());

    let raw = RawBlock::parse_from_raw_block_v2(
        [0, 0, 0xf0, 0x7f].as_slice(),
        &[Field::new("a", Ty::Float, 4)],
        &[4],
        1,
        Precision::Millisecond,
    );
    let (_ty, _len, null) = unsafe { raw.get_raw_value_unchecked(0, 0) };
    dbg!(raw);
    assert!(null.is_null());
}
#[test]
fn test_from_v2() {
    let raw = RawBlock::parse_from_raw_block_v2(
        [1].as_slice(),
        &[Field::new("a", Ty::TinyInt, 1)],
        &[1],
        1,
        Precision::Millisecond,
    );
    let bytes = raw.as_raw_bytes();
    let bytes = Bytes::copy_from_slice(bytes);
    let raw2 = RawBlock::parse_from_raw_block(bytes, raw.precision());
    dbg!(&raw, raw2);
    // dbg!(raw.as_bytes());
    // let v = unsafe { raw.get_ref_unchecked(0, 0) };
    // dbg!(v);

    let raw = RawBlock::parse_from_raw_block_v2(
        [1, 0, 0, 0].as_slice(),
        &[Field::new("a", Ty::Int, 4)],
        &[4],
        1,
        Precision::Millisecond,
    );
    dbg!(&raw);
    // dbg!(raw.as_bytes());
    // let v = unsafe { raw.get_ref_unchecked(0, 0) };
    // dbg!(v);

    let raw = RawBlock::parse_from_raw_block_v2(
        [2, 0, b'a', b'b'].as_slice(),
        &[Field::new("b", Ty::VarChar, 2)],
        &[4],
        1,
        Precision::Millisecond,
    );
    dbg!(&raw);
    // dbg!(raw.as_bytes());
    // let v = unsafe { raw.get_ref_unchecked(0, 0) };
    // dbg!(v);

    let raw = RawBlock::parse_from_raw_block_v2(
        [2, 0, b'a', b'b'].as_slice(),
        &[Field::new("b", Ty::VarChar, 2)],
        &[4],
        1,
        Precision::Millisecond,
    );
    dbg!(&raw);
    let raw = RawBlock::parse_from_raw_block_v2(
        &[1, 1, 1][..],
        &[
            Field::new("a", Ty::TinyInt, 1),
            Field::new("b", Ty::SmallInt, 2),
        ],
        &[1, 2],
        1,
        Precision::Millisecond,
    );
    dbg!(&raw);

    println!("{}", raw.pretty_format());
}
