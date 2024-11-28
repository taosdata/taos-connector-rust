use crate::{AsyncQueryable, ColumnView, Queryable, RawResult, Value};

pub trait Bindable<Q>
where
    Q: Queryable,
    Self: Sized,
{
    fn init(taos: &Q) -> RawResult<Self>;

    fn prepare(&mut self, sql: &str) -> RawResult<&mut Self>;

    fn bind(&mut self, datas: &[Stmt2BindData]) -> RawResult<&mut Self>;

    fn exec(&mut self) -> RawResult<usize>;

    fn affected_rows(&self) -> usize;

    fn result(&self) -> RawResult<Q::ResultSet>;
}

#[async_trait::async_trait]
pub trait AsyncBindable<Q>
where
    Q: AsyncQueryable,
    Self: Sized,
{
    async fn init(taos: &Q) -> RawResult<Self>;

    async fn prepare(&mut self, sql: &str) -> RawResult<&mut Self>;

    async fn bind(&mut self, datas: &[Stmt2BindData]) -> RawResult<&mut Self>;

    async fn exec(&mut self) -> RawResult<usize>;

    async fn affected_rows(&self) -> usize;

    async fn result(&self) -> RawResult<Q::AsyncResultSet>;
}

pub struct Stmt2BindData<'a> {
    table_name: Option<&'a str>,
    tags: Option<&'a [Value]>,
    columns: Option<&'a [ColumnView]>,
}

impl<'a> Stmt2BindData<'a> {
    pub fn new(
        table_name: Option<&'a str>,
        tags: Option<&'a [Value]>,
        columns: Option<&'a [ColumnView]>,
    ) -> Self {
        Self {
            table_name,
            tags,
            columns,
        }
    }

    pub fn table_name(&self) -> Option<&str> {
        self.table_name
    }

    pub fn tags(&self) -> Option<&[Value]> {
        self.tags
    }

    pub fn columns(&self) -> Option<&[ColumnView]> {
        self.columns
    }
}
