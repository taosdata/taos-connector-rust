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

#[derive(Clone, Debug)]
pub struct Stmt2BindData {
    table_name: Option<String>,
    tags: Option<Vec<Value>>,
    columns: Option<Vec<ColumnView>>,
}

impl Stmt2BindData {
    pub fn new(
        table_name: Option<String>,
        tags: Option<Vec<Value>>,
        columns: Option<Vec<ColumnView>>,
    ) -> Self {
        Self {
            table_name,
            tags,
            columns,
        }
    }

    pub fn table_name(&self) -> Option<&String> {
        self.table_name.as_ref()
    }

    pub fn tags(&self) -> Option<&Vec<Value>> {
        self.tags.as_ref()
    }

    pub fn columns(&self) -> Option<&Vec<ColumnView>> {
        self.columns.as_ref()
    }
}
