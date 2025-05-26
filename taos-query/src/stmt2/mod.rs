use crate::{AsyncQueryable, ColumnView, Queryable, RawResult, Value};

pub trait Stmt2Bindable<Q>
where
    Q: Queryable,
    Self: Sized,
{
    fn init(taos: &Q) -> RawResult<Self>;

    fn prepare(&mut self, sql: &str) -> RawResult<&mut Self>;

    fn bind(&mut self, params: &[Stmt2BindParam]) -> RawResult<&mut Self>;

    /// Batch bind parameters.
    ///
    /// # Note
    /// Developers must ensure that the lengths of `table_names`, `tags_list`, and `columns_list`
    /// in `param` are consistent. Otherwise, it may lead to undefined behavior or runtime errors.
    fn bind_batch(&mut self, param: Stmt2BatchBindParam) -> RawResult<&mut Self> {
        let params = param
            .table_names
            .into_iter()
            .zip(param.tags_list.into_iter())
            .zip(param.columns_list.into_iter())
            .map(|((table_name, tags), columns)| Stmt2BindParam::new(table_name, tags, columns))
            .collect::<Vec<_>>();

        self.bind(&params)
    }

    fn exec(&mut self) -> RawResult<usize>;

    fn affected_rows(&self) -> usize;

    fn result_set(&self) -> RawResult<Q::ResultSet>;
}

#[async_trait::async_trait]
pub trait Stmt2AsyncBindable<Q>
where
    Q: AsyncQueryable,
    Self: Sized,
{
    async fn init(taos: &Q) -> RawResult<Self>;

    async fn prepare(&mut self, sql: &str) -> RawResult<&mut Self>;

    async fn bind(&mut self, params: &[Stmt2BindParam]) -> RawResult<&mut Self>;

    /// Batch bind parameters.
    ///
    /// # Note
    /// Developers must ensure that the lengths of `table_names`, `tags_list`, and `columns_list`
    /// in `param` are consistent. Otherwise, it may lead to undefined behavior or runtime errors.
    async fn bind_batch(&mut self, param: Stmt2BatchBindParam) -> RawResult<&mut Self> {
        let params = param
            .table_names
            .into_iter()
            .zip(param.tags_list.into_iter())
            .zip(param.columns_list.into_iter())
            .map(|((table_name, tags), columns)| Stmt2BindParam::new(table_name, tags, columns))
            .collect::<Vec<_>>();

        self.bind(&params).await
    }

    async fn exec(&mut self) -> RawResult<usize>;

    async fn affected_rows(&self) -> usize;

    async fn result_set(&self) -> RawResult<Q::AsyncResultSet>;
}

#[derive(Clone, Debug)]
pub struct Stmt2BindParam {
    table_name: Option<String>,
    tags: Option<Vec<Value>>,
    columns: Option<Vec<ColumnView>>,
}

impl Stmt2BindParam {
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

    pub fn with_table_name(&mut self, table_name: String) {
        self.table_name = Some(table_name);
    }

    pub fn table_name(&self) -> Option<&String> {
        self.table_name.as_ref()
    }

    pub fn with_tags(&mut self, tags: Vec<Value>) {
        self.tags = Some(tags);
    }

    pub fn tags(&self) -> Option<&Vec<Value>> {
        self.tags.as_ref()
    }

    pub fn with_columns(&mut self, columns: Vec<ColumnView>) {
        self.columns = Some(columns);
    }

    pub fn columns(&self) -> Option<&Vec<ColumnView>> {
        self.columns.as_ref()
    }
}

#[derive(Clone, Debug)]
pub struct Stmt2BatchBindParam {
    table_names: Vec<Option<String>>,
    tags_list: Vec<Option<Vec<Value>>>,
    columns_list: Vec<Option<Vec<ColumnView>>>,
}

impl Stmt2BatchBindParam {
    pub fn new(
        table_names: Vec<Option<String>>,
        tags_list: Vec<Option<Vec<Value>>>,
        columns_list: Vec<Option<Vec<ColumnView>>>,
    ) -> Self {
        Self {
            table_names,
            tags_list,
            columns_list,
        }
    }

    pub fn with_table_names(&mut self, table_names: Vec<Option<String>>) {
        self.table_names = table_names;
    }

    pub fn table_names(&self) -> &Vec<Option<String>> {
        &self.table_names
    }

    pub fn with_tags_list(&mut self, tags_list: Vec<Option<Vec<Value>>>) {
        self.tags_list = tags_list;
    }

    pub fn tags_list(&self) -> &Vec<Option<Vec<Value>>> {
        &self.tags_list
    }

    pub fn with_columns_list(&mut self, columns_list: Vec<Option<Vec<ColumnView>>>) {
        self.columns_list = columns_list;
    }

    pub fn columns_list(&self) -> &Vec<Option<Vec<ColumnView>>> {
        &self.columns_list
    }
}
