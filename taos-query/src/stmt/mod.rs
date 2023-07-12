use crate::{
    common::{views::ColumnView, Value},
    Queryable, RawResult,
};

mod column;
pub use column::*;

pub trait Bindable<Q>
where
    Q: Queryable,
    Self: Sized,
{
    fn init(taos: &Q) -> RawResult<Self>;

    fn prepare<S: AsRef<str>>(&mut self, sql: S) -> RawResult<&mut Self>;

    fn set_tbname<S: AsRef<str>>(&mut self, name: S) -> RawResult<&mut Self>;

    fn set_tags(&mut self, tags: &[Value]) -> RawResult<&mut Self>;

    fn set_tbname_tags<S: AsRef<str>>(&mut self, name: S, tags: &[Value]) -> RawResult<&mut Self> {
        self.set_tbname(name)?.set_tags(tags)
    }

    fn bind(&mut self, params: &[ColumnView]) -> RawResult<&mut Self>;

    fn add_batch(&mut self) -> RawResult<&mut Self>;

    fn execute(&mut self) -> RawResult<usize>;

    fn affected_rows(&self) -> usize;

    fn result_set(&mut self) -> RawResult<Q::ResultSet> {
        todo!()
    }
}
