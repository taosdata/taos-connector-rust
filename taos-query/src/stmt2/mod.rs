// Copyright (c) 2024 TAOS Data, Inc.
//
// SPDX-License-Identifier: MIT
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use crate::{AsyncQueryable, ColumnView, Queryable, RawResult, Value};

pub trait Stmt2Bindable<Q>
where
    Q: Queryable,
    Self: Sized,
{
    fn init(taos: &Q) -> RawResult<Self>;

    fn prepare(&mut self, sql: &str) -> RawResult<&mut Self>;

    fn bind(&mut self, params: &[Stmt2BindParam]) -> RawResult<&mut Self>;

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
