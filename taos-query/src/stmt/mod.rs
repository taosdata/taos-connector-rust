// Copyright (c) 2022 TAOS Data, Inc.
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

pub trait Bindable<Q>
where
    Q: Queryable,
    Self: Sized,
{
    fn init(taos: &Q) -> RawResult<Self>;

    fn init_with_req_id(taos: &Q, req_id: u64) -> RawResult<Self>;

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

#[async_trait::async_trait]
pub trait AsyncBindable<Q>
where
    Q: AsyncQueryable,
    Self: Sized,
{
    async fn init(taos: &Q) -> RawResult<Self>;

    async fn init_with_req_id(taos: &Q, req_id: u64) -> RawResult<Self>;

    async fn prepare(&mut self, sql: &str) -> RawResult<&mut Self>;

    async fn set_tbname(&mut self, name: &str) -> RawResult<&mut Self>;

    async fn set_tags(&mut self, tags: &[Value]) -> RawResult<&mut Self>;

    async fn set_tbname_tags(&mut self, name: &str, tags: &[Value]) -> RawResult<&mut Self> {
        self.set_tbname(name).await?.set_tags(tags).await
    }

    async fn bind(&mut self, params: &[ColumnView]) -> RawResult<&mut Self>;

    async fn add_batch(&mut self) -> RawResult<&mut Self>;

    async fn execute(&mut self) -> RawResult<usize>;

    async fn affected_rows(&self) -> usize;

    async fn result_set(&mut self) -> RawResult<Q::AsyncResultSet> {
        todo!()
    }
}
