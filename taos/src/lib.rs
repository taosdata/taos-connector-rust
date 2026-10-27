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

pub use taos_query;
pub use taos_query::prelude::{RawError as Error, *};

#[cfg(any(feature = "deadpool", feature = "r2d2"))]
pub type TaosPool = taos_query::prelude::Pool<TaosBuilder>;

#[cfg(any(feature = "ws", feature = "optin"))]
pub mod sync {
    pub use taos_query::prelude::sync::*;

    #[cfg(feature = "ws")]
    pub use super::Stmt2;
    pub use super::{Consumer, MessageSet, Offset, Stmt, Taos, TaosBuilder, TmqBuilder};
}

#[cfg(all(feature = "ws", feature = "optin"))]
mod stmt;
#[cfg(all(feature = "ws", feature = "optin"))]
pub use stmt::Stmt;

#[cfg(all(feature = "ws", feature = "optin"))]
mod stmt2;
#[cfg(all(feature = "ws", feature = "optin"))]
pub use stmt2::Stmt2;

#[cfg(all(feature = "ws", feature = "optin"))]
mod tmq;
#[cfg(all(feature = "ws", feature = "optin"))]
pub use tmq::{Consumer, Data, MessageSet, Meta, Offset, TmqBuilder};

#[cfg(all(feature = "ws", feature = "optin"))]
mod query;
#[cfg(all(feature = "ws", feature = "optin"))]
pub use query::*;
#[cfg(all(feature = "optin", not(feature = "ws")))]
pub use sys::tmq::Offset;
#[cfg(all(feature = "ws", not(feature = "optin")))]
pub use taos_ws::*;

#[cfg(all(feature = "optin", not(feature = "ws")))]
pub use crate::sys::*;

#[cfg(all(not(feature = "ws"), not(feature = "optin")))]
compile_error!("Either feature \"ws\" or \"native\"|"optin" or both must be enabled for this crate.");

#[cfg(feature = "optin")]
pub(crate) use taos_optin as sys;

shadow_rs::shadow!(build);
