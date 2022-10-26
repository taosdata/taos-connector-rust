pub use taos_query::prelude::*;

#[cfg(any(feature = "ws", feature = "native"))]
pub mod sync {
    pub use taos_query::prelude::sync::*;

    pub use super::Stmt;
    pub use super::{Consumer, MessageSet, TmqBuilder};
    pub use super::{Taos, TaosBuilder};
}

#[cfg(all(feature = "ws", feature = "native"))]
mod stmt;
#[cfg(all(feature = "ws", feature = "native"))]
pub use stmt::Stmt;

#[cfg(all(feature = "ws", feature = "native"))]
mod tmq;
#[cfg(all(feature = "ws", feature = "native"))]
pub use tmq::{Consumer, Data, MessageSet, Meta, TmqBuilder};

#[cfg(all(feature = "ws", feature = "native"))]
mod query;
#[cfg(all(feature = "ws", feature = "native"))]
pub use query::*;

#[cfg(all(feature = "ws", not(feature = "native")))]
pub use taos_ws::*;

#[cfg(all(feature = "native", not(feature = "ws")))]
pub use taos_sys::*;

#[cfg(all(not(feature = "ws"), not(feature = "native")))]
compile_error!("Either feature \"ws\" or \"native\" or both must be enabled for this crate.");
