pub use taos_query;
pub use taos_query::prelude::*;

pub use taos_query::prelude::RawError as Error;

pub type TaosPool = taos_query::prelude::Pool<TaosBuilder>;

#[cfg(any(feature = "ws", feature = "optin"))]
pub mod sync {
    pub use taos_query::prelude::sync::*;

    pub use super::Stmt;
    pub use super::{Consumer, MessageSet, Offset, TmqBuilder};
    pub use super::{Taos, TaosBuilder};
}

#[cfg(all(feature = "ws", feature = "optin"))]
mod stmt;
#[cfg(all(feature = "ws", feature = "optin"))]
pub use stmt::Stmt;

#[cfg(all(feature = "ws", feature = "optin"))]
mod tmq;
#[cfg(all(feature = "ws", feature = "optin"))]
pub use tmq::{Consumer, Data, MessageSet, Meta, Offset, TmqBuilder};

#[cfg(all(feature = "ws", feature = "optin"))]
mod query;
#[cfg(all(feature = "ws", feature = "optin"))]
pub use query::*;

#[cfg(all(feature = "ws", not(feature = "optin")))]
pub use taos_ws::*;

#[cfg(all(any(feature = "optin"), not(feature = "ws")))]
pub use crate::sys::*;
#[cfg(all(any(feature = "optin"), not(feature = "ws")))]
pub use sys::tmq::Offset;

#[cfg(all(not(feature = "ws"), not(feature = "optin")))]
compile_error!("Either feature \"ws\" or \"native\"|"optin" or both must be enabled for this crate.");

// #[cfg(all(feature = "optin", feature = "native"))]
// compile_error!(
//     "Feature \"optin\" is conflicted with \"native\", choose only one feature for native"
// );

#[cfg(feature = "optin")]
pub(crate) use taos_optin as sys;

#[cfg(not(feature = "optin"))]
#[cfg(feature = "native")]
pub(crate) use taos_sys as sys;

shadow_rs::shadow!(build);
