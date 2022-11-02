pub use taos_query::prelude::*;

#[cfg(any(feature = "ws", feature = "native", feature = "optin"))]
pub mod sync {
    pub use taos_query::prelude::sync::*;

    pub use super::Stmt;
    pub use super::{Consumer, MessageSet, TmqBuilder};
    pub use super::{Taos, TaosBuilder};
}

#[cfg(all(feature = "ws", any(feature = "native", feature = "optin")))]
mod stmt;
#[cfg(all(feature = "ws", any(feature = "native", feature = "optin")))]
pub use stmt::Stmt;

#[cfg(all(feature = "ws", any(feature = "native", feature = "optin")))]
mod tmq;
#[cfg(all(feature = "ws", any(feature = "native", feature = "optin")))]
pub use tmq::{Consumer, Data, MessageSet, Meta, TmqBuilder};

#[cfg(all(feature = "ws", any(feature = "native", feature = "optin")))]
mod query;
#[cfg(all(feature = "ws", any(feature = "native", feature = "optin")))]
pub use query::*;

#[cfg(all(feature = "ws", not(any(feature = "native", feature = "optin"))))]
pub use taos_ws::*;

#[cfg(all(any(feature = "native", feature = "optin"), not(feature = "ws")))]
pub use taos_sys::*;

#[cfg(all(not(feature = "ws"), not(feature = "native"), not(feature = "optin")))]
compile_error!("Either feature \"ws\" or \"native\"|"optin" or both must be enabled for this crate.");

#[cfg(all(feature = "optin", feature = "native"))]
compile_error!(
    "Feature \"optin\" is conflicted with \"native\", choose only one feature for native"
);

#[cfg(feature = "optin")]
pub use taos_optin as taos_sys;
