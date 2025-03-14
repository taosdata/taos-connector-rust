use bigdecimal::{BigDecimal, ToPrimitive};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decimal<T> {
    pub(crate) data: T,
    pub(crate) precision: u8,
    pub(crate) scale: u8,
}

impl<T: Into<bigdecimal::num_bigint::BigInt> + Copy> Decimal<T> {
    pub(crate) fn as_bigdecimal(&self) -> bigdecimal::BigDecimal {
        BigDecimal::from_bigint(self.data.into(), self.scale as _)
    }
}

macro_rules! from_bigdecimal {
    ($ty: ty) => {
        impl Decimal<$ty> {
            pub(crate) fn from_bigdecimal(decimal: &BigDecimal) -> Option<Self> {
                let (num, scale) = decimal.as_bigint_and_exponent();
                paste::paste! {
                    num.[<to_$ty>]().map(|data| Self {
                        data,
                        precision: decimal.digits() as _,
                        scale: scale as _,
                    })
                }
            }
        }
    };
}

from_bigdecimal!(i128);
from_bigdecimal!(i64);

impl<T: Into<bigdecimal::num_bigint::BigInt> + Copy> std::fmt::Display for Decimal<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_bigdecimal().fmt(f)
    }
}
