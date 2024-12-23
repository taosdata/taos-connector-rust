use taos_query::common::itypes::IsValue;
use taos_query::common::Ty;

use crate::types::{BindFrom, TaosBindV2, TaosBindV3};
impl<T> From<&T> for TaosBindV2
where
    T: IsValue + Clone,
{
    #[inline(always)]
    fn from(v: &T) -> Self {
        macro_rules! as_is {
            () => {
                Self::from_primitive(v)
            };
        }

        match T::TY {
            Ty::Bool
            | Ty::TinyInt
            | Ty::SmallInt
            | Ty::Int
            | Ty::BigInt
            | Ty::UTinyInt
            | Ty::USmallInt
            | Ty::UInt
            | Ty::UBigInt
            | Ty::Float
            | Ty::Double => as_is!(),
            Ty::Timestamp => Self::from_timestamp(v.as_timestamp()),
            Ty::VarChar => Self::from_varchar(v.as_var_char()),
            Ty::NChar => Self::from_nchar(v.as_nchar()),
            Ty::Json => todo!(),
            _ => Self::null(),
        }
    }
}

impl<T> From<&T> for TaosBindV3
where
    T: IsValue + Clone,
{
    #[inline(always)]
    fn from(v: &T) -> Self {
        macro_rules! as_is {
            () => {
                Self::from_primitive(v)
            };
        }

        match T::TY {
            Ty::Bool
            | Ty::TinyInt
            | Ty::SmallInt
            | Ty::Int
            | Ty::BigInt
            | Ty::UTinyInt
            | Ty::USmallInt
            | Ty::UInt
            | Ty::UBigInt
            | Ty::Float
            | Ty::Double => as_is!(),
            Ty::Timestamp => Self::from_timestamp(v.as_timestamp()),
            Ty::VarChar => Self::from_varchar(v.as_var_char()),
            Ty::NChar => Self::from_nchar(v.as_nchar()),
            Ty::Json => todo!(),
            _ => Self::null(),
        }
    }
}

// pub trait ToBind: IsValue + Clone {
//     fn to_bind(&self) -> TaosBind {
//         macro_rules! as_is {
//             () => {
//                 TaosBind::from_primitive(self)
//             };
//         }

//         if self.is_null() {
//             return TaosBind::null();
//         }

//         match Self::TY {
//             Ty::Null => TaosBind::null(),
//             Ty::Bool => as_is!(),
//             Ty::TinyInt => as_is!(),
//             Ty::SmallInt => as_is!(),
//             Ty::Int => as_is!(),
//             Ty::BigInt => as_is!(),
//             Ty::UTinyInt => as_is!(),
//             Ty::USmallInt => as_is!(),
//             Ty::UInt => as_is!(),
//             Ty::UBigInt => as_is!(),
//             Ty::Float => as_is!(),
//             Ty::Double => as_is!(),
//             Ty::Timestamp => TaosBind::from_timestamp(self.as_timestamp()),
//             Ty::VarChar => TaosBind::from_varchar(self.as_var_char()),
//             Ty::NChar => TaosBind::from_nchar(self.as_nchar()),
//             Ty::Json => todo!(),
//             _ => TaosBind::null(),
//         }
//     }
// }

// impl<T: IsValue> ToBind for T {}

#[cfg(test)]
mod tests_v2 {

    use taos_query::common::itypes::IVarChar;

    use crate::types::TaosBindV2 as TaosBind;

    #[test]
    fn bind_bool() {
        for v in [true, false] {
            let bind = TaosBind::from(&v);
            dbg!(&bind);
            let v1 = unsafe { *(bind.buffer as *const bool) };
            assert_eq!(v1, v);
        }
    }

    #[test]
    fn bind_i8() {
        for v in [0i8, 1i8].iter() {
            let bind = TaosBind::from(v);
            dbg!(&bind);
            let v1 = unsafe { (bind.buffer as *const i8).read() };
            assert_eq!(*v, v1);
        }
    }
    #[test]
    fn bind_var_char() {
        {
            let v = &IVarChar::from("abc");
            let bind = TaosBind::from(v);
            dbg!(&bind);

            let v1 =
                unsafe { std::str::from_utf8(std::slice::from_raw_parts(bind.buffer() as _, 3)) }
                    .unwrap();

            dbg!(v1);
            assert!(v1 == v.as_str());
        }
    }
}
#[cfg(test)]
mod tests_v3 {

    use taos_query::common::itypes::IVarChar;

    use crate::types::TaosBindV3 as TaosBind;

    #[test]
    fn bind_bool() {
        for v in [true, false].iter() {
            let bind = TaosBind::from(v);
            dbg!(&bind);
            let v1 = unsafe { (bind.buffer() as *const bool).read() };
            assert_eq!(v1, *v);
        }
    }

    #[test]
    fn bind_i8() {
        for v in [0i8, 1i8].iter() {
            let bind = TaosBind::from(v);
            dbg!(&bind);
            let v1 = unsafe { (bind.buffer() as *const i8).read() };
            assert_eq!(v1, *v);
        }
    }
    // todo: skip this test.
    // #[test]
    fn _bind_var_char() {
        {
            let v = &IVarChar::from("abc");
            let bind = TaosBind::from(v);
            dbg!(&bind);
            let v1 =
                unsafe { std::str::from_utf8(std::slice::from_raw_parts(bind.buffer() as _, 3)) }
                    .unwrap();

            dbg!(v1);
            assert!(v1 == v.as_str());
        }
    }
}
