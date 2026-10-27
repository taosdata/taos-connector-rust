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

use super::*;
use crate::common::itypes::{INChar, IVarChar};

impl From<Vec<bool>> for ColumnView {
    fn from(values: Vec<bool>) -> Self {
        ColumnView::Bool(BoolView::from_iter(values))
    }
}

impl From<Vec<Option<bool>>> for ColumnView {
    fn from(values: Vec<Option<bool>>) -> Self {
        ColumnView::Bool(BoolView::from_iter(values))
    }
}

impl From<Vec<i8>> for ColumnView {
    fn from(values: Vec<i8>) -> Self {
        ColumnView::TinyInt(TinyIntView::from_iter(values))
    }
}

impl From<Vec<Option<i8>>> for ColumnView {
    fn from(values: Vec<Option<i8>>) -> Self {
        ColumnView::TinyInt(TinyIntView::from_iter(values))
    }
}

impl From<Vec<u8>> for ColumnView {
    fn from(values: Vec<u8>) -> Self {
        ColumnView::UTinyInt(UTinyIntView::from_iter(values))
    }
}

impl From<Vec<Option<u8>>> for ColumnView {
    fn from(values: Vec<Option<u8>>) -> Self {
        ColumnView::UTinyInt(UTinyIntView::from_iter(values))
    }
}

impl From<Vec<i16>> for ColumnView {
    fn from(values: Vec<i16>) -> Self {
        ColumnView::SmallInt(SmallIntView::from_iter(values))
    }
}

impl From<Vec<Option<i16>>> for ColumnView {
    fn from(values: Vec<Option<i16>>) -> Self {
        ColumnView::SmallInt(SmallIntView::from_iter(values))
    }
}

impl From<Vec<u16>> for ColumnView {
    fn from(values: Vec<u16>) -> Self {
        ColumnView::USmallInt(USmallIntView::from_iter(values))
    }
}

impl From<Vec<Option<u16>>> for ColumnView {
    fn from(values: Vec<Option<u16>>) -> Self {
        ColumnView::USmallInt(USmallIntView::from_iter(values))
    }
}

impl From<Vec<i32>> for ColumnView {
    fn from(values: Vec<i32>) -> Self {
        ColumnView::Int(IntView::from_iter(values))
    }
}

impl From<Vec<Option<i32>>> for ColumnView {
    fn from(values: Vec<Option<i32>>) -> Self {
        ColumnView::Int(IntView::from_iter(values))
    }
}

impl From<Vec<u32>> for ColumnView {
    fn from(values: Vec<u32>) -> Self {
        ColumnView::UInt(UIntView::from_iter(values))
    }
}

impl From<Vec<Option<u32>>> for ColumnView {
    fn from(values: Vec<Option<u32>>) -> Self {
        ColumnView::UInt(UIntView::from_iter(values))
    }
}

impl From<Vec<i64>> for ColumnView {
    fn from(values: Vec<i64>) -> Self {
        ColumnView::BigInt(BigIntView::from_iter(values))
    }
}

impl From<Vec<Option<i64>>> for ColumnView {
    fn from(values: Vec<Option<i64>>) -> Self {
        ColumnView::BigInt(BigIntView::from_iter(values))
    }
}

impl From<Vec<u64>> for ColumnView {
    fn from(values: Vec<u64>) -> Self {
        ColumnView::UBigInt(UBigIntView::from_iter(values))
    }
}

impl From<Vec<Option<u64>>> for ColumnView {
    fn from(values: Vec<Option<u64>>) -> Self {
        ColumnView::UBigInt(UBigIntView::from_iter(values))
    }
}

impl From<Vec<f32>> for ColumnView {
    fn from(values: Vec<f32>) -> Self {
        ColumnView::Float(FloatView::from_iter(values))
    }
}

impl From<Vec<Option<f32>>> for ColumnView {
    fn from(values: Vec<Option<f32>>) -> Self {
        ColumnView::Float(FloatView::from_iter(values))
    }
}

impl From<Vec<f64>> for ColumnView {
    fn from(values: Vec<f64>) -> Self {
        ColumnView::Double(DoubleView::from_iter(values))
    }
}

impl From<Vec<Option<f64>>> for ColumnView {
    fn from(values: Vec<Option<f64>>) -> Self {
        ColumnView::Double(DoubleView::from_iter(values))
    }
}

impl From<Vec<Option<IVarChar>>> for ColumnView {
    fn from(values: Vec<Option<IVarChar>>) -> Self {
        ColumnView::VarChar(VarCharView::from_iter::<IVarChar, _, _, _>(values))
    }
}

impl From<Vec<Option<INChar>>> for ColumnView {
    fn from(values: Vec<Option<INChar>>) -> Self {
        ColumnView::NChar(NCharView::from_iter::<INChar, _, _, _>(values))
    }
}
