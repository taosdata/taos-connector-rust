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

use std::ffi::{c_void, CStr};

use taos_query::common::{Field, Ty};

#[repr(C)]
#[derive(Debug, Copy, Clone)]
struct CFieldV2 {
    pub name: [u8; 65usize],
    pub type_: u8,
    pub bytes: i16,
}

impl CFieldV2 {
    pub fn name(&self) -> &CStr {
        unsafe { CStr::from_ptr(self.name.as_ptr() as _) }
    }

    pub fn type_(&self) -> Ty {
        self.type_.into()
    }

    pub fn bytes(&self) -> u32 {
        self.bytes as _
    }
}

impl From<&CFieldV2> for Field {
    fn from(field: &CFieldV2) -> Field {
        Field::new(
            field
                .name()
                .to_str()
                .expect("invalid utf-8 field name")
                .to_string(),
            field.type_(),
            field.bytes(),
        )
    }
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
struct CFieldV3 {
    pub name: [u8; 65usize],
    pub type_: u8,
    pub bytes: i32,
}

impl CFieldV3 {
    pub fn name(&self) -> &CStr {
        unsafe { CStr::from_ptr(self.name.as_ptr() as _) }
    }

    pub fn type_(&self) -> Ty {
        self.type_.into()
    }

    pub fn bytes(&self) -> u32 {
        self.bytes as _
    }
}

impl From<&CFieldV3> for Field {
    fn from(field: &CFieldV3) -> Field {
        Field::new(
            field
                .name()
                .to_str()
                .expect("invalid utf-8 field name")
                .to_string(),
            field.type_(),
            field.bytes(),
        )
    }
}

pub fn from_raw_fields(version: &str, ptr: *const c_void, len: usize) -> Vec<Field> {
    if version.starts_with('3') {
        (0..len)
            .map(|i| unsafe { (ptr as *const CFieldV3).add(i).as_ref().unwrap() }.into())
            .collect()
    } else {
        (0..len)
            .map(|i| unsafe { (ptr as *const CFieldV2).add(i).as_ref().unwrap() }.into())
            .collect()
    }
}
