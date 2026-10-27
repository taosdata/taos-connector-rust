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

use std::os::raw::*;

pub const SET_CONF_RET_SUCC: SET_CONF_RET_CODE = SET_CONF_RET_CODE::Succ;
pub const SET_CONF_RET_ERR_PART: SET_CONF_RET_CODE = SET_CONF_RET_CODE::ErrPart;
pub const SET_CONF_RET_ERR_INNER: SET_CONF_RET_CODE = SET_CONF_RET_CODE::ErrInner;
pub const SET_CONF_RET_ERR_JSON_INVALID: SET_CONF_RET_CODE = SET_CONF_RET_CODE::ErrJsonInvalid;
pub const SET_CONF_RET_ERR_JSON_PARSE: SET_CONF_RET_CODE = SET_CONF_RET_CODE::ErrJsonParse;
pub const SET_CONF_RET_ERR_ONLY_ONCE: SET_CONF_RET_CODE = SET_CONF_RET_CODE::ErrOnlyOnce;
pub const SET_CONF_RET_ERR_TOO_LONG: SET_CONF_RET_CODE = SET_CONF_RET_CODE::ErrTooLong;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct SetConfRet {
    pub code: SET_CONF_RET_CODE,
    pub msg: [c_char; 1024usize],
}

#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum SET_CONF_RET_CODE {
    Succ = 0,
    ErrPart = -1,
    ErrInner = -2,
    ErrJsonInvalid = -3,
    ErrJsonParse = -4,
    ErrOnlyOnce = -5,
    ErrTooLong = -6,
}

extern "C" {
    pub fn taos_set_config(config: *const c_char) -> SetConfRet;
}
