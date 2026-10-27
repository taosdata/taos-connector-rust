// Copyright (c) 2023 TAOS Data, Inc.
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

use anyhow::Context;
use taos_error::*;

fn main() -> anyhow::Result<()> {
    #[inline(never)]
    fn raise_anyhow_error() -> anyhow::Result<()> {
        Err(anyhow::anyhow!("fake error string"))
    }
    #[inline(never)]
    fn raise_anyhow_error2() -> anyhow::Result<()> {
        raise_anyhow_error().context("Another error")
    }
    #[inline(never)]
    fn raise_anyhow_error3() -> anyhow::Result<()> {
        raise_anyhow_error2().context("Above error")
    }
    #[inline(never)]
    fn raise_error() -> Result<()> {
        Err(Error::new(0x001, "taosc error"))?;
        raise_anyhow_error3()?;
        Ok(())
        // Err(Error::from_anyhow())
    }
    raise_error().map_err(|err| err.context("raise error"))?;
    Ok(())
}
