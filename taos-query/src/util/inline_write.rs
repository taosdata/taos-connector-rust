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

use tokio::io::{AsyncWrite, AsyncWriteExt};

use super::AsyncInlinable;

#[async_trait::async_trait]
pub trait AsyncInlinableWrite: AsyncWrite + Send + Unpin {
    #[inline]
    /// Write `usize` length as little endian `N` bytes.
    async fn write_len_with_width<const N: usize>(&mut self, len: usize) -> std::io::Result<usize> {
        self.write_all(&len.to_le_bytes()[0..N]).await?;
        Ok(N)
    }

    #[inline]
    /// Write inlined bytes to writer with specific length width `N`.
    ///
    /// The inlined bytes are constructed as:
    ///
    /// ```text
    /// +--------------+-----------------+
    /// | len: N bytes | data: len bytes |
    /// +--------------+-----------------+
    /// ```
    ///
    ///  ## Safety
    ///
    ///  Write inlined bytes may not be safe if the input bytes length overflows to the width.
    ///  For example, write `256` bytes with length width `1` is not safe.
    async fn write_inlined_bytes<const N: usize>(
        &mut self,
        bytes: &[u8],
    ) -> std::io::Result<usize> {
        debug_assert_eq!(bytes.len() >> (N * 8), 0); // bytes.len() < (2 ^ (8 * N))
        let len = &bytes.len().to_le_bytes()[0..N];
        self.write_all(len).await?;
        self.write_all(bytes).await?;
        Ok(N + bytes.len())
    }

    #[inline]
    /// Write inlined string with specific length width `N`.
    async fn write_inlined_str<const N: usize>(&mut self, s: &str) -> std::io::Result<usize> {
        self.write_inlined_bytes::<N>(s.as_bytes()).await
    }

    #[inline]
    /// Write an inlinable object.
    async fn write_inlinable<T: AsyncInlinable + Sync>(
        &mut self,
        value: &T,
    ) -> std::io::Result<usize>
    where
        Self: Sized,
    {
        // self.write_inlinable(value).await
        T::write_inlined(value, self).await
    }
}

impl<T> AsyncInlinableWrite for T where T: AsyncWrite + Send + Unpin {}
