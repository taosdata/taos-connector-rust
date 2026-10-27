// Copyright (c) 2025 TAOS Data, Inc.
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

use std::io::BufRead;

use taos_log::layer::TaosLayer;
use taos_log::writer::RollingFileAppender;
use taos_log::QidManager;
use tracing::level_filters::LevelFilter;
use tracing_log::LogTracer;
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Clone)]
struct Qid(u64);

impl QidManager for Qid {
    fn init() -> Self {
        Self(9223372036854775807)
    }

    fn get(&self) -> u64 {
        self.0
    }
}

impl From<u64> for Qid {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

fn main() {
    let mut layers = Vec::with_capacity(2);
    let appender = RollingFileAppender::builder(".")
        .compress(true)
        .reserved_disk_size("1GB")
        .rotation_size("1GB")
        .build()
        .unwrap();
    layers.push(
        TaosLayer::<Qid>::new(appender)
            .with_filter(LevelFilter::INFO)
            .boxed(),
    );

    if cfg!(debug_assertions) {
        layers.push(
            TaosLayer::<Qid, _, _>::new(std::io::stdout)
                .with_location()
                .with_filter(LevelFilter::INFO)
                .boxed(),
        );
    }

    tracing_subscriber::registry().with(layers).init();
    LogTracer::init().unwrap();

    let stdin = std::io::stdin();
    let mut stdin_lock = stdin.lock();
    loop {
        tracing::info_span!("outer", "k" = "kkk").in_scope(|| {
            tracing::info!(a = "aaa", b = "bbb", "outer example");
            log::info!("this is log info log");

            tracing::info_span!("inner").in_scope(|| {
                tracing::trace!("trace example");
                tracing::warn!("warn example");
                tracing::debug!("inner info log example");
                tracing::error!(c = "ccc", d = "ddd", "inner example");
            });
        });

        stdin_lock.read_line(&mut String::new()).unwrap();
    }
}
