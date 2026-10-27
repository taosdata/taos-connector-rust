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

use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};

/// Information for `show topics` record.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Topic {
    topic_name: String,
    db_name: String,
    create_time: DateTime<Local>,
    sql: String,
}

impl Topic {
    /// Topic name.
    pub fn name(&self) -> &str {
        &self.topic_name
    }

    /// Database name of the topic.
    pub fn db_name(&self) -> &str {
        &self.db_name
    }

    /// Created time of the topic
    pub fn create_time(&self) -> &DateTime<Local> {
        &self.create_time
    }

    /// The create sql for the topic
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Check if the topic is a database-scope topic, otherwise is table-scope topic.
    pub fn is_db_topic(&self) -> bool {
        self.sql.contains("as database")
    }

    pub fn is_stable_topic(&self) -> bool {
        self.sql.contains("as stable")
    }
}
