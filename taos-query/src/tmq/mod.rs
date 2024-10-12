use std::{fmt::Debug, pin::Pin, str::FromStr, time::Duration};

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    common::{RawData, RawMeta},
    JsonMeta, RawBlock, RawResult,
};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Timeout {
    /// Wait forever.
    Never,
    /// Try not block, will directly return when set timeout as `None`.
    None,
    /// Wait for a duration of time.
    Duration(Duration),
}

impl Timeout {
    pub fn from_secs(secs: u64) -> Self {
        Self::Duration(Duration::from_secs(secs))
    }

    pub fn from_millis(millis: u64) -> Self {
        Self::Duration(Duration::from_millis(millis))
    }

    pub fn never() -> Self {
        Self::Never
    }

    pub fn none() -> Self {
        Self::None
    }
    pub fn as_raw_timeout(&self) -> i64 {
        match self {
            Timeout::Never => -1,
            Timeout::None => 0,
            Timeout::Duration(t) => t.as_millis() as _,
        }
    }

    pub fn as_duration(&self) -> Duration {
        match self {
            Timeout::Never => Duration::from_secs(i64::MAX as u64 / 1000),
            Timeout::None => Duration::from_secs(0),
            Timeout::Duration(t) => *t,
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum TimeoutError {
    #[error("empty timeout value")]
    Empty,
    #[error("invalid timeout expression `{0}`: {1}")]
    Invalid(String, String),
}

fn is_numeric_or_sign(s: char) -> bool {
    if s != '+' && s != '-' && !s.is_digit(10) {
        return false;
    }
    return true;
}

fn validate_duration_string(duration_string: &str) -> Result<(), String> {
    if !duration_string.contains('e') && !duration_string.contains('E') {
        return Ok(());
    }

    let parts: Vec<&str> = duration_string.split_whitespace().collect();
    for part in parts {
        if part.contains('e') || part.contains('E') {
            // Check if it's an exponent form
            let parts_vec: Vec<&str> = part.split(|c| c == 'e' || c == 'E').collect();
            if parts_vec.len() == 2 && !parts_vec[0].is_empty() && !parts_vec[1].is_empty() {
                let last = parts_vec[0].chars().last().unwrap();
                let first = parts_vec[1].chars().next().unwrap();
                if is_numeric_or_sign(first) && is_numeric_or_sign(last) {
                    return Err("Exponent not allowed".to_string());
                }
            }
        }
    }

    Ok(())
}

impl FromStr for Timeout {
    type Err = TimeoutError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(TimeoutError::Empty);
        }
        match s.to_lowercase().as_str() {
            "never" => Ok(Timeout::Never),
            "none" => Ok(Timeout::None),
            _ => match validate_duration_string(s) {
                Ok(_) => parse_duration::parse(s)
                    .map(Timeout::Duration)
                    .map_err(|err| TimeoutError::Invalid(s.to_string(), err.to_string())),
                Err(err) => Err(TimeoutError::Invalid(s.to_string(), err.to_string())),
            },
        }
    }
}

pub enum MessageSet<M, D> {
    Meta(M),
    Data(D),
    MetaData(M, D),
}

impl<M, D> Debug for MessageSet<M, D>
where
    M: Debug,
    D: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Meta(m) => f.debug_tuple("Meta").field(m).finish(),
            Self::Data(d) => f.debug_tuple("Data").field(d).finish(),
            Self::MetaData(m, d) => f.debug_tuple("MetaData").field(m).field(d).finish(),
        }
    }
}

impl<M, D> MessageSet<M, D> {
    pub fn into_meta(self) -> Option<M> {
        match self {
            MessageSet::Meta(m) => Some(m),
            MessageSet::Data(_) => None,
            MessageSet::MetaData(m, _) => Some(m),
        }
    }
    pub fn into_data(self) -> Option<D> {
        match self {
            MessageSet::Meta(_) => None,
            MessageSet::Data(d) => Some(d),
            MessageSet::MetaData(_, d) => Some(d),
        }
    }

    pub fn has_meta(&self) -> bool {
        matches!(self, &MessageSet::Meta(_) | &MessageSet::MetaData(_, _))
    }
    pub fn has_data(&self) -> bool {
        matches!(self, &MessageSet::Data(_) | &MessageSet::MetaData(_, _))
    }

    pub fn meta(&self) -> Option<&M> {
        match self {
            MessageSet::Meta(m) => Some(m),
            MessageSet::Data(_) => None,
            MessageSet::MetaData(m, _) => Some(m),
        }
    }
    pub fn data(&mut self) -> Option<&mut D> {
        match self {
            MessageSet::Meta(_) => None,
            MessageSet::Data(d) => Some(d),
            MessageSet::MetaData(_, d) => Some(d),
        }
    }
}

#[async_trait::async_trait]
pub trait IsAsyncMeta {
    async fn as_raw_meta(&self) -> RawResult<RawMeta>;

    async fn as_json_meta(&self) -> RawResult<JsonMeta>;
}

impl<T> IsMeta for T
where
    T: IsAsyncMeta + SyncOnAsync,
{
    fn as_raw_meta(&self) -> RawResult<RawMeta> {
        crate::block_in_place_or_global(T::as_raw_meta(self))
    }

    fn as_json_meta(&self) -> RawResult<JsonMeta> {
        crate::block_in_place_or_global(T::as_json_meta(self))
    }
}

#[async_trait::async_trait]
impl<T> IsAsyncMeta for T
where
    T: IsMeta + AsyncOnSync + Send + Sync,
{
    async fn as_raw_meta(&self) -> RawResult<RawMeta> {
        <T as IsMeta>::as_raw_meta(self)
    }

    async fn as_json_meta(&self) -> RawResult<JsonMeta> {
        <T as IsMeta>::as_json_meta(self)
    }
}

pub trait IsMeta {
    fn as_raw_meta(&self) -> RawResult<RawMeta>;

    fn as_json_meta(&self) -> RawResult<JsonMeta>;
}

#[async_trait::async_trait]
pub trait IsAsyncData {
    async fn as_raw_data(&self) -> RawResult<RawData>;
    async fn fetch_raw_block(&self) -> RawResult<Option<RawBlock>>;
}

pub trait IsData {
    fn as_raw_data(&self) -> RawResult<RawData>;
    fn fetch_raw_block(&self) -> RawResult<Option<RawBlock>>;
}

#[async_trait::async_trait]
pub trait AsyncMessage {
    /// Check if the message contains meta.
    fn has_meta(&self) -> bool;
    /// Check if the message contains data.
    fn has_data(&self) -> bool;

    /// Return raw data as bytes.
    async fn as_raw_data(&self) -> RawResult<RawData>;

    /// Extract meta message.
    async fn get_meta(&self) -> RawResult<Option<RawMeta>>;
    async fn fetch_raw_block(&self) -> RawResult<Option<RawBlock>>;
}

pub type VGroupId = i32;
pub type Offset = i64;
pub type Timing = i64;

/// Extract offset information.
pub trait IsOffset {
    /// Database name for current message
    fn database(&self) -> &str;

    /// Topic name for current message.
    fn topic(&self) -> &str;

    /// VGroup id for current message.
    fn vgroup_id(&self) -> VGroupId;

    /// offset of current message.
    fn offset(&self) -> Offset;

    /// timing cost for current message.
    fn timing(&self) -> Timing;
}

#[repr(C)]
#[derive(Debug, Default, Copy, Clone, Deserialize, Serialize)]
pub struct Assignment {
    vgroup_id: VGroupId,
    offset: i64,
    begin: i64,
    end: i64,
}

impl Assignment {
    pub fn new(vgroup_id: VGroupId, offset: i64, begin: i64, end: i64) -> Self {
        Self {
            vgroup_id,
            offset,
            begin,
            end,
        }
    }

    pub fn vgroup_id(&self) -> VGroupId {
        self.vgroup_id
    }

    pub fn current_offset(&self) -> i64 {
        self.offset
    }

    pub fn begin(&self) -> i64 {
        self.begin
    }

    pub fn end(&self) -> i64 {
        self.end
    }
}

pub trait AsConsumer: Sized {
    type Offset: IsOffset;
    type Meta: IsMeta;
    type Data: IntoIterator<Item = RawResult<RawBlock>>;

    /// Default timeout getter for message stream.
    fn default_timeout(&self) -> Timeout {
        Timeout::Never
    }

    fn subscribe<T: Into<String>, I: IntoIterator<Item = T> + Send>(
        &mut self,
        topics: I,
    ) -> RawResult<()>;

    /// None means wait until next message come.
    fn recv_timeout(
        &self,
        timeout: Timeout,
    ) -> RawResult<Option<(Self::Offset, MessageSet<Self::Meta, Self::Data>)>>;

    fn recv(&self) -> RawResult<Option<(Self::Offset, MessageSet<Self::Meta, Self::Data>)>> {
        self.recv_timeout(self.default_timeout())
    }

    fn iter_data_only(
        &self,
        timeout: Timeout,
    ) -> Box<dyn '_ + Iterator<Item = RawResult<(Self::Offset, Self::Data)>>> {
        Box::new(
            self.iter_with_timeout(timeout)
                .filter_map_ok(|m| m.1.into_data().map(|data| (m.0, data))),
        )
    }

    fn iter_with_timeout(&self, timeout: Timeout) -> MessageSetsIter<'_, Self> {
        MessageSetsIter {
            consumer: self,
            timeout,
        }
    }

    fn iter(&self) -> MessageSetsIter<'_, Self> {
        self.iter_with_timeout(self.default_timeout())
    }

    fn commit(&self, offset: Self::Offset) -> RawResult<()>;
    fn commit_all(&self) -> RawResult<()>;

    fn commit_offset(&self, topic_name: &str, vgroup_id: VGroupId, offset: i64) -> RawResult<()>;

    fn unsubscribe(self) {
        drop(self)
    }

    fn list_topics(&self) -> RawResult<Vec<String>>;

    fn assignments(&self) -> Option<Vec<(String, Vec<Assignment>)>>;

    fn offset_seek(&mut self, topic: &str, vg_id: VGroupId, offset: i64) -> RawResult<()>;

    fn committed(&self, topic: &str, vgroup_id: VGroupId) -> RawResult<i64>;

    fn position(&self, topic: &str, vgroup_id: VGroupId) -> RawResult<i64>;
}

pub struct MessageSetsIter<'a, C> {
    consumer: &'a C,
    timeout: Timeout,
}

impl<'a, C> Iterator for MessageSetsIter<'a, C>
where
    C: AsConsumer,
{
    type Item = RawResult<(C::Offset, MessageSet<C::Meta, C::Data>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.consumer.recv_timeout(self.timeout).transpose()
    }
}

#[async_trait::async_trait]
pub trait AsAsyncConsumer: Sized + Send + Sync {
    type Offset: IsOffset;
    type Meta: IsAsyncMeta;
    type Data: IsAsyncData;

    fn default_timeout(&self) -> Timeout;

    async fn subscribe<T: Into<String>, I: IntoIterator<Item = T> + Send>(
        &mut self,
        topics: I,
    ) -> RawResult<()>;

    /// None means wait until next message come.
    async fn recv_timeout(
        &self,
        timeout: Timeout,
    ) -> RawResult<Option<(Self::Offset, MessageSet<Self::Meta, Self::Data>)>>;

    fn stream_with_timeout(
        &self,
        timeout: Timeout,
    ) -> Pin<
        Box<
            dyn '_
                + Send
                + futures::Stream<
                    Item = RawResult<(Self::Offset, MessageSet<Self::Meta, Self::Data>)>,
                >,
        >,
    > {
        Box::pin(futures::stream::unfold((), move |_| async move {
            let weather = self.recv_timeout(timeout).await.transpose();
            weather.map(|res| (res, ()))
        }))
    }

    fn stream(
        &self,
    ) -> Pin<
        Box<
            dyn '_
                + Send
                + futures::Stream<
                    Item = RawResult<(Self::Offset, MessageSet<Self::Meta, Self::Data>)>,
                >,
        >,
    > {
        self.stream_with_timeout(self.default_timeout())
    }

    async fn commit(&self, offset: Self::Offset) -> RawResult<()>;
    async fn commit_all(&self) -> RawResult<()>;

    async fn commit_offset(
        &self,
        topic_name: &str,
        vgroup_id: VGroupId,
        offset: i64,
    ) -> RawResult<()>;

    async fn unsubscribe(self) {
        drop(self)
    }

    async fn list_topics(&self) -> RawResult<Vec<String>>;

    async fn assignments(&self) -> Option<Vec<(String, Vec<Assignment>)>>;

    async fn topic_assignment(&self, topic: &str) -> Vec<Assignment>;

    async fn offset_seek(&mut self, topic: &str, vgroup_id: VGroupId, offset: i64)
        -> RawResult<()>;

    async fn committed(&self, topic: &str, vgroup_id: VGroupId) -> RawResult<i64>;

    async fn position(&self, topic: &str, vgroup_id: VGroupId) -> RawResult<i64>;
}

/// Marker trait to impl sync on async impl.
pub trait SyncOnAsync {}

pub trait AsyncOnSync {}

impl<C> AsConsumer for C
where
    C: AsAsyncConsumer + SyncOnAsync,
    C::Meta: IsMeta,
    C::Data: IntoIterator<Item = RawResult<RawBlock>>,
{
    type Offset = C::Offset;

    type Meta = C::Meta;

    type Data = C::Data;

    fn subscribe<T: Into<String>, I: IntoIterator<Item = T> + Send>(
        &mut self,
        topics: I,
    ) -> RawResult<()> {
        crate::block_in_place_or_global(<C as AsAsyncConsumer>::subscribe(self, topics))
    }

    fn recv_timeout(
        &self,
        timeout: Timeout,
    ) -> RawResult<Option<(Self::Offset, MessageSet<Self::Meta, Self::Data>)>> {
        crate::block_in_place_or_global(<C as AsAsyncConsumer>::recv_timeout(self, timeout))
    }

    fn commit(&self, offset: Self::Offset) -> RawResult<()> {
        crate::block_in_place_or_global(<C as AsAsyncConsumer>::commit(self, offset))
    }

    fn commit_all(&self) -> RawResult<()> {
        crate::block_in_place_or_global(<C as AsAsyncConsumer>::commit_all(self))
    }

    fn commit_offset(&self, topic_name: &str, vgroup_id: VGroupId, offset: i64) -> RawResult<()> {
        crate::block_in_place_or_global(<C as AsAsyncConsumer>::commit_offset(
            self, topic_name, vgroup_id, offset,
        ))
    }

    fn list_topics(&self) -> RawResult<Vec<String>> {
        crate::block_in_place_or_global(<C as AsAsyncConsumer>::list_topics(self))
    }

    fn assignments(&self) -> Option<Vec<(String, Vec<Assignment>)>> {
        crate::block_in_place_or_global(<C as AsAsyncConsumer>::assignments(self))
    }

    fn offset_seek(&mut self, topic: &str, vg_id: VGroupId, offset: i64) -> RawResult<()> {
        crate::block_in_place_or_global(<C as AsAsyncConsumer>::offset_seek(
            self, topic, vg_id, offset,
        ))
    }

    fn committed(&self, topic: &str, vgroup_id: VGroupId) -> RawResult<i64> {
        crate::block_in_place_or_global(<C as AsAsyncConsumer>::committed(self, topic, vgroup_id))
    }

    fn position(&self, topic: &str, vgroup_id: VGroupId) -> RawResult<i64> {
        crate::block_in_place_or_global(<C as AsAsyncConsumer>::position(self, topic, vgroup_id))
    }
}

// #[async_trait::async_trait]
// impl<C> AsAsyncConsumer for C
// where
//     C: AsConsumer + AsyncOnSync + Send + Sync + 'static,
//     C::Error: 'static + Sync + Send,
//     C::Meta: IsAsyncMeta + Send,
//     C::Offset: 'static + Sync + Send,
//     C::Data: 'static + Send + Sync,
// {
//     type Error = C::Error;

//     type Offset = C::Offset;

//     type Meta = C::Meta;

//     type Data = C::Data;

//     async fn subscribe<T: Into<String>, I: IntoIterator<Item = T> + Send>(
//         &mut self,
//         topics: I,
//     ) -> Result<()> {
//         <C as AsConsumer>::subscribe(self, topics)
//     }

//     async fn recv_timeout(
//         &self,
//         timeout: Timeout,
//     ) -> Result<Option<(Self::Offset, MessageSet<Self::Meta, Self::Data>)>> {
//         <C as AsConsumer>::recv_timeout(self, timeout)
//     }

//     async fn commit(&self, offset: Self::Offset) -> Result<()> {
//         <C as AsConsumer>::commit(self, offset)
//     }
// }
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_duration_string() {
        // Valid cases
        assert_eq!(validate_duration_string("1.23e4"), Ok(()));
        assert_eq!(validate_duration_string("1.23e-4"), Ok(()));
        assert_eq!(validate_duration_string("1.23"), Ok(()));

        // Invalid exponent
        assert_eq!(
            validate_duration_string("1.23e11"),
            Err("Exponent out of range".to_string())
        );
        assert_eq!(
            validate_duration_string("1.23e-11"),
            Err("Exponent out of range".to_string())
        );

        // Invalid format
        assert_eq!(
            validate_duration_string("1.23eabc"),
            Err("Invalid exponent".to_string())
        );
    }

    #[test]
    fn test_timeout_from_str() {
        // Valid cases
        assert_eq!(validate_duration_string("5sec"), Ok(()));
        assert_eq!(validate_duration_string("5s"), Ok(()));
        assert_eq!(validate_duration_string("123s"), Ok(()));
        assert_eq!(validate_duration_string("1.23ms"), Ok(()));
        assert_eq!(validate_duration_string("1 day"), Ok(()));
        assert_eq!(validate_duration_string("5econds"), Ok(())); // 'e' in the middle of a word

        // Invalid cases
        assert_eq!(
            validate_duration_string("1e10 sec"),
            Err("Exponent not allowed".to_string())
        );
        assert_eq!(
            validate_duration_string("1E10 ms"),
            Err("Exponent not allowed".to_string())
        );

        assert_eq!(
            validate_duration_string("1E10ms"),
            Err("Exponent not allowed".to_string())
        );
        assert_eq!(
            validate_duration_string("1e-1"),
            Err("Exponent not allowed".to_string())
        );
        assert_eq!(
            validate_duration_string("1.84467e19 seconds"),
            Err("Exponent not allowed".to_string())
        );

        assert_eq!(
            validate_duration_string("1E-1"),
            Err("Exponent not allowed".to_string())
        );
    }
}
