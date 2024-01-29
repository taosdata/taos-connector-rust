use http::Uri;
use std::{collections::HashMap, path::PathBuf};
use ws_tool::{
    codec::{self, PMDConfig, WindowBit},
    connector::{self, get_host, get_scheme},
    errors::WsError,
    protocol::Mode,
    stream, ClientBuilder,
};

pub struct ClientConfig {
    pub read_buf: usize,
    pub write_buf: usize,
    pub certs: Vec<PathBuf>,
    pub window: Option<WindowBit>,
    pub context_take_over: bool,
    pub extra_headers: HashMap<String, String>,
    pub set_socket_fn: Box<dyn FnMut(&std::net::TcpStream) -> Result<(), WsError> + Send>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            read_buf: Default::default(),
            write_buf: Default::default(),
            certs: Default::default(),
            window: Default::default(),
            context_take_over: Default::default(),
            extra_headers: Default::default(),
            set_socket_fn: Box::new(|_| Ok(())),
        }
    }
}

impl ClientConfig {
    pub fn buffered() -> Self {
        Self {
            read_buf: 8192,
            write_buf: 8192,
            ..Default::default()
        }
    }

    #[allow(unused)]
    pub fn connect_with<C, F>(
        &mut self,
        uri: impl TryInto<Uri, Error = http::uri::InvalidUri>,
        mut check_fn: F,
    ) -> Result<C, WsError>
    where
        F: FnMut(
            String,
            http::Response<()>,
            stream::BufStream<stream::SyncStream>,
        ) -> Result<C, WsError>,
    {
        let (uri, mode, builder) = self.prepare(uri)?;
        let stream = connector::tcp_connect(&uri)?;
        (self.set_socket_fn)(&stream)?;
        let check_fn = |key, resp, stream| {
            let stream = stream::BufStream::with_capacity(self.read_buf, self.write_buf, stream);
            check_fn(key, resp, stream)
        };
        match mode {
            Mode::WS => builder.with_stream(uri, stream::SyncStream::Raw(stream), check_fn),
            Mode::WSS => {
                let host = get_host(&uri)?;
                if cfg!(feature = "rustls") {
                    #[cfg(feature = "rustls")]
                    {
                        let stream = connector::wrap_rustls(stream, host, self.certs.clone())?;
                        builder.with_stream(uri, stream::SyncStream::Rustls(stream), check_fn)
                    }
                    #[cfg(not(feature = "rustls"))]
                    {
                        panic!("")
                    }
                } else if cfg!(feature = "sync-native-tls") {
                    #[cfg(feature = "sync-native-tls")]
                    {
                        let stream = connector::wrap_native_tls(stream, host, self.certs.clone())?;
                        builder.with_stream(uri, stream::SyncStream::NativeTls(stream), check_fn)
                    }
                    #[cfg(not(feature = "sync-native-tls"))]
                    {
                        panic!("")
                    }
                } else {
                    panic!("for ssl connection, rustls or native-tls feature is required")
                }
            }
        }
    }

    #[cfg(feature = "defalte-sync")]
    pub fn defalte_connect_with<C, F>(
        &mut self,
        uri: impl TryInto<Uri, Error = http::uri::InvalidUri>,
        mut check_fn: F,
    ) -> Result<C, WsError>
    where
        F: FnMut(
            String,
            http::Response<()>,
            stream::BufStream<stream::SyncStream>,
        ) -> Result<C, WsError>,
    {
        let (uri, mode, builder) = self.prepare(uri)?;

        let stream = connector::tcp_connect(&uri)?;
        let deflater = codec::DeflateCodec::new();
        let stream = stream::SyncStream::Deflate(stream, deflater);

        (self.set_socket_fn)(&stream)?;
        let check_fn = |key, resp, stream| {
            let stream = stream::BufStream::with_capacity(self.read_buf, self.write_buf, stream);
            check_fn(key, resp, stream)
        };

        let (mut header, data) = self.read_state.async_receive(stream).await?;
        let data = data.to_vec();
        let compressed = header.rsv1;
        let is_data_frame = header.code.is_data();
        if compressed && !is_data_frame {
            return Err(WsError::ProtocolError {
                close_code: 1002,
                error: ProtocolError::CompressedControlFrame,
            });
        }
        if !is_data_frame || !compressed {
            return Ok((header, data.to_vec()));
        }
        let frame = match self.de.as_mut() {
            Some(handler) => {
                let mut de_data = vec![];
                handler
                    .de
                    .de_compress(&[&data, &[0, 0, 255, 255]], &mut de_data)
                    .map_err(|code| WsError::DeCompressFailed(code.to_string()))?;
                if (self.is_server && handler.config.server_no_context_takeover)
                    || (!self.is_server && handler.config.client_no_context_takeover)
                {
                    handler
                        .de
                        .reset()
                        .map_err(|code| WsError::DeCompressFailed(code.to_string()))?;
                    tracing::trace!("reset decompressor state");
                }
                de_data
            }
            None => {
                if header.rsv1 {
                    return Err(WsError::DeCompressFailed(
                        "extension not enabled but got compressed frame".into(),
                    ));
                } else {
                    data
                }
            }
        };

        let mut header = codec::DeflateCodec::decode_header(&frame)?;
        header.rsv1 = false;

        match mode {
            Mode::WS => builder.with_stream(uri, stream::SyncStream::Raw(stream), check_fn),
            Mode::WSS => {
                let host = get_host(&uri)?;
                if cfg!(feature = "rustls") {
                    #[cfg(feature = "rustls")]
                    {
                        let stream = connector::wrap_rustls(stream, host, self.certs.clone())?;
                        builder.with_stream(uri, stream::SyncStream::Rustls(stream), check_fn)
                    }
                    #[cfg(not(feature = "rustls"))]
                    {
                        panic!("")
                    }
                } else if cfg!(feature = "sync-native-tls") {
                    #[cfg(feature = "sync-native-tls")]
                    {
                        let stream = connector::wrap_native_tls(stream, host, self.certs.clone())?;
                        builder.with_stream(uri, stream::SyncStream::NativeTls(stream), check_fn)
                    }
                    #[cfg(not(feature = "sync-native-tls"))]
                    {
                        panic!("")
                    }
                } else {
                    panic!("for ssl connection, rustls or native-tls feature is required")
                }
            }
        }

        deflater.check_fn(key, resp, stream);

        let (mut header, mut data) = self.async_receive_one(stream).await?;
        if !self.config.merge_frame {
            self.fragmented_data.clear();
            self.fragmented_data.append(&mut data);
            break Ok((header, &self.fragmented_data));
        }
        match header.code {
            OpCode::Continue => {
                if !self.fragmented {
                    return Err(WsError::ProtocolError {
                        close_code: 1002,
                        error: ProtocolError::MissInitialFragmentedFrame,
                    });
                }
                let fin = header.fin;
                self.fragmented_data.extend_from_slice(&data);
                if fin {
                    self.fragmented = false;
                    header.code = self.fragmented_type;
                    break Ok((header, &self.fragmented_data));
                } else {
                    continue;
                }
            }
            OpCode::Text | OpCode::Binary => {
                if self.fragmented {
                    return Err(WsError::ProtocolError {
                        close_code: 1002,
                        error: ProtocolError::NotContinueFrameAfterFragmented,
                    });
                }
                if !header.fin {
                    self.fragmented = true;
                    self.fragmented_type = header.code;
                    if header.code == OpCode::Text
                        && self.config.validate_utf8.is_fast_fail()
                        && simdutf8::basic::from_utf8(&data).is_err()
                    {
                        return Err(WsError::ProtocolError {
                            close_code: 1007,
                            error: ProtocolError::InvalidUtf8,
                        });
                    }
                    self.fragmented_data.clear();
                    self.fragmented_data.extend_from_slice(&data);
                    continue;
                } else {
                    if header.code == OpCode::Text
                        && self.config.validate_utf8.should_check()
                        && simdutf8::basic::from_utf8(&data).is_err()
                    {
                        return Err(WsError::ProtocolError {
                            close_code: 1007,
                            error: ProtocolError::InvalidUtf8,
                        });
                    }
                    self.fragmented_data.clear();
                    self.fragmented_data.extend_from_slice(&data);
                    break Ok((header, &self.fragmented_data));
                }
            }
            OpCode::Close | OpCode::Ping | OpCode::Pong => {
                self.control_buf = data;
                break Ok((header, &self.control_buf));
            }
            _ => break Err(WsError::UnsupportedFrame(header.code)),
        }
    }

    #[cfg(feature = "sync")]
    pub fn connect(
        &mut self,
        uri: impl TryInto<Uri, Error = http::uri::InvalidUri>,
    ) -> Result<codec::DeflateCodec<stream::BufStream<stream::SyncStream>>, WsError> {
        self.connect_with(uri, codec::DeflateCodec::check_fn)
    }

    #[allow(unused)]
    pub async fn async_connect_with<C, F>(
        &mut self,
        uri: impl TryInto<Uri, Error = http::uri::InvalidUri>,
        mut check_fn: F,
    ) -> Result<C, WsError>
    where
        F: FnMut(
            String,
            http::Response<()>,
            tokio::io::BufStream<stream::AsyncStream>,
        ) -> Result<C, WsError>,
    {
        let (uri, mode, builder) = self.prepare(uri)?;
        tracing::trace!("connecting uri: {:?}", &uri);
        let stream = connector::async_tcp_connect(&uri).await?;
        let stream = stream.into_std()?;
        (self.set_socket_fn)(&stream)?;
        let stream = tokio::net::TcpStream::from_std(stream)?;
        let check_fn = |key, resp, stream: stream::AsyncStream| {
            let stream = tokio::io::BufStream::with_capacity(self.read_buf, self.write_buf, stream);
            check_fn(key, resp, stream)
        };
        match mode {
            Mode::WS => {
                builder
                    .async_with_stream(uri, stream::AsyncStream::Raw(stream), check_fn)
                    .await
            }
            Mode::WSS => {
                let host = get_host(&uri)?;
                if cfg!(feature = "rustls") {
                    #[cfg(feature = "rustls")]
                    {
                        let stream =
                            connector::async_wrap_rustls(stream, host, self.certs.clone()).await?;
                        builder
                            .async_with_stream(
                                uri,
                                stream::AsyncStream::Rustls(tokio_rustls::TlsStream::Client(
                                    stream,
                                )),
                                check_fn,
                            )
                            .await
                    }
                    #[cfg(not(feature = "rustls"))]
                    {
                        panic!("")
                    }
                } else if cfg!(feature = "native-tls") {
                    #[cfg(feature = "native-tls")]
                    {
                        let stream =
                            connector::async_wrap_native_tls(stream, host, self.certs.clone())
                                .await?;
                        builder
                            .async_with_stream(
                                uri,
                                stream::AsyncStream::NativeTls(stream),
                                check_fn,
                            )
                            .await
                    }
                    #[cfg(not(feature = "native-tls"))]
                    {
                        panic!("")
                    }
                } else {
                    panic!("for ssl connection, rustls or native-tls feature is required")
                }
            }
        }
    }

    #[cfg(feature = "defalte-async")]
    pub async fn async_defalte_connect_with<C, F> (
        &mut self,
        uri: impl TryInto<Uri, Error = http::uri::InvalidUri>,
        mut check_fn: F,
    ) -> Result<C, WsError>
    where
        F: FnMut(
            String,
            http::Response<()>,
            tokio::io::BufStream<stream::AsyncStream>,
        ) -> Result<C, WsError>,
    {
        let (uri, mode, builder) = self.prepare(uri)?;

        let stream = connector::async_tcp_connect(&uri).await?;
        let deflater = codec::AsyncDeflateCodec::new();
        let stream = stream::AsyncStream::Deflate(stream, deflater);

        (self.set_socket_fn)(&stream)?;
        let check_fn = |key, resp, stream| {
            let stream = tokio::io::BufStream::with_capacity(self.read_buf, self.write_buf, stream);
            check_fn(key, resp, stream)
        };

        let (mut header, data) = self.read_state.async_receive(stream).await?;
        let data = data.to_vec();
        let compressed = header.rsv1;
        let is_data_frame = header.code.is_data();
        if compressed && !is_data_frame {
            return Err(WsError::ProtocolError {
                close_code: 1002,
                error: ProtocolError::CompressedControlFrame,
            });
        }
        if !is_data_frame || !compressed {
            return Ok((header, data.to_vec()));
        }
        let frame = match self.de.as_mut() {
            Some(handler) => {
                let mut de_data = vec![];
                handler
                    .de
                    .de_compress(&[&data, &[0, 0, 255, 255]], &mut de_data)
                    .map_err(|code| WsError::DeCompressFailed(code.to_string()))?;
                if (self.is_server && handler.config.server_no_context_takeover)
                    || (!self.is_server && handler.config.client_no_context_takeover)
                {
                    handler
                        .de
                        .reset()
                        .map_err(|code| WsError::DeCompressFailed(code.to_string()))?;
                    tracing::trace!("reset decompressor state");
                }
                de_data
            }
            None => {
                if header.rsv1 {
                    return Err(WsError::DeCompressFailed(
                        "extension not enabled but got compressed frame".into(),
                    ));
                } else {
                    data
                }
            }
        };

        let mut header = codec::DeflateCodec::decode_header(&frame)?;
        header.rsv1 = false;

        match mode {
            Mode::WS => {
                builder
                    .async_with_stream(uri, stream::AsyncStream::Raw(stream), check_fn)
                    .await
            }
            Mode::WSS => {
                let host = get_host(&uri)?;
                if cfg!(feature = "rustls") {
                    #[cfg(feature = "rustls")]
                    {
                        let stream =
                            connector::async_wrap_rustls(stream, host, self.certs.clone()).await?;
                        builder
                            .async_with_stream(
                                uri,
                                stream::AsyncStream::Rustls(tokio_rustls::TlsStream::Client(
                                    stream,
                                )),
                                check_fn,
                            )
                            .await
                    }
                    #[cfg(not(feature = "rustls"))]
                    {
                        panic!("")
                    }
                } else if cfg!(feature = "native-tls") {
                    #[cfg(feature = "native-tls")]
                    {
                        let stream =
                            connector::async_wrap_native_tls(stream, host, self.certs.clone())
                                .await?;
                        builder
                            .async_with_stream(
                                uri,
                                stream::AsyncStream::NativeTls(stream),
                                check_fn,
                            )
                            .await
                    }
                    #[cfg(not(feature = "native-tls"))]
                    {
                        panic!("")
                    }
                } else {
                    panic!("for ssl connection, rustls or native-tls feature is required")
                }
            }
        }

        deflater.check_fn(key, resp, stream);

        let (mut header, mut data) = self.async_receive_one(stream).await?;
        if !self.config.merge_frame {
            self.fragmented_data.clear();
            self.fragmented_data.append(&mut data);
            break Ok((header, &self.fragmented_data));
        }
        match header.code {
            OpCode::Continue => {
                if !self.fragmented {
                    return Err(WsError::ProtocolError {
                        close_code: 1002,
                        error: ProtocolError::MissInitialFragmentedFrame,
                    });
                }
                let fin = header.fin;
                self.fragmented_data.extend_from_slice(&data);
                if fin {
                    self.fragmented = false;
                    header.code = self.fragmented_type;
                    break Ok((header, &self.fragmented_data));
                } else {
                    continue;
                }
            }
            OpCode::Text | OpCode::Binary => {
                if self.fragmented {
                    return Err(WsError::ProtocolError {
                        close_code: 1002,
                        error: ProtocolError::NotContinueFrameAfterFragmented,
                    });
                }
                if !header.fin {
                    self.fragmented = true;
                    self.fragmented_type = header.code;
                    if header.code == OpCode::Text
                        && self.config.validate_utf8.is_fast_fail()
                        && simdutf8::basic::from_utf8(&data).is_err()
                    {
                        return Err(WsError::ProtocolError {
                            close_code: 1007,
                            error: ProtocolError::InvalidUtf8,
                        });
                    }
                    self.fragmented_data.clear();
                    self.fragmented_data.extend_from_slice(&data);
                    continue;
                } else {
                    if header.code == OpCode::Text
                        && self.config.validate_utf8.should_check()
                        && simdutf8::basic::from_utf8(&data).is_err()
                    {
                        return Err(WsError::ProtocolError {
                            close_code: 1007,
                            error: ProtocolError::InvalidUtf8,
                        });
                    }
                    self.fragmented_data.clear();
                    self.fragmented_data.extend_from_slice(&data);
                    break Ok((header, &self.fragmented_data));
                }
            }
            OpCode::Close | OpCode::Ping | OpCode::Pong => {
                self.control_buf = data;
                break Ok((header, &self.control_buf));
            }
            _ => break Err(WsError::UnsupportedFrame(header.code)),
        }
    }

    pub async fn async_connect(
        &mut self,
        uri: impl TryInto<Uri, Error = http::uri::InvalidUri>,
    ) -> Result<codec::AsyncDeflateCodec<tokio::io::BufStream<stream::AsyncStream>>, WsError> {
        self.async_connect_with(uri, codec::AsyncDeflateCodec::check_fn)
            .await
    }

    fn prepare(
        &mut self,
        uri: impl TryInto<Uri, Error = http::uri::InvalidUri>,
    ) -> Result<(Uri, Mode, ClientBuilder), WsError> {
        let uri = uri
            .try_into()
            .map_err(|e| WsError::InvalidUri(e.to_string()))?;
        let mode = get_scheme(&uri)?;
        let mut builder = ClientBuilder::new();
        let pmd_conf = self.window.map(|w| PMDConfig {
            server_no_context_takeover: self.context_take_over,
            client_no_context_takeover: self.context_take_over,
            server_max_window_bits: w,
            client_max_window_bits: w,
        });
        if let Some(conf) = pmd_conf {
            builder = builder.extension(conf.ext_string())
        }
        for (k, v) in &self.extra_headers {
            builder = builder.header(k, v);
        }
        Ok((uri, mode, builder))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use maplit::hashmap;
    use tracing::*;
    use tracing_subscriber::util::SubscriberInitExt;

    #[tokio::test]
    async fn test_async_connect_with() {
        let _subscriber = tracing_subscriber::fmt::fmt()
            .with_max_level(Level::INFO)
            .with_file(true)
            .with_line_number(true)
            .finish();
        let _ = _subscriber.try_init();

        let check_fn =
            |_: String, _: http::Response<()>, _: tokio::io::BufStream<stream::AsyncStream>| Ok(());

        // Test Accept-Encoding with gzip, deflate
        let mut config = ClientConfig {
            window: Some(WindowBit::Fifteen),

            extra_headers: hashmap! {
                "Accept-Encoding".to_string() => "gzip, deflate".to_string(),
            },
            ..Default::default()
        };

        let result = config
            .async_connect_with("ws://localhost:6041/ws", check_fn)
            .await;
        assert!(result.is_ok());

        // Test Accept-Encoding without deflate
        let mut config = ClientConfig {
            window: Some(WindowBit::Fifteen),

            extra_headers: hashmap! {
                "Accept-Encoding".to_string() => "gzip".to_string(),
            },
            ..Default::default()
        };

        let result = config
            .async_connect_with("ws://localhost:6041/ws", check_fn)
            .await;
        assert!(result.is_ok());
    }
}
