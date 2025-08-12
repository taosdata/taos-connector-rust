use futures::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::sync::{Mutex, Notify};
use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};

pub type ProxyFn = Arc<dyn Fn(&Message, &mut ProxyContext) -> ProxyAction + Send + Sync>;

#[non_exhaustive]
#[derive(Debug, Clone, Copy)]
pub enum ProxyAction {
    Restart,
    Forward,
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ProxyContext {
    pub req_count: usize,
}

#[derive(Debug, Clone)]
pub struct WsProxy {
    running: Arc<Mutex<bool>>,
    stop_notify: Arc<Notify>,
}

impl WsProxy {
    pub async fn start(listen_addr: SocketAddr, backend_url: String, judge: ProxyFn) -> Arc<Self> {
        let stop_notify = Arc::new(Notify::new());
        let running = Arc::new(Mutex::new(true));
        let proxy = Arc::new(Self {
            stop_notify: stop_notify.clone(),
            running: running.clone(),
        });
        let judge_state = Arc::new(Mutex::new(ProxyContext { req_count: 0 }));

        let proxy_clone = proxy.clone();
        tokio::spawn(async move {
            loop {
                let listener = match TcpListener::bind(listen_addr).await {
                    Ok(l) => l,
                    Err(e) => {
                        eprintln!("Failed to bind: {e}");
                        return;
                    }
                };

                let backend_url = backend_url.clone();
                let judge = judge.clone();
                let judge_state = judge_state.clone();
                let stop_notify = stop_notify.clone();
                let running = running.clone();

                let accept_loop = async {
                    loop {
                        tokio::select! {
                            Ok((stream, _)) = listener.accept() => {
                                let _backend_url = backend_url.clone();
                                let judge = judge.clone();
                                let judge_state = judge_state.clone();
                                let running = running.clone();
                                let stop_notify = stop_notify.clone();
                                tokio::spawn(async move {
                                    let ws_client = match accept_async(stream).await {
                                        Ok(ws) => ws,
                                        Err(e) => {
                                            eprintln!("accept_async error: {e}");
                                            return;
                                        }
                                    };
                                    let (mut client_ws_sink, mut client_ws_stream) = ws_client.split();

                                    // FIXME
                                    let (backend_ws, _) = match connect_async("ws://localhost:6041/ws").await {
                                        Ok(pair) => pair,
                                        Err(e) => {
                                            eprintln!("connect_async error: {e}");
                                            return;
                                        }
                                    };
                                    let (mut backend_ws_sink, mut backend_ws_stream) = backend_ws.split();

                                    // Client -> Backend
                                    let req_to_backend = async {
                                        while let Some(Ok(msg)) = client_ws_stream.next().await {
                                            tracing::trace!("received message from client: {:?}", msg);
                                            let mut state = judge_state.lock().await;
                                            match judge(&msg, &mut *state) {
                                                ProxyAction::Restart => {
                                                    {
                                                        let mut running_guard = running.lock().await;
                                                        *running_guard = false;
                                                    }
                                                    stop_notify.notify_waiters();
                                                    break;
                                                }
                                                ProxyAction::Forward => {
                                                    if backend_ws_sink.send(msg).await.is_err() {
                                                        tracing::error!("failed to send message to backend");
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    };

                                    // Backend -> Client
                                    let resp_to_client = async {
                                        while let Some(Ok(msg)) = backend_ws_stream.next().await {
                                            if client_ws_sink.send(msg).await.is_err() {
                                                break;
                                            }
                                        }
                                    };

                                    tokio::select! {
                                        _ = req_to_backend => {},
                                        _ = resp_to_client => {},
                                    }
                                });
                            }
                            _ = stop_notify.notified() => {
                                break;
                            }
                        }
                    }
                };

                accept_loop.await;

                let mut running_guard = running.lock().await;
                if *running_guard {
                    break;
                } else {
                    *running_guard = true;
                    tracing::info!("ws proxy is restarting...");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
            }
        });

        proxy_clone
    }

    pub async fn stop(&self) {
        self.stop_notify.notify_waiters();
    }
}
