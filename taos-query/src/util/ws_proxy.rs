use futures::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Notify};
use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};

// pub type JudgeFn = Arc<dyn Fn(&Message) -> bool + Send + Sync>;

pub type JudgeFn = Arc<dyn Fn(&Message, &mut JudgeState) -> JudgeAction + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JudgeAction {
    RestartProxy,
    Forward,
    // 你可以扩展更多动作
}

// TODO: use context
pub struct JudgeState {
    pub init_count: usize,
    // 你可以加更多字段
}

#[allow(dead_code)]
pub struct WsProxy {
    stop_notify: Arc<Notify>,
    running: Arc<Mutex<bool>>,
}

impl WsProxy {
    // TODO: <A: ToSocketAddrs>
    pub async fn start(listen_addr: SocketAddr, backend_url: String, judge: JudgeFn) -> Arc<Self> {
        let stop_notify = Arc::new(Notify::new());
        let running = Arc::new(Mutex::new(true));
        let proxy = Arc::new(Self {
            stop_notify: stop_notify.clone(),
            running: running.clone(),
        });
        let judge_state = Arc::new(Mutex::new(JudgeState { init_count: 0 }));

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
                                                JudgeAction::RestartProxy => {
                                                    {
                                                        let mut running_guard = running.lock().await;
                                                        *running_guard = false;
                                                    }
                                                    stop_notify.notify_waiters();
                                                    break;
                                                }
                                                JudgeAction::Forward => {
                                                    // backend_ws_sink.send(msg).await.ok();
                                                    if backend_ws_sink.send(msg).await.is_err() {
                                                        break;
                                                    }
                                                }
                                            }
                                            // if judge(&msg) {
                                            //     // 满足条件，优雅重启
                                            //     {
                                            //         let mut running_guard = running.lock().await;
                                            //         *running_guard = false;
                                            //     }
                                            //     stop_notify.notify_waiters();
                                            //     break;
                                            // } else {
                                            //     if backend_ws_sink.send(msg).await.is_err() {
                                            //         break;
                                            //     }
                                            // }
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

                // 检查是否需要重启
                let mut running_guard = running.lock().await;
                if *running_guard {
                    break; // 正常停止
                } else {
                    // 重启
                    *running_guard = true;
                    println!("Proxy restarting...");
                    // 等待端口释放
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
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
