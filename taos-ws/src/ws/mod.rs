#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::query::infra::WsResArgs;
    use crate::query::WsConnReq;

    use super::*;
    use futures::SinkExt;
    use futures::StreamExt;
    use tokio_websockets::{ClientBuilder, Message};

    #[tokio::test]
    async fn test_ws() {
        let uri = "ws://127.0.0.1:6041/rest/ws".try_into().unwrap();
        let (client, resp) = ClientBuilder::from_uri(uri).connect().await.unwrap();
        dbg!(resp);

        let (mut source, mut sink) = client.split();
        // crate::query::infra:
        let version = crate::query::infra::WsSend::Version;

        source
            .send(Message::text(serde_json::to_string(&version).unwrap()))
            .await
            .unwrap();

        let conn = crate::query::infra::WsSend::Conn {
            req_id: 0,
            req: WsConnReq::new("root", "taosdata"),
        };
        source
            .send(Message::text(serde_json::to_string(&version).unwrap()))
            .await
            .unwrap();
        source
            .send(Message::text(serde_json::to_string(&conn).unwrap()))
            .await
            .unwrap();

        let query = crate::query::infra::WsSend::Query {
            req_id: 1,
            sql: "show databases".into(),
        };
        source
            .send(Message::text(serde_json::to_string(&query).unwrap()))
            .await
            .unwrap();

        let query = crate::query::infra::WsSend::Query {
            req_id: 2,
            sql: "select 1".into(),
        };
        source
            .send(Message::text(serde_json::to_string(&query).unwrap()))
            .await
            .unwrap();

        let query = crate::query::infra::WsSend::Fetch(WsResArgs { req_id: 1, id: 1 });
        source
            .send(Message::text(serde_json::to_string(&query).unwrap()))
            .await
            .unwrap();
        let query = crate::query::infra::WsSend::Fetch(WsResArgs { req_id: 1, id: 2 });
        source
            .send(Message::text(serde_json::to_string(&query).unwrap()))
            .await
            .unwrap();

        let (sender, receiver) = tokio::sync::mpsc::channel(1024);

        let send_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            let mut receiver = receiver;
            loop {
                let msg = tokio::select! {
                    _ = interval.tick() => {
                        Message::ping("data")
                    }
                    res = receiver.recv() => {
                        if let Some(res) = res {
                            res
                        } else {
                            break;
                        }
                    }
                };
                let a = source.send(msg).await;
                dbg!(a);
            }
            println!("finished");
        });
        let handle = tokio::spawn(async move {
            while let Some(Ok(msg)) = sink.next().await {
                if let Some(text) = msg.as_text() {
                    let resp: crate::query::infra::WsRecv = serde_json::from_str(text).unwrap();
                    dbg!(resp.data);
                    // assert_eq!(text, "Hello world!");
                    // We got one message, just stop now
                    // source.close().await.unwrap();
                }
            }
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        sender.send(Message::close(None, "Closed by rust connector"));

        // let abort_handle = handle.abort_handle();
        // abort_handle.abort();
        // tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
