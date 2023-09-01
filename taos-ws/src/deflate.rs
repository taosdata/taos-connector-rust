use std::time::Duration;

use crate::query::infra::WsResArgs;
use crate::query::WsConnReq;

use tracing::*;
use tracing_subscriber::util::SubscriberInitExt;
use ws_tool::{
    codec::{AsyncDeflateCodec, PMDConfig, WindowBit},
    errors::WsError,
    frame::{OpCode, OwnedFrame},
    ClientBuilder,
};

#[tokio::test]
async fn test_ws() -> anyhow::Result<()> {
    let _subscriber = tracing_subscriber::fmt::fmt()
        .with_max_level(Level::INFO)
        .with_file(true)
        .with_line_number(true)
        .finish();
    // _subscriber.try_init().expect("failed to init log");

    let uri = "ws://127.0.0.1:6041/rest/ws".try_into().unwrap();
    let config = PMDConfig::default();

    let mut client = ClientBuilder::new()
        .extension(config.ext_string())
        .async_connect(uri, AsyncDeflateCodec::check_fn)
        .await?;
    let (mut sink, mut source) = client.split();
    // crate::query::infra:
    let version = crate::query::infra::WsSend::Version;

    source
        .send(OpCode::Text, &serde_json::to_vec(&version)?)
        .await
        .unwrap();

    source.flush().await?;

    let conn = crate::query::infra::WsSend::Conn {
        req_id: 0,
        req: WsConnReq::new("root", "taosdata"),
    };

    source
        .send(OpCode::Text, &serde_json::to_vec(&conn).unwrap())
        .await
        .unwrap();
    source.flush().await?;

    let query = crate::query::infra::WsSend::Query {
        req_id: 1,
        sql: "show databases".into(),
    };
    source
        .send(OpCode::Text, &serde_json::to_vec(&query).unwrap())
        .await
        .unwrap();
    source.flush().await?;
    let query = crate::query::infra::WsSend::Query {
        req_id: 2,
        sql: "select 1".into(),
    };
    source
        .send(OpCode::Text, &serde_json::to_vec(&query).unwrap())
        .await
        .unwrap();
    source.flush().await?;
    let query = crate::query::infra::WsSend::Fetch(WsResArgs { req_id: 1, id: 1 });
    source
        .send(OpCode::Text, &serde_json::to_vec(&query).unwrap())
        .await
        .unwrap();
    source.flush().await?;
    let query = crate::query::infra::WsSend::Fetch(WsResArgs { req_id: 1, id: 2 });
    source
        .send(OpCode::Text, &serde_json::to_vec(&query).unwrap())
        .await
        .unwrap();
    source.flush().await?;

    let (sender, receiver) = tokio::sync::mpsc::channel(1024);

    let _ = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        let mut receiver = receiver;
        loop {
            let msg = tokio::select! {
                _ = interval.tick() => {
                    OpCode::Ping
                }
                res = receiver.recv() => {
                    if let Some(res) = res {
                        res
                    } else {
                        break;
                    }
                }
            };
            let a = source.send(dbg!(msg), b"12345").await;
            dbg!(a);
        }
        println!("finished");
    });
    let handle = tokio::spawn(async move {
        loop {
            let frame = sink.receive().await.unwrap();
            let (header, payload) = frame.parts();
            dbg!(&header.opcode(), &payload);
            let code = header.opcode();

            match code {
                OpCode::Binary => {
                    println!("{:?}", payload);
                }
                OpCode::Text => {
                    let resp: crate::query::infra::WsRecv =
                        serde_json::from_slice(&payload).unwrap();
                    dbg!(resp.data);
                }
                _ => (),
            }
        }
        // let sink = sink.stream_mut();
        // while let Some(Ok(msg)) = sinknext().await {
        //     if let Some(text) = msg.as_text() {
        //         let resp: crate::query::infra::WsRecv = serde_json::from_str(text).unwrap();
        //         dbg!(resp.data);
        //         // assert_eq!(text, "Hello world!");
        //         // We got one message, just stop now
        //         // source.close().await.unwrap();
        //     }
        // }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    sender.send(OpCode::Close);

    // let abort_handle = handle.abort_handle();
    // abort_handle.abort();
    // tokio::time::sleep(Duration::from_millis(200)).await;
    Ok(())
}
