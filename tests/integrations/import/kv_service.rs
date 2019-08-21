// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;
use std::time::Duration;

use clap::crate_version;
use futures::{stream, Future, Stream};
use tempdir::TempDir;
use uuid::Uuid;

use grpcio::{ChannelBuilder, Environment, Result, WriteFlags};
use kvproto::import_kvpb::*;
use kvproto::import_kvpb_grpc::*;

use test_util::retry;
use tikv_importer::import::{ImportKVServer, TiKvConfig};

fn new_kv_server() -> (ImportKVServer, ImportKvClient, TempDir) {
    let temp_dir = TempDir::new("test_import_kv_server").unwrap();

    let mut cfg = TiKvConfig::default();
    cfg.server.addr = "127.0.0.1:0".to_owned();
    cfg.import.import_dir = temp_dir.path().to_str().unwrap().to_owned();
    let server = ImportKVServer::new(&cfg);

    let ch = {
        let env = Arc::new(Environment::new(1));
        let addr = server.bind_addrs().first().unwrap();
        ChannelBuilder::new(env)
            .keepalive_timeout(Duration::from_secs(60))
            .connect(&format!("{}:{}", addr.0, addr.1))
    };
    let client = ImportKvClient::new(ch);

    // Return temp_dir as well, so that temp dir will be properly
    // deleted when it is dropped.
    (server, client, temp_dir)
}

#[test]
fn test_kv_service() {
    let (mut server, client, _) = new_kv_server();
    server.start();

    let resp = retry!(client.get_version(&GetVersionRequest::new())).unwrap();
    assert_eq!(resp.get_version(), crate_version!());
    assert_eq!(resp.get_commit().len(), 40);

    let resp = retry!(client.get_metrics(&GetMetricsRequest::new())).unwrap();
    // It's true since we just send a get_version rpc
    assert!(resp.get_prometheus().contains("request=\"get_version\",result=\"ok\""));

    let uuid = Uuid::new_v4().as_bytes().to_vec();
    let mut head = WriteHead::new();
    head.set_uuid(uuid.clone());

    let resp = retry!(client.get_metrics(&GetMetricsRequest::new())).unwrap();
    // It's true since we just send a get_version rpc
    assert!(resp.get_prometheus().contains("request=\"get_version\",result=\"ok\""));

    let uuid = Uuid::new_v4().as_bytes().to_vec();   
    let mut open = OpenEngineRequest::new();
    open.set_uuid(uuid.clone());

    let mut close = CloseEngineRequest::new();
    close.set_uuid(uuid.clone());

    let mut write = WriteEngineV3Request::new();
    
    write.set_uuid(uuid);
    write.set_commit_ts(123);
    let mut p = KVPair::new();
    p.set_key(vec![123]);
    p.set_value(vec![123]);
    write.take_pairs().push(p);

    // Write an engine before it is opened.
    // Only send the write head here to avoid other gRPC errors.
    let resp = retry!(client.write_engine_v3(&write)).unwrap();
    assert!(resp.get_error().has_engine_not_found());

    // Close an engine before it it opened.
    let resp = retry!(client.close_engine(&close)).unwrap();
    assert!(resp.get_error().has_engine_not_found());

    retry!(client.open_engine(&open)).unwrap();
    let resp = retry!(client.write_engine_v3(&write)).unwrap();
    assert!(!resp.has_error());
    let resp = retry!(client.write_engine_v3(&write)).unwrap();
    assert!(!resp.has_error());
    let resp = retry!(client.close_engine(&close)).unwrap();
    assert!(!resp.has_error());

    server.shutdown();
}
