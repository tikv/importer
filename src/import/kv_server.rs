// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use grpcio::{ChannelBuilder, EnvBuilder, Server as GrpcServer, ServerBuilder};
use kvproto::import_kvpb::create_import_kv;

use security::SecurityManager;
use tikv_util::thd_name;

use super::{ImportKVService, KVImporter, TiKvConfig};
use crate::import::status_server::StatusServer;

/// ImportKVServer is a gRPC server that provides service to write key-value
/// pairs into RocksDB engines for later ingesting into tikv-server.
pub struct ImportKVServer {
    grpc_server: GrpcServer,
    status_server: Option<StatusServer>,
}

impl ImportKVServer {
    pub fn new(tikv: &TiKvConfig) -> ImportKVServer {
        let cfg = &tikv.server;
        let addr = SocketAddr::from_str(&cfg.addr).unwrap();

        let security_mgr = Arc::new(SecurityManager::new(&tikv.security).unwrap());

        let importer = KVImporter::new(
            tikv.import.clone(),
            tikv.rocksdb.clone(),
            security_mgr.clone(),
        )
        .unwrap();
        let import_service = ImportKVService::new(tikv.import.clone(), Arc::new(importer));

        let env = Arc::new(
            EnvBuilder::new()
                .name_prefix(thd_name!("import-server"))
                .cq_count(cfg.grpc_concurrency)
                .build(),
        );

        let channel_args = ChannelBuilder::new(Arc::clone(&env))
            .stream_initial_window_size(cfg.grpc_stream_initial_window_size.0 as i32)
            .max_concurrent_stream(cfg.grpc_concurrent_stream)
            .max_send_message_len(-1)
            .max_receive_message_len(-1)
            .build_args();

        let grpc_server = security_mgr
            .bind(
                ServerBuilder::new(env.clone()),
                &addr.ip().to_string(),
                addr.port(),
            )
            .channel_args(channel_args)
            .register_service(create_import_kv(import_service))
            .build()
            .unwrap();

        let status_server = tikv
            .status_server_address
            .as_ref()
            .map(|address| StatusServer::new(address, tikv.security.clone()));
        ImportKVServer {
            grpc_server,
            status_server,
        }
    }

    pub fn start(&mut self) {
        self.grpc_server.start();
        if let Some(server) = &mut self.status_server {
            server.start();
        }
    }

    pub fn shutdown(&mut self) {
        self.grpc_server.shutdown();
        if let Some(server) = self.status_server.take() {
            server.shutdown();
        }
    }

    pub fn bind_addrs(&self) -> impl Iterator<Item = (&String, u16)> + '_ {
        self.grpc_server.bind_addrs()
    }
}
