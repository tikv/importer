// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_rocks::RocksEngine;
use raftstore::store::{transport::CasualRouter, CasualMessage};
use security::SecurityConfig;
use tikv::config::ConfigController;
use tikv::server::status_server::StatusServer as TiKVStatusServer;

#[derive(Clone)]
struct MockRouter;

impl CasualRouter<RocksEngine> for MockRouter {
    fn send(&self, region_id: u64, _: CasualMessage<RocksEngine>) -> raftstore::Result<()> {
        Err(raftstore::Error::RegionNotFound(region_id))
    }
}

pub struct StatusServer {
    inner_server: TiKVStatusServer<RocksEngine, MockRouter>,
    addr: String,
}

impl StatusServer {
    pub fn new(addr: &str, security_cfg: SecurityConfig) -> StatusServer {
        StatusServer {
            inner_server: TiKVStatusServer::new(
                1,
                None,
                ConfigController::default(),
                Arc::new(security_cfg),
                MockRouter,
            )
            .expect("failed to create status server"),
            addr: addr.to_owned(),
        }
    }

    pub fn start(&mut self) {
        if let Err(e) = self
            .inner_server
            .start(self.addr.clone(), self.addr.clone())
        {
            warn!("fail to setup status server: {:?}", e)
        }
    }

    pub fn shutdown(self) {
        self.inner_server.stop()
    }
}
