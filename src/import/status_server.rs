// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use tikv::server::status_server::StatusServer as TiKVStatusServer;
use tikv::config::ConfigController;
use security::SecurityConfig;

pub struct StatusServer {
    inner_server: TiKVStatusServer,
    addr: String,
    security_cfg: SecurityConfig,
}

impl StatusServer {
    pub fn new(addr: &str, security_cfg: SecurityConfig) -> StatusServer {
        StatusServer {
            inner_server: TiKVStatusServer::new(1, None, ConfigController::default()),
            addr: addr.to_owned(),
            security_cfg,
        }
    }

    pub fn start(&mut self) {
        if let Err(e) = self.inner_server.start(self.addr.clone(), &self.security_cfg) {
            warn!("fail to setup status server: {:?}", e)
        }
    }

    pub fn shutdown(self) {
        self.inner_server.stop()
    }
}
