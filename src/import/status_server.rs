// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use tikv::server::status_server::StatusServer as TiKVStatusServer;

pub struct StatusServer {
    inner_server: TiKVStatusServer,
    addr: String,
}

impl StatusServer {
    pub fn new(addr: &str) -> StatusServer {
        // FIXME: Shouldn't need to construct tikv::config::TiKvConfig to use status server.
        let tikv_config = tikv::config::TiKvConfig::default();

        StatusServer {
            inner_server: TiKVStatusServer::new(1, tikv_config),
            addr: addr.to_owned(),
        }
    }

    pub fn start(&mut self) {
        if let Err(e) = self.inner_server.start(self.addr.clone()) {
            warn!("fail to setup status server: {:?}", e)
        }
    }

    pub fn shutdown(self) {
        self.inner_server.stop()
    }
}
