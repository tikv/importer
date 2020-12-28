// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::{self, BoxFuture, FutureExt};
use futures::stream::{self, StreamExt};
use futures::SinkExt;
use grpcio::{CallOption, Channel, ChannelBuilder, EnvBuilder, Environment, WriteFlags};

use engine_rocksdb::SequentialFile;
use kvproto::import_sstpb::*;
use kvproto::kvrpcpb::*;
use kvproto::pdpb::OperatorStatus;
use kvproto::tikvpb::TikvClient;

use collections::{HashMap, HashMapEntry};
use pd_client::{Config as PdConfig, Error as PdError, PdClient, RegionInfo, RpcClient};
use security::SecurityManager;
use txn_types::Key;

use super::common::*;
use super::{Error, Result};

pub trait ImportClient: Send + Sync + Clone + 'static {
    fn get_region(&self, _: &[u8]) -> Result<RegionInfo> {
        unimplemented!()
    }

    fn split_region(&self, _: &RegionInfo, _: &[u8]) -> Result<SplitRegionResponse> {
        unimplemented!()
    }

    fn scatter_region(&self, _: &RegionInfo) -> Result<()> {
        unimplemented!()
    }

    fn upload_sst(&self, _: u64, _: UploadStream) -> BoxFuture<'_, Result<UploadResponse>> {
        unimplemented!()
    }

    fn ingest_sst(&self, _: u64, _: IngestRequest) -> Result<IngestResponse> {
        unimplemented!()
    }

    fn has_region_id(&self, _: u64) -> BoxFuture<'_, Result<bool>> {
        unimplemented!()
    }

    fn is_scatter_region_finished(&self, _: u64) -> Result<bool> {
        unimplemented!()
    }

    fn is_space_enough(&self, _: u64, _: u64) -> Result<bool> {
        unimplemented!()
    }
}

pub struct Client {
    pd: Arc<RpcClient>,
    env: Arc<Environment>,
    channels: Mutex<HashMap<u64, Channel>>,
    min_available_ratio: f64,
    security_mgr: Arc<SecurityManager>,
}

impl Client {
    pub async fn new(
        pd_addr: &str,
        cq_count: usize,
        min_available_ratio: f64,
        security_mgr: Arc<SecurityManager>,
    ) -> Result<Client> {
        let cfg = PdConfig::new(vec![pd_addr.to_owned()]);
        let env = Arc::new(
            EnvBuilder::new()
                .name_prefix("import-client")
                .cq_count(cq_count)
                .build(),
        );
        let rpc_client =
            RpcClient::new_async(&cfg, Some(env.clone()), security_mgr.clone()).await?;
        Ok(Client {
            pd: Arc::new(rpc_client),
            env,
            channels: Mutex::new(HashMap::default()),
            min_available_ratio,
            security_mgr,
        })
    }

    fn option(&self, timeout: Duration) -> CallOption {
        let write_flags = WriteFlags::default().buffer_hint(true);
        CallOption::default()
            .timeout(timeout)
            .write_flags(write_flags)
    }

    fn resolve(&self, store_id: u64) -> Result<Channel> {
        let mut channels = self.channels.lock().unwrap();
        match channels.entry(store_id) {
            HashMapEntry::Occupied(e) => Ok(e.get().clone()),
            HashMapEntry::Vacant(e) => {
                let store = self.pd.get_store(store_id)?;
                let builder = ChannelBuilder::new(self.env.clone());
                let tar_addr = if !store.get_peer_address().is_empty() {
                    store.get_peer_address()
                } else {
                    store.get_address()
                };
                let channel = self.security_mgr.connect(builder, tar_addr);
                Ok(e.insert(channel).clone())
            }
        }
    }

    fn post_resolve<T>(&self, store_id: u64, res: Result<T>) -> Result<T> {
        res.map_err(|e| {
            self.channels.lock().unwrap().remove(&store_id);
            e
        })
    }

    pub async fn switch_cluster(&self, req: &SwitchModeRequest) -> Result<()> {
        let mut futures = Vec::new();
        // Exclude tombstone stores.
        for store in self.pd.get_all_stores(true)? {
            let ch = match self.resolve(store.get_id()) {
                Ok(v) => v,
                Err(e) => {
                    error!("get store channel failed"; "store" => ?store, "err" => %e);
                    continue;
                }
            };
            let client = ImportSstClient::new(ch);
            let future = match client.switch_mode_async(req) {
                Ok(v) => v,
                Err(e) => {
                    error!("switch mode failed"; "store" => ?store, "err" => %e);
                    continue;
                }
            };
            futures.push(future);
        }

        future::try_join_all(futures).await?;
        Ok(())
    }

    pub async fn compact_cluster(&self, req: &CompactRequest) -> Result<()> {
        let mut futures = Vec::new();
        // Exclude tombstone stores.
        for store in self.pd.get_all_stores(true)? {
            let ch = match self.resolve(store.get_id()) {
                Ok(v) => v,
                Err(e) => {
                    error!("get store channel failed"; "store" => ?store, "err" => %e);
                    continue;
                }
            };
            let client = ImportSstClient::new(ch);
            let future = match client.compact_async(req) {
                Ok(v) => v,
                Err(e) => {
                    error!("compact failed"; "store" => ?store, "err" => %e);
                    continue;
                }
            };
            futures.push(future);
        }

        future::try_join_all(futures).await?;
        Ok(())
    }
}

impl Clone for Client {
    fn clone(&self) -> Client {
        Client {
            pd: Arc::clone(&self.pd),
            env: Arc::clone(&self.env),
            channels: Mutex::new(HashMap::default()),
            min_available_ratio: self.min_available_ratio,
            security_mgr: self.security_mgr.clone(),
        }
    }
}

impl ImportClient for Client {
    fn get_region(&self, key: &[u8]) -> Result<RegionInfo> {
        self.pd.get_region_info(key).map_err(Error::from)
    }

    fn split_region(&self, region: &RegionInfo, split_key: &[u8]) -> Result<SplitRegionResponse> {
        let ctx = new_context(region);
        let store_id = ctx.get_peer().get_store_id();

        let mut req = SplitRegionRequest::default();
        req.set_context(ctx);
        req.set_split_key(Key::from_encoded_slice(split_key).into_raw()?);

        let ch = self.resolve(store_id)?;
        let client = TikvClient::new(ch);
        let res = client.split_region_opt(&req, self.option(Duration::from_secs(3)));
        self.post_resolve(store_id, res.map_err(Error::from))
    }

    fn scatter_region(&self, region: &RegionInfo) -> Result<()> {
        self.pd.scatter_region(region.clone()).map_err(Error::from)
    }

    fn upload_sst(
        &self,
        store_id: u64,
        req: UploadStream,
    ) -> BoxFuture<'_, Result<UploadResponse>> {
        async move {
            let ch = self.resolve(store_id)?;
            let client = ImportSstClient::new(ch);
            let (tx, rx) = client.upload_opt(self.option(Duration::from_secs(30)))?;
            stream::iter(req)
                .forward(tx.sink_map_err(Error::from))
                .await?;
            self.post_resolve(store_id, rx.await.map_err(Error::from))
        }
        .boxed()
    }

    fn ingest_sst(&self, store_id: u64, req: IngestRequest) -> Result<IngestResponse> {
        let ch = self.resolve(store_id)?;
        let client = ImportSstClient::new(ch);
        let res = client.ingest_opt(&req, self.option(Duration::from_secs(30)));
        self.post_resolve(store_id, res.map_err(Error::from))
    }

    fn has_region_id(&self, id: u64) -> BoxFuture<'_, Result<bool>> {
        async move { Ok(self.pd.get_region_by_id(id).await?.is_some()) }.boxed()
    }

    fn is_scatter_region_finished(&self, region_id: u64) -> Result<bool> {
        match self.pd.get_operator(region_id) {
            Ok(resp) => {
                // If the current operator of region is not `scatter-region`, we could assume
                // that `scatter-operator` has finished or timeout.
                Ok(resp.desc != b"scatter-region" || resp.get_status() != OperatorStatus::Running)
            }
            Err(PdError::RegionNotFound(_)) => Ok(true), // heartbeat may not send to PD
            Err(err) => {
                error!("check scatter region operator result"; "region_id" => %region_id, "err" => %err);
                Err(Error::from(err))
            }
        }
    }

    fn is_space_enough(&self, store_id: u64, size: u64) -> Result<bool> {
        let stats = self.pd.get_store_stats(store_id)?;
        let available_ratio = stats.available.saturating_sub(size) as f64 / stats.capacity as f64;
        // Ensure target store have available disk space
        Ok(available_ratio > self.min_available_ratio)
    }
}

pub struct UploadStream<R = SequentialFile> {
    meta: Option<SstMeta>,
    data: R,
}

impl<R> UploadStream<R> {
    pub fn new(meta: SstMeta, data: R) -> Self {
        Self {
            meta: Some(meta),
            data,
        }
    }
}

const UPLOAD_CHUNK_SIZE: usize = 1024 * 1024;

impl<R: Read> Iterator for UploadStream<R> {
    type Item = Result<(UploadRequest, WriteFlags)>;

    fn next(&mut self) -> Option<Self::Item> {
        let flags = WriteFlags::default().buffer_hint(true);

        if let Some(meta) = self.meta.take() {
            let mut chunk = UploadRequest::default();
            chunk.set_meta(meta);
            return Some(Ok((chunk, flags)));
        }

        let mut buf = Vec::with_capacity(UPLOAD_CHUNK_SIZE);
        if let Err(e) = self
            .data
            .by_ref()
            .take(UPLOAD_CHUNK_SIZE as u64)
            .read_to_end(&mut buf)
        {
            return Some(Err(e.into()));
        }
        if buf.is_empty() {
            return None;
        }

        let mut chunk = UploadRequest::default();
        chunk.set_data(buf);
        Some(Ok((chunk, flags)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::RngCore;

    #[test]
    fn test_upload_stream() {
        let mut meta = SstMeta::default();
        meta.set_crc32(123);
        meta.set_length(321);

        let mut data = vec![0u8; UPLOAD_CHUNK_SIZE * 4];
        rand::thread_rng().fill_bytes(&mut data);

        let mut stream = UploadStream::new(meta.clone(), &*data);

        // Check meta.
        if let Some(res) = stream.next() {
            let (upload, _) = res.unwrap();
            assert_eq!(upload.get_meta().get_crc32(), meta.get_crc32());
            assert_eq!(upload.get_meta().get_length(), meta.get_length());
        } else {
            panic!("can not poll upload meta");
        }

        // Check data.
        let mut buf: Vec<u8> = Vec::with_capacity(UPLOAD_CHUNK_SIZE * 4);
        for res in stream {
            let (upload, _) = res.unwrap();
            buf.extend(upload.get_data());
        }
        assert_eq!(buf, data);
    }
}
