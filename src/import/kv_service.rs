// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use futures::executor::{ThreadPool, ThreadPoolBuilder};
use futures::future::FutureExt;
use futures::stream::StreamExt;
use futures::task::SpawnExt;
use grpcio::{ClientStreamingSink, RequestStream, RpcContext, UnarySink};
use kvproto::import_kvpb::*;
use uuid::Uuid;

use tikv_util::time::Instant;
use txn_types::Key;

use super::client::*;
use super::metrics::{self, *};
use super::service::*;
use super::{Config, Error, KVImporter};
use crate::send_rpc_response;

#[derive(Clone)]
pub struct ImportKVService {
    cfg: Config,
    threads: ThreadPool,
    importer: Arc<KVImporter>,
}

impl ImportKVService {
    pub fn new(cfg: Config, importer: Arc<KVImporter>) -> ImportKVService {
        let threads = ThreadPoolBuilder::new()
            .name_prefix("kv-importer")
            .pool_size(cfg.num_threads)
            .create()
            .unwrap();
        ImportKVService {
            cfg,
            threads,
            importer,
        }
    }
}

macro_rules! try_engine {
    (<$resp_ty:ty> $e:expr) => {
        match $e {
            Err(Error::EngineNotFound(v)) => {
                let mut resp = <$resp_ty>::default();
                resp.mut_error()
                    .mut_engine_not_found()
                    .set_uuid(v.as_bytes().to_vec());
                return Ok(resp);
            }
            e => e?,
        }
    };
}

/// ImportKV provides a service to import key-value pairs to TiKV.
///
/// In order to import key-value pairs to TiKV, the user should:
/// 1. Opens an engine identified by a UUID.
/// 2. Opens a write streams to write key-value batches to the opened engine.
///    Different streams/clients can write to the same engine concurrently.
/// 3. Closes the engine after all write batches have been finished. An
///    engine can only be closed when all write streams are closed. An
///    engine can only be closed once, and it can not be opened again
///    once it is closed.
/// 4. Imports the data in the engine to the target cluster. Note that
///    the import process is not atomic, and it requires the data to be
///    idempotent on retry. An engine can only be imported after it is
///    closed. An engine can be imported multiple times, but can not be
///    imported concurrently.
/// 5. Cleans up the engine after it has been imported. Delete all data
///    in the engine. An engine can not be cleaned up when it is
///    writing or importing.
impl ImportKv for ImportKVService {
    /// Switches the target cluster to normal/import mode.
    ///
    /// Under import mode, cluster will stop automatic compaction and
    /// turn off write stall mechanism.
    fn switch_mode(
        &mut self,
        ctx: RpcContext<'_>,
        req: SwitchModeRequest,
        sink: UnarySink<SwitchModeResponse>,
    ) {
        let label = "switch_mode";
        let timer = Instant::now_coarse();
        let min_available_ratio = self.cfg.min_available_ratio;
        let security_mgr = self.importer.security_mgr.clone();

        ctx.spawn(
            self.threads
                .spawn_with_handle(async move {
                    let client = Client::new(req.get_pd_addr(), 1, min_available_ratio, security_mgr)?;
                    match client.switch_cluster(req.get_request()).await {
                        Ok(_) => {
                            info!("switch cluster"; "req" => ?req.get_request());
                            Ok(SwitchModeResponse::default())
                        }
                        Err(e) => {
                            error!("switch cluster failed"; "req" => ?req.get_request(), "err" => %e);
                            Err(e)
                        }
                    }
                }
                .then(move |res| send_rpc_response!(res, sink, label, timer))
            ).unwrap(),
        )
    }

    fn open_engine(
        &mut self,
        ctx: RpcContext<'_>,
        req: OpenEngineRequest,
        sink: UnarySink<OpenEngineResponse>,
    ) {
        let label = "open_engine";
        let timer = Instant::now_coarse();
        let import = Arc::clone(&self.importer);

        ctx.spawn(
            self.threads
                .spawn_with_handle(
                    async move {
                        let uuid = Uuid::from_slice(req.get_uuid())?;
                        import
                            .open_engine(uuid)
                            .map(|_| OpenEngineResponse::default())
                    }
                    .then(move |res| send_rpc_response!(res, sink, label, timer)),
                )
                .unwrap(),
        )
    }

    fn write_engine(
        &mut self,
        ctx: RpcContext<'_>,
        mut stream: RequestStream<WriteEngineRequest>,
        sink: ClientStreamingSink<WriteEngineResponse>,
    ) {
        let label = "write_engine";
        let timer = Instant::now_coarse();
        let import = Arc::clone(&self.importer);

        ctx.spawn(
            self.threads
                .spawn_with_handle(
                    async move {
                        let uuid = match stream.next().await {
                            Some(Ok(ref chunk)) if chunk.has_head() => {
                                Uuid::from_slice(chunk.get_head().get_uuid())?
                            }
                            Some(Err(e)) => return Err(e.into()),
                            _ => return Err(Error::InvalidChunk),
                        };
                        let engine = try_engine!(<WriteEngineResponse> import.bind_engine(uuid));

                        while let Some(chunk) = stream.next().await {
                            let mut chunk = chunk?;
                            if !chunk.has_batch() {
                                return Err(Error::InvalidChunk);
                            }
                            let start = Instant::now_coarse();
                            let batch = chunk.take_batch();
                            let batch_size = engine.write(batch)?;
                            IMPORT_WRITE_CHUNK_BYTES.observe(batch_size as f64);
                            IMPORT_WRITE_CHUNK_DURATION.observe(start.elapsed_secs());
                        }

                        Ok(WriteEngineResponse::default())
                    }
                    .then(move |res| send_rpc_response!(res, sink, label, timer)),
                )
                .unwrap(),
        )
    }

    fn write_engine_v3(
        &mut self,
        ctx: RpcContext<'_>,
        req: WriteEngineV3Request,
        sink: UnarySink<WriteEngineResponse>,
    ) {
        let label = "write_engine";
        let timer = Instant::now_coarse();
        let import = Arc::clone(&self.importer);

        ctx.spawn(
            self.threads
                .spawn_with_handle(
                    async move {
                        let uuid = Uuid::from_slice(req.get_uuid())?;
                        let engine = try_engine!(<WriteEngineResponse> import.bind_engine(uuid));

                        let ts = req.get_commit_ts();
                        let start = Instant::now_coarse();
                        let write_size = engine.write_v3(ts, req.get_pairs())?;
                        IMPORT_WRITE_CHUNK_BYTES.observe(write_size as f64);
                        IMPORT_WRITE_CHUNK_DURATION.observe(start.elapsed_secs());
                        Ok(WriteEngineResponse::default())
                    }
                    .then(move |res| send_rpc_response!(res, sink, label, timer)),
                )
                .unwrap(),
        )
    }

    fn close_engine(
        &mut self,
        ctx: RpcContext<'_>,
        req: CloseEngineRequest,
        sink: UnarySink<CloseEngineResponse>,
    ) {
        let label = "close_engine";
        let timer = Instant::now_coarse();
        let import = Arc::clone(&self.importer);

        ctx.spawn(
            self.threads
                .spawn_with_handle(
                    async move {
                        let uuid = Uuid::from_slice(req.get_uuid())?;
                        try_engine!(<CloseEngineResponse> import.close_engine(uuid));
                        Ok(CloseEngineResponse::default())
                    }
                    .then(move |res| send_rpc_response!(res, sink, label, timer)),
                )
                .unwrap(),
        )
    }

    fn import_engine(
        &mut self,
        ctx: RpcContext<'_>,
        req: ImportEngineRequest,
        sink: UnarySink<ImportEngineResponse>,
    ) {
        let label = "import_engine";
        let timer = Instant::now_coarse();
        let import = Arc::clone(&self.importer);

        ctx.spawn(
            self.threads
                .spawn_with_handle(
                    async move {
                        let uuid = Uuid::from_slice(req.get_uuid())?;
                        import.import_engine(uuid, req.get_pd_addr()).await?;
                        Ok(ImportEngineResponse::default())
                    }
                    .then(move |res| send_rpc_response!(res, sink, label, timer)),
                )
                .unwrap(),
        )
    }

    fn cleanup_engine(
        &mut self,
        ctx: RpcContext<'_>,
        req: CleanupEngineRequest,
        sink: UnarySink<CleanupEngineResponse>,
    ) {
        let label = "cleanup_engine";
        let timer = Instant::now_coarse();
        let import = Arc::clone(&self.importer);

        ctx.spawn(
            self.threads
                .spawn_with_handle(
                    async move {
                        let uuid = Uuid::from_slice(req.get_uuid())?;
                        import.cleanup_engine(uuid)?;
                        Ok(CleanupEngineResponse::default())
                    }
                    .then(move |res| send_rpc_response!(res, sink, label, timer)),
                )
                .unwrap(),
        )
    }

    /// It's recommended to call `compact_cluster` before reading from
    /// the database, because otherwise the read can be very slow.
    fn compact_cluster(
        &mut self,
        ctx: RpcContext<'_>,
        req: CompactClusterRequest,
        sink: UnarySink<CompactClusterResponse>,
    ) {
        let label = "compact_cluster";
        let timer = Instant::now_coarse();
        let min_available_ratio = self.cfg.min_available_ratio;
        let security_mgr = self.importer.security_mgr.clone();

        let mut compact = req.get_request().clone();
        if compact.has_range() {
            // Convert the range to a TiKV encoded data range.
            let start = Key::from_raw(compact.get_range().get_start());
            compact
                .mut_range()
                .set_start(keys::data_key(start.as_encoded()));
            let end = Key::from_raw(compact.get_range().get_end());
            compact
                .mut_range()
                .set_end(keys::data_end_key(end.as_encoded()));
        }

        ctx.spawn(
            self.threads
                .spawn_with_handle(
                    async move {
                        let client =
                            Client::new(req.get_pd_addr(), 1, min_available_ratio, security_mgr)?;
                        match client.compact_cluster(&compact).await {
                            Ok(_) => {
                                info!("compact cluster"; "req" => ?compact);
                                Ok(CompactClusterResponse::default())
                            }
                            Err(e) => {
                                error!("compact cluster failed"; "req" => ?compact, "err" => %e);
                                Err(e)
                            }
                        }
                    }
                    .then(move |res| send_rpc_response!(res, sink, label, timer)),
                )
                .unwrap(),
        )
    }

    fn get_version(
        &mut self,
        ctx: RpcContext<'_>,
        _req: GetVersionRequest,
        sink: UnarySink<GetVersionResponse>,
    ) {
        let label = "get_version";
        let timer = Instant::now_coarse();

        ctx.spawn(
            self.threads
                .spawn_with_handle(
                    async move {
                        let v = env!("CARGO_PKG_VERSION");
                        let c = env!("TIKV_BUILD_GIT_HASH");
                        let mut res = GetVersionResponse::default();
                        res.set_version(v.to_owned());
                        res.set_commit(c.to_owned());
                        Ok(res)
                    }
                    .then(move |res| send_rpc_response!(res, sink, label, timer)),
                )
                .unwrap(),
        )
    }

    fn get_metrics(
        &mut self,
        ctx: RpcContext<'_>,
        _req: GetMetricsRequest,
        sink: UnarySink<GetMetricsResponse>,
    ) {
        let label = "get_metrics";
        let timer = Instant::now_coarse();

        ctx.spawn(
            self.threads
                .spawn_with_handle(
                    async move {
                        let mut res = GetMetricsResponse::default();
                        res.set_prometheus(metrics::dump());
                        Ok(res)
                    }
                    .then(move |res| send_rpc_response!(res, sink, label, timer)),
                )
                .unwrap(),
        )
    }
}
