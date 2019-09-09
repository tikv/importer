// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
use std::fs;
use std::path::PathBuf;

use engine::rocks::util::{get_cf_handle, new_engine_opt};
use engine::rocks::{IngestExternalFileOptions, Writable, DB};
use engine::{CF_DEFAULT, CF_WRITE};
use kvproto::import_kvpb::*;
use storage;
use storage::Storage;
use tikv::config::DbConfig;
use tikv::raftstore::store::keys;
use tikv::storage::mvcc::Write;
use tikv_util::codec::number::NumberEncoder;
use tikv_util::collections::HashMap;
use uuid::Uuid;

use super::common::*;
use super::engine::*;
use super::{Error, Result};

pub struct RewriteKeysJob {
    uuid: Uuid,
    req: RestoreFileRequest,
    temp_dir: PathBuf,
}

impl RewriteKeysJob {
    pub fn new(uuid: Uuid, req: RestoreFileRequest, temp_dir: PathBuf) -> RewriteKeysJob {
        RewriteKeysJob {
            uuid,
            req,
            temp_dir,
        }
    }

    pub fn run(&self) -> Result<WriteBatch> {
        let db = self.write_files_to_temp_db()?;

        let default_wb = self.rewrite_keys(&db, CF_DEFAULT)?;
        Ok(default_wb)
    }

    fn rewrite_keys(&self, db: &DB, cf: &str) -> Result<WriteBatch> {
        let mut wb = WriteBatch::default();
        wb.set_commit_ts(self.req.get_restore_ts());

        let mut table_ids = HashMap::default();
        let mut index_ids = HashMap::default();
        for p in self.req.get_table_ids() {
            let mut id = Vec::default();
            id.encode_i64(p.get_new_id())?;
            table_ids.insert(p.get_old_id(), id);
        }
        for p in self.req.get_index_ids() {
            let mut id = Vec::default();
            id.encode_i64(p.get_new_id())?;
            index_ids.insert(p.get_old_id(), id);
        }

        scan_db_cf(&db, cf, &[], &[], |k, v| {
            // keys in sst file is encoded key with the DATA_PREFIX, should remove the
            // DATA_PREFIX before replacing ids of a key
            let key = replace_ids_in_key(keys::origin_key(k), &table_ids, &index_ids)?;

            if key.is_some() {
                let mut m = Mutation::default();
                m.set_op(MutationOp::Put);
                m.set_key(key.clone().unwrap());
                m.set_value(v.to_vec());

                wb.mut_mutations().push(m);
            }
            Ok(true)
        })?;

        Ok(wb)
    }

    fn write_files_to_temp_db(&self) -> Result<DB> {
        let db = {
            let db_path = self.temp_dir.join(format!(
                "ingest-{}-{}",
                self.uuid,
                self.req.get_write().get_name()
            ));
            let db_cfg = DbConfig::default();
            let (db_opts, cf_opts) = tune_dboptions_for_bulk_load(&db_cfg);
            let db = new_engine_opt(db_path.to_str().unwrap(), db_opts, cf_opts)?;
            info!("create temp db"; "path" => ?db_path);
            db
        };

        let default_cf_handle = get_cf_handle(&db, CF_DEFAULT)?;
        if self.req.has_default() {
            let default_sst = self.get_sst_file(
                self.req.get_path(),
                self.req.get_default().get_name(),
                self.req.get_default().get_crc32(),
            )?;
            info!("get default file"; "name" => self.req.get_default().get_name());
            db.ingest_external_file_cf(
                default_cf_handle,
                &IngestExternalFileOptions::new(),
                &[default_sst.to_str().unwrap()],
            )?;
        }

        let write_sst = self.get_sst_file(
            self.req.get_path(),
            self.req.get_write().get_name(),
            self.req.get_write().get_crc32(),
        )?;
        let write_cf_handle = get_cf_handle(&db, CF_WRITE)?;
        info!("get write file"; "name" => self.req.get_write().get_name());
        db.ingest_external_file_cf(
            write_cf_handle,
            &IngestExternalFileOptions::new(),
            &[write_sst.to_str().unwrap()],
        )?;

        // write short values to default cf
        scan_db_cf(&db, CF_WRITE, &[], &[], |k, v| {
            let w = Write::parse(v)
                .map_err(|_| Error::RestoreFileFailed("parse write cf error".to_string()))?;
            if w.short_value.is_some() {
                db.put_cf(default_cf_handle, k, &w.short_value.unwrap())?;
            }
            Ok(true)
        })?;

        Ok(db)
    }

    fn get_sst_file(&self, url: &str, name: &str, crc32: u32) -> Result<PathBuf> {
        let storage = storage::create_storage(url)?;
        let mut file_reader = storage.read(name)?;
        info!("read file from external storage"; "name" => name);
        let (_, file_crc32) = compute_reader_crc32(&mut file_reader)?;
        if crc32 != file_crc32 {
            return Err(Error::InvalidChunk);
        }

        file_reader = storage.read(name)?;
        let path = self
            .temp_dir
            .join(format!("ingest-{}-sst-{}", self.uuid, name));
        if path.exists() {
            return Err(Error::FileExists(path));
        }
        let mut data = Vec::default();
        file_reader.read_to_end(&mut data)?;
        fs::write(&path, &data)?;
        Ok(path)
    }
}
