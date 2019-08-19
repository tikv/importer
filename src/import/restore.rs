// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use engine::rocks::util::{get_cf_handle, new_engine_opt};
use engine::rocks::{self, IngestExternalFileOptions, Writable, DB};
use hex;
use kvproto::backup::*;
use kvproto::import_kvpb::*;
use storage;
use tikv::config::DbConfig;
use tikv::raftstore::store::keys;
use tikv::storage::types::Key;
use engine::{CF_DEFAULT, CF_WRITE, WriteOptions};
use storage::Storage;
use tikv::storage::mvcc::Write;
use uuid::Uuid;
use tikv_util::collections::HashMap;
use tikv_util::codec::number::NumberEncoder;

use super::client::*;
use super::common::*;
use super::engine::*;
use super::import::*;
use super::{Config, Error, Result};

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

        let default_wb = self.rewrite_file_keys(&db, CF_DEFAULT, self.req.get_default())?;
        Ok(default_wb)
    }

    fn rewrite_file_keys(&self, db: &DB, cf: &str, file: &File) -> Result<WriteBatch> {
        let mut wb = WriteBatch::default();
        wb.set_commit_ts(self.req.get_restore_ts());
        let cf_handle = get_cf_handle(db, cf)?;

        let mut table_ids = HashMap::default();
        let mut index_ids = HashMap::default();
        for p in self.req.get_table_ids() {
            let mut id = Vec::default();
            id.encode_i64(p.get_new_id())?;
            table_ids.insert(p.get_old_id(), id);
            info!("table id pair"; "old" => p.get_old_id(), "new" => p.get_new_id());
        }
        for p in self.req.get_index_ids() {
            let mut id = Vec::default();
            id.encode_i64(p.get_new_id())?;
            index_ids.insert(p.get_old_id(), id);
            info!("index id pair"; "old" => p.get_old_id(), "new" => p.get_new_id());
        }

        scan_db_cf(&db, cf, &[], &[], |k, v| {
            // keys in sst file is encoded key with the DATA_PREFIX, should remove the
            // DATA_PREFIX before replacing ids of a key
            let key = replace_ids_in_key(
                keys::origin_key(k),
                &table_ids,
                &index_ids,
            )?;

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
            let db_path = self.temp_dir.join(format!("ingest-{}", self.uuid));
            let db_cfg = DbConfig::default();
            let (db_opts, cf_opts) = tune_dboptions_for_bulk_load(&db_cfg);
            new_engine_opt(db_path.to_str().unwrap(), db_opts, cf_opts)?
        };

        let default_cf_handle = get_cf_handle(&db, CF_DEFAULT)?;
        if self.req.has_default() {
            let default_sst = self.get_sst_file(
                self.req.get_path(),
                self.req.get_default().get_name(),
                self.req.get_default().get_crc32(),
            )?;
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
                info!("short value"; "key" => hex::encode_upper(k));
                db.put_cf(default_cf_handle, k, &w.short_value.unwrap())?;
            }
            Ok(true)
        })?;

        Ok(db)
    }

    fn get_sst_file(&self, url: &str, name: &str, crc32: u32) -> Result<PathBuf> {
        let storage = storage::create_storage(url)?;
        let mut file_reader = storage.read(name)?;
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

#[cfg(test)]
mod tests {
    use crate::import::restore::RewriteKeysJob;
    use uuid::Uuid;
    use tempdir::TempDir;
    use tidb_query::codec::table;
    use tikv_util::codec::number::NumberEncoder;
    use tikv::storage::mvcc::{Write, WriteType};
    use engine::rocks::{SstFileWriter, EnvOptions, ColumnFamilyOptions};
    use tikv::storage::types::Key;
    use tikv::raftstore::store::keys;

    #[test]
    fn test_short_value() {
        let uuid = Uuid::new_v4();
        let temp_dir = TempDir::new("test_kv_importer").unwrap();

        // t0_r0
        let mut encoded_zero = Vec::default();
        encoded_zero.encode_i64(0).unwrap();
        let mut encoded_zero_desc = Vec::default();
        encoded_zero_desc.encode_i64_desc(0).unwrap();
        let mut default_key = Vec::default();
        default_key.extend_from_slice(table::TABLE_PREFIX);
        default_key.extend_from_slice(encoded_zero.as_slice());
        default_key.extend_from_slice(table::RECORD_PREFIX_SEP);
        default_key.extend_from_slice(encoded_zero.as_slice());
        default_key.extend_from_slice(encoded_zero_desc.as_slice());

        // t0_i0
        let mut index_key = Vec::default();
        index_key.extend_from_slice(table::TABLE_PREFIX);
        index_key.extend_from_slice(encoded_zero.as_slice());
        index_key.extend_from_slice(table::INDEX_PREFIX_SEP);
        index_key.extend_from_slice(encoded_zero.as_slice());
        index_key.extend_from_slice(encoded_zero_desc.as_slice());

        let w = Write::new(WriteType::Put, 0, Some(encoded_zero.clone()));

        let mut cf_opts = ColumnFamilyOptions::default();
        cf_opts.set_env(env.clone());
        let mut default_sst_writer = SstFileWriter::new(EnvOptions::new(), cf_opts.clone());
        default_sst_writer
            .put(
                &keys::data_key(&Key::from_raw(&default_key).append_ts(0).into_encoded()),
                b"v0",
            )
            .unwrap();
        let mut write_sst_writer = SstFileWriter::new(EnvOptions::new(), cf_opts);
        write_sst_writer
            .put(
                &keys::data_key(&Key::from_raw(&index_key).append_ts(0).into_encoded()),
                &w.to_bytes(),
            )
            .unwrap();

        RewriteKeysJob::new(uuid, )
    }
}