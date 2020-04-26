// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp;
use std::fmt;
use std::i32;
use std::io;
use std::ops::Deref;
use std::path::{Path, PathBuf, MAIN_SEPARATOR};
use std::sync::Arc;

use uuid::Uuid;

use kvproto::import_kvpb::mutation::Op as MutationOp;
use kvproto::import_kvpb::*;
use kvproto::import_sstpb::*;

use engine::rocks::util::{new_engine_opt, CFOptions};
use engine::rocks::{
    BlockBasedOptions, Cache, ColumnFamilyOptions, DBIterator, DBOptions, Env,
    LRUCacheOptions, ReadOptions, Writable, DB,
};
use engine_traits::{CF_DEFAULT, CF_WRITE, IndexHandle};
use engine_rocksdb::{SstFileWriter, WriteBatch as RawBatch, SequentialFile, EnvOptions, ExternalSstFileInfo};
use tikv::config::DbConfig;
use engine_rocks::{RangeProperties, RangePropertiesCollectorFactory, SizeProperties, UserCollectedPropertiesDecoder};
use tikv::storage::mvcc::{Write, WriteType};
use tikv_util::config::MB;
use txn_types::{is_short_value, Key, TimeStamp};

use super::common::*;
use super::Result;
use crate::import::stream::SSTFile;
use tikv_util::security::SecurityManager;

/// Engine wraps rocksdb::DB with customized options to support efficient bulk
/// write.
pub struct Engine {
    db: Arc<DB>,
    uuid: Uuid,
    db_cfg: DbConfig,
    security_mgr: Arc<SecurityManager>,
}

impl Engine {
    pub fn new<P: AsRef<Path>>(
        path: P,
        uuid: Uuid,
        db_cfg: DbConfig,
        security_mgr: Arc<SecurityManager>,
    ) -> Result<Engine> {
        let db = {
            let (db_opts, cf_opts) = tune_dboptions_for_bulk_load(&db_cfg);
            new_engine_opt(path.as_ref().to_str().unwrap(), db_opts, vec![cf_opts])?
        };
        Ok(Engine {
            db: Arc::new(db),
            uuid,
            db_cfg,
            security_mgr,
        })
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub fn write(&self, batch: WriteBatch) -> Result<usize> {
        // Just a guess.
        let wb_cap = cmp::min(batch.get_mutations().len() * 128, MB as usize);
        let wb = RawBatch::with_capacity(wb_cap);
        let commit_ts = TimeStamp::new(batch.get_commit_ts());
        for m in batch.get_mutations().iter() {
            match m.get_op() {
                MutationOp::Put => {
                    let k = Key::from_raw(m.get_key()).append_ts(commit_ts);
                    wb.put(k.as_encoded(), m.get_value()).unwrap();
                }
            }
        }

        let size = wb.data_size();
        self.write_without_wal(&wb)?;

        Ok(size)
    }

    pub fn write_v3(&self, commit_ts: u64, pairs: &[KvPair]) -> Result<usize> {
        // Just a guess.
        let wb_cap = cmp::min(pairs.len() * 128, MB as usize);
        let wb = RawBatch::with_capacity(wb_cap);
        let commit_ts = TimeStamp::new(commit_ts);
        for p in pairs {
            let k = Key::from_raw(p.get_key()).append_ts(commit_ts);
            wb.put(k.as_encoded(), p.get_value()).unwrap();
        }

        let size = wb.data_size();
        self.write_without_wal(&wb)?;

        Ok(size)
    }

    pub fn new_iter(&self, verify_checksum: bool) -> DBIterator<Arc<DB>> {
        let mut ropts = ReadOptions::new();
        ropts.fill_cache(false);
        ropts.set_verify_checksums(verify_checksum);
        DBIterator::new(Arc::clone(&self.db), ropts)
    }

    pub fn new_sst_writer(&self) -> Result<SSTWriter> {
        SSTWriter::new(&self.db_cfg, &self.security_mgr, self.db.path())
    }

    pub fn get_size_properties(&self) -> Result<SizeProperties> {
        let mut res = SizeProperties::default();
        let collection = self.get_properties_of_all_tables()?;
        for (_, v) in &*collection {
            let props = RangeProperties::decode(&UserCollectedPropertiesDecoder(v.user_collected_properties()))?;
            let mut prev_size = 0;
            for (key, range) in props.offsets {
                res.index_handles.add(
                    key,
                    IndexHandle {
                        size: range.size - prev_size,
                        offset: range.size,
                    },
                );
                prev_size = range.size;
            }
            res.total_size += prev_size;
        }
        Ok(res)
    }
}

impl Deref for Engine {
    type Target = DB;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

impl fmt::Debug for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Engine")
            .field("uuid", &self.uuid())
            .field("path", &self.path().to_owned())
            .finish()
    }
}

pub struct LazySSTInfo {
    env: Arc<Env>,
    file_path: PathBuf,
    pub(crate) file_size: u64,
    pub(crate) range: Range,
    pub(crate) cf_name: &'static str,
}

impl fmt::Debug for LazySSTInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LazySSTInfo")
            .field("file_path", &self.file_path)
            .field("file_size", &self.file_size)
            .field("range", &self.range)
            .field("cf_name", &self.cf_name)
            .finish()
    }
}

impl LazySSTInfo {
    fn new(env: Arc<Env>, info: ExternalSstFileInfo, cf_name: &'static str) -> Self {
        // This range doesn't contain the data prefix, like the region range.
        let mut range = Range::default();
        range.set_start(keys::origin_key(info.smallest_key()).to_owned());
        range.set_end(keys::origin_key(info.largest_key()).to_owned());

        Self {
            env,
            file_path: info.file_path(),
            file_size: info.file_size(),
            range,
            cf_name,
        }
    }

    pub fn open(&self) -> Result<SequentialFile> {
        Ok(self
            .env
            .new_sequential_file(self.file_path.to_str().unwrap(), EnvOptions::new())?)
    }

    pub(crate) fn into_sst_file(self) -> Result<SSTFile> {
        let mut seq_file = self.open()?;

        // TODO: If we can compute the CRC simultaneously with upload, we don't
        // need to open() and read() the file twice.
        let mut writer = Crc32Writer {
            digest: crc32fast::Hasher::new(),
            length: 0,
        };
        io::copy(&mut seq_file, &mut writer)?;

        let mut meta = SstMeta::default();
        meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());
        meta.set_range(self.range.clone());
        meta.set_crc32(writer.digest.finalize());
        meta.set_length(writer.length);
        meta.set_cf_name(self.cf_name.to_owned());

        Ok(SSTFile { meta, info: self })
    }
}

impl Drop for LazySSTInfo {
    fn drop(&mut self) {
        match self.env.delete_file(self.file_path.to_str().unwrap()) {
            Ok(()) => {
                info!("cleanup SST completed"; "file_path" => ?self.file_path);
            }
            Err(err) => {
                warn!("cleanup SST failed"; "file_path" => ?self.file_path, "err" => %err);
            }
        }
    }
}

struct Crc32Writer {
    digest: crc32fast::Hasher,
    length: u64,
}

impl io::Write for Crc32Writer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.digest.update(buf);
        self.length += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub struct SSTWriter {
    env: Arc<Env>,
    // we need to preserve base env for reading raw file while env is an encrypted env
    base_env: Option<Arc<Env>>,
    default: SstFileWriter,
    default_entries: u64,
    write: SstFileWriter,
    write_entries: u64,
}

impl SSTWriter {
    pub fn new(db_cfg: &DbConfig, _security_mgr: &SecurityManager, path: &str) -> Result<SSTWriter> {
        let env = Arc::new(Env::new_mem());
        let base_env = None;
        let uuid = Uuid::new_v4().to_string();
        // Placeholder. SstFileWriter don't actually use block cache.
        let cache = None;

        // Creates a writer for default CF
        // Here is where we set table_properties_collector_factory, so that we can collect
        // some properties about SST
        let mut default_opts = db_cfg.defaultcf.build_opt(&cache);
        default_opts.set_env(Arc::clone(&env));
        default_opts.compression_per_level(&db_cfg.defaultcf.compression_per_level);
        let mut default = SstFileWriter::new(EnvOptions::new(), default_opts);
        default.open(&format!("{}{}.{}:default", path, MAIN_SEPARATOR, uuid))?;

        // Creates a writer for write CF
        let mut write_opts = db_cfg.writecf.build_opt(&cache);
        write_opts.set_env(Arc::clone(&env));
        write_opts.compression_per_level(&db_cfg.writecf.compression_per_level);
        let mut write = SstFileWriter::new(EnvOptions::new(), write_opts);
        write.open(&format!("{}{}.{}:write", path, MAIN_SEPARATOR, uuid))?;

        Ok(SSTWriter {
            env,
            base_env,
            default,
            default_entries: 0,
            write,
            write_entries: 0,
        })
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let k = keys::data_key(key);
        let (_, commit_ts) = Key::split_on_ts_for(key)?;
        if is_short_value(value) {
            let w = Write::new(WriteType::Put, commit_ts, Some(value.to_vec()));
            self.write.put(&k, &w.as_ref().to_bytes())?;
            self.write_entries += 1;
        } else {
            let w = Write::new(WriteType::Put, commit_ts, None);
            self.write.put(&k, &w.as_ref().to_bytes())?;
            self.write_entries += 1;
            self.default.put(&k, value)?;
            self.default_entries += 1;
        }
        Ok(())
    }

    pub fn finish(&mut self) -> Result<Vec<LazySSTInfo>> {
        let mut infos = Vec::with_capacity(2);
        if self.default_entries > 0 {
            let info = self.default.finish()?;
            infos.push(LazySSTInfo::new(
                Arc::clone(self.base_env.as_ref().unwrap_or_else(|| &self.env)),
                info,
                CF_DEFAULT,
            ));
        }
        if self.write_entries > 0 {
            let info = self.write.finish()?;
            infos.push(LazySSTInfo::new(
                Arc::clone(self.base_env.as_ref().unwrap_or_else(|| &self.env)),
                info,
                CF_WRITE,
            ));
        }
        Ok(infos)
    }
}

/// Gets a set of approximately equal size ranges from `props`.
/// The maximum number of ranges cannot exceed `max_ranges`,
/// and the minimum number of ranges cannot be smaller than `min_range_size`
pub fn get_approximate_ranges(
    props: &SizeProperties,
    max_ranges: usize,
    min_range_size: usize,
) -> Vec<RangeInfo> {
    let range_size = (props.total_size as usize + max_ranges - 1) / max_ranges;
    let range_size = cmp::max(range_size, min_range_size);

    let mut size = 0;
    let mut start = RANGE_MIN;
    let mut ranges = Vec::new();
    for (i, (k, v)) in props.index_handles.iter().enumerate() {
        size += v.size as usize;
        let end = if i == (props.index_handles.len() - 1) {
            // Index range end is inclusive, so we need to use RANGE_MAX as
            // the last range end.
            RANGE_MAX
        } else {
            k
        };
        if size >= range_size || i == (props.index_handles.len() - 1) {
            let range = RangeInfo::new(start, end, size);
            ranges.push(range);
            size = 0;
            start = end;
        }
    }

    ranges
}

fn tune_dboptions_for_bulk_load(opts: &DbConfig) -> (DBOptions, CFOptions<'_>) {
    const DISABLED: i32 = i32::MAX;

    let mut db_opts = DBOptions::new();
    db_opts.create_if_missing(true);
    db_opts.enable_statistics(false);
    // Vector memtable doesn't support concurrent write.
    db_opts.allow_concurrent_memtable_write(false);
    // RocksDB preserves `max_background_jobs/4` for flush.
    db_opts.set_max_background_jobs(opts.max_background_jobs);

    // Put index and filter in block cache to restrict memory usage.
    let mut cache_opts = LRUCacheOptions::new();
    cache_opts.set_capacity(128 * MB as usize);
    let mut block_base_opts = BlockBasedOptions::new();
    block_base_opts.set_block_cache(&Cache::new_lru_cache(cache_opts));
    block_base_opts.set_cache_index_and_filter_blocks(true);
    let mut cf_opts = ColumnFamilyOptions::new();
    cf_opts.set_block_based_table_factory(&block_base_opts);
    cf_opts.compression_per_level(&opts.defaultcf.compression_per_level);
    // Consider using a large write buffer but be careful about OOM.
    cf_opts.set_write_buffer_size(opts.defaultcf.write_buffer_size.0);
    cf_opts.set_target_file_size_base(opts.defaultcf.write_buffer_size.0);
    cf_opts.set_vector_memtable_factory(opts.defaultcf.write_buffer_size.0);
    cf_opts.set_max_write_buffer_number(opts.defaultcf.max_write_buffer_number);
    // Disable compaction and rate limit.
    cf_opts.set_disable_auto_compactions(true);
    cf_opts.set_soft_pending_compaction_bytes_limit(0);
    cf_opts.set_hard_pending_compaction_bytes_limit(0);
    cf_opts.set_level_zero_stop_writes_trigger(DISABLED);
    cf_opts.set_level_zero_slowdown_writes_trigger(DISABLED);
    // Add size properties to get approximate ranges wihout scan.
    let f = Box::new(RangePropertiesCollectorFactory::default());
    cf_opts.add_table_properties_collector_factory("tikv.size-properties-collector", f);
    (db_opts, CFOptions::new(CF_DEFAULT, cf_opts))
}

#[cfg(test)]
mod tests {
    use super::*;

    use engine::rocks::util::new_engine_opt;
    use engine::rocks::IngestExternalFileOptions;
    use kvproto::kvrpcpb::IsolationLevel;
    use kvproto::metapb::{Peer, Region};
    use std::fs::File;
    use std::io;
    use tempdir::TempDir;

    use engine_rocks::RocksEngine;
    use rand::{
        rngs::{OsRng, StdRng},
        RngCore, SeedableRng,
    };
    use raftstore::store::RegionSnapshot;
    use tikv::storage::config::BlockCacheConfig;
    use tikv::storage::mvcc::MvccReader;
    use tikv_util::security::SecurityManager;

    fn new_engine() -> (TempDir, Engine) {
        let dir = TempDir::new("test_import_engine").unwrap();
        let uuid = Uuid::new_v4();
        let db_cfg = DbConfig::default();
        let security_mgr = Arc::default();
        let engine = Engine::new(dir.path(), uuid, db_cfg, security_mgr).unwrap();
        (dir, engine)
    }

    fn new_write_batch(n: u8, ts: u64) -> WriteBatch {
        let mut wb = WriteBatch::default();
        for i in 0..n {
            let mut m = Mutation::default();
            m.set_op(MutationOp::Put);
            m.set_key(vec![i]);
            m.set_value(vec![i]);
            wb.mut_mutations().push(m);
        }
        wb.set_commit_ts(ts);
        wb
    }

    fn new_kv_pairs(n: u8) -> Vec<KvPair> {
        let mut pairs = vec![KvPair::default(); n as usize];
        for i in 0..n {
            let mut p = KvPair::default();
            p.set_key(vec![i]);
            p.set_value(vec![i]);
            pairs[i as usize] = p;
        }
        pairs
    }

    fn new_encoded_key(i: u8, ts: u64) -> Vec<u8> {
        let ts = TimeStamp::new(ts);
        Key::from_raw(&[i]).append_ts(ts).into_encoded()
    }

    #[test]
    fn test_write() {
        let (_dir, engine) = new_engine();

        let n = 10;
        let commit_ts = 10;
        let wb = new_write_batch(n, commit_ts);
        engine.write(wb).unwrap();

        for i in 0..n {
            let key = new_encoded_key(i, commit_ts);
            assert_eq!(engine.get(&key).unwrap().unwrap(), &[i]);
        }
    }

    #[test]
    fn test_write_v3() {
        let (_dir, engine) = new_engine();

        let n = 10;
        let commit_ts = 10;
        let pairs = new_kv_pairs(n);
        engine.write_v3(commit_ts, &pairs).unwrap();

        for i in 0..n {
            let key = new_encoded_key(i, commit_ts);
            assert_eq!(engine.get(&key).unwrap().unwrap(), &[i]);
        }
    }

    #[test]
    fn test_sst_writer() {
        test_sst_writer_with(1, &[CF_WRITE], &SecurityManager::default());
        test_sst_writer_with(1024, &[CF_DEFAULT, CF_WRITE], &SecurityManager::default());
    }

    fn test_sst_writer_with(value_size: usize, cf_names: &[&str], security_mgr: &SecurityManager) {
        let temp_dir = TempDir::new("_test_sst_writer").unwrap();

        let cfg = DbConfig::default();
        let db_opts = cfg.build_opt();
        let cache = BlockCacheConfig::default().build_shared_cache();
        let cfs_opts = cfg.build_cf_opts(&cache);
        let db = new_engine_opt(temp_dir.path().to_str().unwrap(), db_opts, cfs_opts).unwrap();
        let db = Arc::new(db);

        let n = 10;
        let commit_ts = 10;
        let mut w = SSTWriter::new(&cfg, &security_mgr, temp_dir.path().to_str().unwrap()).unwrap();

        // Write some keys.
        let value = vec![1u8; value_size];
        for i in 0..n {
            let key = new_encoded_key(i, commit_ts);
            w.put(&key, &value).unwrap();
        }

        let infos = w.finish().unwrap();
        assert_eq!(infos.len(), cf_names.len());

        for (info, cf_name) in infos.iter().zip(cf_names.iter()) {
            // Check SSTInfo
            let start = new_encoded_key(0, commit_ts);
            let end = new_encoded_key(n - 1, commit_ts);
            assert_eq!(info.range.get_start(), start.as_slice());
            assert_eq!(info.range.get_end(), end.as_slice());
            assert_eq!(info.cf_name, cf_name.to_owned());

            // Write the data to a file and ingest it to the engine.
            let path = Path::new(db.path()).join("test.sst");
            {
                let mut src = info.open().unwrap();
                let mut dest = File::create(&path).unwrap();
                io::copy(&mut src, &mut dest).unwrap();
            }
            let mut opts = IngestExternalFileOptions::new();
            opts.move_files(true);
            let handle = db.cf_handle(cf_name).unwrap();
            db.ingest_external_file_cf(handle, &opts, &[path.to_str().unwrap()])
                .unwrap();
        }

        // Make a fake region snapshot.
        let mut region = Region::default();
        region.set_id(1);
        region.mut_peers().push(Peer::default());
        let snap = RegionSnapshot::<RocksEngine>::from_raw(Arc::clone(&db), region);

        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);
        // Make sure that all kvs are right.
        for i in 0..n {
            let k = Key::from_raw(&[i]);
            let v = reader.get(&k, TimeStamp::new(commit_ts), false).unwrap().unwrap();
            assert_eq!(&v, &value);
        }
        // Make sure that no extra keys are added.
        let (keys, _) = reader.scan_keys(None, (n + 1) as usize).unwrap();
        assert_eq!(keys.len(), n as usize);
        for (i, expected) in keys.iter().enumerate() {
            let k = Key::from_raw(&[i as u8]);
            assert_eq!(k.as_encoded(), expected.as_encoded());
        }
    }

    const SIZE_INDEX_DISTANCE: usize = 4 * 1024 * 1024;

    #[test]
    fn test_approximate_ranges() {
        let (_dir, engine) = new_engine();

        let num_files = 3;
        let num_entries = 3;
        for i in 0..num_files {
            for j in 0..num_entries {
                // (0, 3, 6), (1, 4, 7), (2, 5, 8)
                let k = [i + j * num_files];
                let v = vec![0u8; SIZE_INDEX_DISTANCE - k.len()];
                engine.put(&k, &v).unwrap();
                engine.flush(true).unwrap();
            }
        }

        let props = engine.get_size_properties().unwrap();
        assert_eq!(props.total_size, (SIZE_INDEX_DISTANCE as u64) * 9);

        let ranges = get_approximate_ranges(&props, 1, 0);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].start, RANGE_MIN.to_owned());
        assert_eq!(ranges[0].end, RANGE_MAX.to_owned());

        let ranges = get_approximate_ranges(&props, 3, 0);
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].start, RANGE_MIN.to_owned());
        assert_eq!(ranges[0].end, vec![2]);
        assert_eq!(ranges[1].start, vec![2]);
        assert_eq!(ranges[1].end, vec![5]);
        assert_eq!(ranges[2].start, vec![5]);
        assert_eq!(ranges[2].end, RANGE_MAX.to_owned());

        let ranges = get_approximate_ranges(&props, 4, SIZE_INDEX_DISTANCE * 4);
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].start, RANGE_MIN.to_owned());
        assert_eq!(ranges[0].end, vec![3]);
        assert_eq!(ranges[1].start, vec![3]);
        assert_eq!(ranges[1].end, vec![7]);
        assert_eq!(ranges[2].start, vec![7]);
        assert_eq!(ranges[2].end, RANGE_MAX.to_owned());
    }

    #[test]
    fn test_size_of_huge_engine() {
        let mut seed = <StdRng as SeedableRng>::Seed::default();
        OsRng.fill_bytes(&mut seed);
        eprintln!("test_size_of_huge_engine seed = {:x?}", &seed);

        let mut rng = StdRng::from_seed(seed);

        let (_dir, engine) = new_engine();

        for i in 0..100 {
            let mut wb = WriteBatch::default();
            for j in 0..1000 {
                let key = format!("t{:08}_r{:08}", i, j).into_bytes();
                let mut value = vec![0u8; 78];
                rng.fill_bytes(&mut value);

                let mut m = Mutation::default();
                m.set_op(MutationOp::Put);
                m.set_key(key);
                m.set_value(value);
                wb.mut_mutations().push(m);
            }
            wb.set_commit_ts(i + 1);
            engine.write(wb).unwrap();
        }
        engine.flush(true).unwrap();

        let props = engine.get_size_properties().unwrap();
        assert_eq!(props.total_size, 100 * 1000 * (78 + 19 + 16));

        let mut cur_size = 0;
        for (_, v) in props.index_handles.iter() {
            assert_eq!(cur_size + v.size, v.offset);
            cur_size = v.offset;
        }
        assert_eq!(cur_size, props.total_size);
    }
}
