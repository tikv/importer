// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs;
use std::io;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;

use kvproto::import_kvpb::*;
use kvproto::import_sstpb::*;
use kvproto::kvrpcpb::*;
use kvproto::metapb::*;

use crc::crc32::{self, Hasher32};
use pd_client::RegionInfo;

use super::client::*;
use super::engine::tune_dboptions_for_bulk_load;
use super::{Error, Result};
use engine::rocks::util::new_engine_opt;
use engine::rocks::{IngestExternalFileOptions, DB};
use tidb_query::codec::table;
use tikv::config::DbConfig;
use tikv::storage::types::Key;
use tikv::raftstore::store::keys;
use tikv_util::codec::number;
use tikv_util::codec::number::NumberEncoder;
use tikv_util::collections::HashMap;
use uuid::Uuid;

// Just used as a mark, don't use them in comparison.
pub const RANGE_MIN: &[u8] = &[];
pub const RANGE_MAX: &[u8] = &[];

pub fn new_range(start: &[u8], end: &[u8]) -> Range {
    let mut range = Range::new();
    range.set_start(start.to_owned());
    range.set_end(end.to_owned());
    range
}

pub fn before_end(key: &[u8], end: &[u8]) -> bool {
    key < end || end == RANGE_MAX
}

pub fn inside_region(key: &[u8], region: &Region) -> bool {
    key >= region.get_start_key() && before_end(key, region.get_end_key())
}

#[derive(Clone, Debug)]
pub struct RangeInfo {
    pub range: Range,
    pub size: usize,
}

impl RangeInfo {
    pub fn new(start: &[u8], end: &[u8], size: usize) -> RangeInfo {
        RangeInfo {
            range: new_range(start, end),
            size,
        }
    }
}

impl Deref for RangeInfo {
    type Target = Range;

    fn deref(&self) -> &Self::Target {
        &self.range
    }
}

/// RangeContext helps to decide a range end key.
pub struct RangeContext<Client> {
    client: Arc<Client>,
    region: Option<RegionInfo>,
    raw_size: usize,
    limit_size: usize,
}

impl<Client: ImportClient> RangeContext<Client> {
    pub fn new(client: Arc<Client>, limit_size: usize) -> RangeContext<Client> {
        RangeContext {
            client,
            region: None,
            raw_size: 0,
            limit_size,
        }
    }

    pub fn add(&mut self, size: usize) {
        self.raw_size += size;
    }

    /// Reset size and region for the next key.
    pub fn reset(&mut self, key: &[u8]) {
        self.raw_size = 0;
        if let Some(ref region) = self.region {
            if before_end(key, region.get_end_key()) {
                // Still belongs in this region, no need to update.
                return;
            }
        }
        self.region = match self.client.get_region(key) {
            Ok(region) => Some(region),
            Err(e) => {
                error!("get region failed"; "err" => %e);
                None
            }
        }
    }

    pub fn raw_size(&self) -> usize {
        self.raw_size
    }

    /// Check size and region range to see if we should stop before this key.
    pub fn should_stop_before(&self, key: &[u8]) -> bool {
        if self.raw_size >= self.limit_size {
            return true;
        }
        match self.region {
            Some(ref region) => !before_end(key, region.get_end_key()),
            None => false,
        }
    }
}

pub fn new_context(region: &RegionInfo) -> Context {
    let peer = if let Some(ref leader) = region.leader {
        leader.clone()
    } else {
        // We don't know the leader, just choose the first one.
        region.get_peers().first().unwrap().clone()
    };

    let mut ctx = Context::new();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(peer.clone());
    ctx
}

pub fn find_region_peer(region: &Region, store_id: u64) -> Option<Peer> {
    region
        .get_peers()
        .iter()
        .find(|p| p.get_store_id() == store_id)
        .cloned()
}

pub fn compute_reader_crc32<R: io::Read>(reader: &mut R) -> Result<(u64, u32)> {
    let mut digest = crc32::Digest::new(crc32::IEEE);
    let mut length = 0u64;
    let mut buf: [u8; 65536] = unsafe { MaybeUninit::uninit().assume_init() };

    loop {
        let size = reader.read(&mut buf)?;
        if size == 0 {
            break;
        }
        digest.write(&buf[..size]);
        length += size as u64;
    }
    Ok((length, digest.sum32()))
}

pub fn write_to_temp_db<R: io::Read>(
    reader: &mut R,
    temp_dir: &PathBuf,
    uuid: Uuid,
    db_cfg: &DbConfig,
) -> Result<DB> {
    let sst_file_path = {
        let path = temp_dir.join(format!("ingest-{}-sst", uuid));
        if path.exists() {
            return Err(Error::FileExists(path));
        }
        let mut data = Vec::default();
        reader.read_to_end(&mut data)?;
        fs::write(&path, &data)?;
        path
    };
    let db = {
        let db_path = temp_dir.join(format!("ingest-{}", uuid));
        let (db_opts, cf_opts) = tune_dboptions_for_bulk_load(db_cfg);
        new_engine_opt(db_path.to_str().unwrap(), db_opts, vec![cf_opts])?
    };
    db.ingest_external_file(
        &IngestExternalFileOptions::new(),
        &[sst_file_path.to_str().unwrap()],
    )?;
    Ok(db)
}

pub fn replace_ids_in_key(k: &[u8], table_ids: &[IdPair], index_ids: &[IdPair]) -> Result<Vec<u8>> {
    let mut table_id_map = HashMap::default();
    let mut index_id_map = HashMap::default();
    for p in table_ids {
        let mut id = Vec::default();
        id.encode_i64(p.get_new_id())?;
        table_id_map.insert(p.get_old_id(), id);
    }
    for p in index_ids {
        let mut id = Vec::default();
        id.encode_i64(p.get_new_id())?;
        index_id_map.insert(p.get_old_id(), id);
    }

    // TiDB record key format: t{table_id}_r{handle}
    // TiDB index key format: t{table_id}_i{index_id}_...
    // After receiving a key-value pair, TiKV would encode the key , then we must convert a key
    // to a raw key before we parse it.
    let old_key = Key::from_encoded(k.to_vec()).into_raw()?;
    let mut new_key = table::TABLE_PREFIX.to_owned();
    if !old_key.starts_with(table::TABLE_PREFIX) {
        return Ok(k.to_vec());
    }
    let table_id = {
        let mut remaining = &old_key[table::TABLE_PREFIX.len()..];
        number::decode_i64(&mut remaining)?
    };
    new_key.append(
        &mut table_id_map
            .get(&table_id)
            .ok_or(Error::ImportFileFailed("unexpected table id".to_string()))?
            .clone(),
    );
    let key_type_prefix = &old_key[table::TABLE_PREFIX_KEY_LEN..table::PREFIX_LEN];
    if key_type_prefix == table::INDEX_PREFIX_SEP {
        new_key.extend_from_slice(table::INDEX_PREFIX_SEP);
        let mut index_id_slice = &old_key[table::PREFIX_LEN..table::PREFIX_LEN + table::ID_LEN];
        let index_id = number::decode_i64(&mut index_id_slice)?;
        new_key.append(
            &mut index_id_map
                .get(&index_id)
                .ok_or(Error::ImportFileFailed("unexpected index id".to_string()))?
                .clone(),
        );
        new_key.extend_from_slice(&old_key[table::PREFIX_LEN + table::ID_LEN..]);
    } else {
        new_key.extend_from_slice(&old_key[table::TABLE_PREFIX_KEY_LEN..]);
    }

    Ok(Key::from_raw(&new_key).into_encoded())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::import::test_helpers::*;

    #[test]
    fn test_before_end() {
        assert!(before_end(b"ab", b"bc"));
        assert!(!before_end(b"ab", b"ab"));
        assert!(!before_end(b"cd", b"bc"));
        assert!(before_end(b"cd", RANGE_MAX));
    }

    fn new_region_range(start: &[u8], end: &[u8]) -> Region {
        let mut r = Region::new();
        r.set_start_key(start.to_vec());
        r.set_end_key(end.to_vec());
        r
    }

    #[test]
    fn test_inside_region() {
        assert!(inside_region(&[], &new_region_range(&[], &[])));
        assert!(inside_region(&[1], &new_region_range(&[], &[])));
        assert!(inside_region(&[1], &new_region_range(&[], &[2])));
        assert!(inside_region(&[1], &new_region_range(&[0], &[])));
        assert!(inside_region(&[1], &new_region_range(&[1], &[])));
        assert!(inside_region(&[1], &new_region_range(&[0], &[2])));
        assert!(!inside_region(&[2], &new_region_range(&[], &[2])));
        assert!(!inside_region(&[2], &new_region_range(&[3], &[])));
        assert!(!inside_region(&[2], &new_region_range(&[0], &[1])));
    }

    #[test]
    fn test_range_context() {
        let mut client = MockClient::new();
        client.add_region_range(b"", b"k4");
        client.add_region_range(b"k4", b"");

        let mut ctx = RangeContext::new(Arc::new(client), 8);

        ctx.add(4);
        assert!(!ctx.should_stop_before(b"k2"));
        ctx.add(4);
        assert_eq!(ctx.raw_size(), 8);
        // Reach size limit.
        assert!(ctx.should_stop_before(b"k3"));

        ctx.reset(b"k3");
        assert_eq!(ctx.raw_size(), 0);
        ctx.add(4);
        assert_eq!(ctx.raw_size(), 4);
        // Reach region end.
        assert!(ctx.should_stop_before(b"k4"));

        ctx.reset(b"k4");
        assert_eq!(ctx.raw_size(), 0);
        ctx.add(4);
        assert!(!ctx.should_stop_before(b"k5"));
    }
}
