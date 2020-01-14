// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use kvproto::kvrpcpb::*;
use kvproto::metapb::*;

use pd_client::RegionInfo;
use tikv_util::collections::HashMap;

use super::client::*;
use super::common::*;
use super::Result;

#[derive(Clone)]
pub struct MockClient {
    counter: Arc<AtomicUsize>,
    regions: Arc<Mutex<HashMap<u64, Region>>>,
    scatter_regions: Arc<Mutex<HashMap<u64, Region>>>,
}

impl MockClient {
    pub fn new() -> MockClient {
        MockClient {
            counter: Arc::new(AtomicUsize::new(1)),
            regions: Arc::new(Mutex::new(HashMap::default())),
            scatter_regions: Arc::new(Mutex::new(HashMap::default())),
        }
    }

    fn alloc_id(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::SeqCst) as u64
    }

    pub fn add_region_range(&mut self, start: &[u8], end: &[u8]) {
        let mut r = Region::default();
        r.set_id(self.alloc_id());
        r.set_start_key(start.to_owned());
        r.set_end_key(end.to_owned());
        let mut peer = Peer::default();
        peer.set_id(self.alloc_id());
        peer.set_store_id(self.alloc_id());
        r.mut_peers().push(peer);
        let mut regions = self.regions.lock().unwrap();
        regions.insert(r.get_id(), r);
    }

    pub fn get_scatter_region(&self, id: u64) -> Option<RegionInfo> {
        let regions = self.scatter_regions.lock().unwrap();
        regions.get(&id).map(|r| RegionInfo::new(r.clone(), None))
    }
}

impl ImportClient for MockClient {
    fn get_region(&self, key: &[u8]) -> Result<RegionInfo> {
        let mut found = None;
        for region in self.regions.lock().unwrap().values() {
            if inside_region(key, region) {
                found = Some(region.clone());
                break;
            }
        }
        Ok(RegionInfo::new(found.unwrap(), None))
    }

    fn split_region(&self, _: &RegionInfo, split_key: &[u8]) -> Result<SplitRegionResponse> {
        let mut regions = self.regions.lock().unwrap();

        let region = regions
            .iter()
            .map(|(_, r)| r)
            .find(|r| {
                split_key >= r.get_start_key()
                    && (split_key < r.get_end_key() || r.get_end_key().is_empty())
            })
            .unwrap()
            .clone();

        regions.remove(&region.get_id());

        let mut left = region.clone();
        left.set_id(self.alloc_id());
        left.set_end_key(split_key.to_vec());
        regions.insert(left.get_id(), left.clone());

        let mut right = region.clone();
        right.set_start_key(split_key.to_vec());
        regions.insert(right.get_id(), right.clone());

        let mut resp = SplitRegionResponse::default();
        resp.set_left(left);
        resp.set_right(right);
        Ok(resp)
    }

    fn scatter_region(&self, region: &RegionInfo) -> Result<()> {
        let mut regions = self.scatter_regions.lock().unwrap();
        regions.insert(region.get_id(), region.region.clone());
        Ok(())
    }

    fn has_region_id(&self, region_id: u64) -> Result<bool> {
        let regions = self.regions.lock().unwrap();
        Ok(regions.contains_key(&region_id))
    }

    fn is_scatter_region_finished(&self, _: u64) -> Result<bool> {
        Ok(true)
    }

    fn is_space_enough(&self, _: u64, _: u64) -> Result<bool> {
        Ok(true)
    }
}
