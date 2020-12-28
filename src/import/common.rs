// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use kvproto::import_sstpb::*;
use kvproto::kvrpcpb::*;
use kvproto::metapb::*;

use hex::ToHex;
use pd_client::RegionInfo;

use super::client::*;

// Just used as a mark, don't use them in comparison.
pub const RANGE_MIN: &[u8] = &[];
pub const RANGE_MAX: &[u8] = &[];

pub fn new_range(start: &[u8], end: &[u8]) -> Range {
    let mut range = Range::default();
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
    pub async fn reset(&mut self, key: &[u8]) {
        self.raw_size = 0;
        if let Some(ref region) = self.region {
            if before_end(key, region.get_end_key()) {
                // Still belongs in this region, no need to update.
                return;
            }
        }
        self.region = match self.client.get_region(key).await {
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

    let mut ctx = Context::default();
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

/// Wrapper of any type which provides a more human-readable debug output.
pub struct ReadableDebug<T>(pub T);

impl fmt::Debug for ReadableDebug<(&[u8], &[u8])> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (start, end) = self.0;
        let common_prefix_len = start.iter().zip(end).take_while(|(a, b)| a == b).count();
        (&start[..common_prefix_len]).write_hex_upper(f)?;
        f.write_str("{")?;
        (&start[common_prefix_len..]).write_hex_upper(f)?;
        f.write_str("..")?;
        (&end[common_prefix_len..]).write_hex_upper(f)?;
        f.write_str("}")
    }
}

impl fmt::Debug for ReadableDebug<&Range> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        ReadableDebug((self.0.get_start(), self.0.get_end())).fmt(f)
    }
}

impl fmt::Debug for ReadableDebug<&Region> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Region")
            .field("id", &self.0.get_id())
            .field(
                "range",
                &ReadableDebug((self.0.get_start_key(), self.0.get_end_key())),
            )
            .field("region_epoch", self.0.get_region_epoch())
            .field("peers", &self.0.get_peers())
            .finish()
    }
}

impl fmt::Debug for ReadableDebug<&RegionInfo> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegionInfo")
            .field("region", &ReadableDebug(&self.0.region))
            .field("leader", &self.0.leader)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::import::test_helpers::*;

    use futures::executor::block_on;

    #[test]
    fn test_before_end() {
        assert!(before_end(b"ab", b"bc"));
        assert!(!before_end(b"ab", b"ab"));
        assert!(!before_end(b"cd", b"bc"));
        assert!(before_end(b"cd", RANGE_MAX));
    }

    fn new_region_range(start: &[u8], end: &[u8]) -> Region {
        let mut r = Region::default();
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

        block_on(ctx.reset(b"k3"));
        assert_eq!(ctx.raw_size(), 0);
        ctx.add(4);
        assert_eq!(ctx.raw_size(), 4);
        // Reach region end.
        assert!(ctx.should_stop_before(b"k4"));

        block_on(ctx.reset(b"k4"));
        assert_eq!(ctx.raw_size(), 0);
        ctx.add(4);
        assert!(!ctx.should_stop_before(b"k5"));
    }

    #[test]
    fn test_readable_range() {
        assert_eq!(
            format!("{:?}", ReadableDebug(&new_range(b"xyw", b"xyz"))),
            "7879{77..7A}".to_owned()
        );
        assert_eq!(
            format!("{:?}", ReadableDebug(&new_range(b"xy", b"xyz"))),
            "7879{..7A}".to_owned()
        );
        assert_eq!(
            format!("{:?}", ReadableDebug(&new_range(b"abc", b"def"))),
            "{616263..646566}".to_owned()
        );
        assert_eq!(
            format!("{:?}", ReadableDebug(&new_range(b"", b"def"))),
            "{..646566}".to_owned()
        );
        assert_eq!(
            format!("{:?}", ReadableDebug(&new_range(b"pqrst", b"pqrst"))),
            "7071727374{..}".to_owned()
        );
        assert_eq!(
            format!("{:?}", ReadableDebug(&new_range(b"\xab\xcd\xef", b""))),
            "{ABCDEF..}".to_owned()
        );
    }
}
