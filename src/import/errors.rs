// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::Error as IoError;
use std::num::ParseIntError;
use std::path::PathBuf;
use std::result;

use grpcio::Error as GrpcError;
use kvproto::errorpb;
use kvproto::metapb::*;
use thiserror::Error;
use uuid::{self, Uuid};

use pd_client::{Error as PdError, RegionInfo};
use tikv_util::codec::Error as CodecError;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Io(#[from] IoError),
    #[error("{0}")]
    Grpc(#[from] GrpcError),
    #[error("{0}")]
    Uuid(#[from] uuid::BytesError),
    #[error("{0}")]
    Codec(#[from] CodecError),
    #[error("RocksDB {0}")]
    RocksDB(String),
    #[error("Engine {0:?}")]
    Engine(#[from] engine_traits::Error),
    #[error("{0}")]
    ParseIntError(#[from] ParseIntError),
    #[error("File {0:?} exists")]
    FileExists(PathBuf),
    #[error("File {0:?} not exists")]
    FileNotExists(PathBuf),
    #[error("File {path:?} corrupted: {reason}")]
    FileCorrupted { path: PathBuf, reason: String },
    #[error("Invalid SST path {0:?}")]
    InvalidSSTPath(PathBuf),
    #[error("Engine {0} is in use")]
    EngineInUse(Uuid),
    #[error("Engine {0} not found")]
    EngineNotFound(Uuid),
    #[error("Invalid proto message {0}")]
    InvalidProtoMessage(String),
    #[error("Invalid chunk")]
    InvalidChunk,
    #[error("{0}")]
    PdRPC(#[from] PdError),
    #[error("TikvRPC {0:?}")]
    TikvRPC(errorpb::Error),
    #[error("NotLeader, leader may {0:?}")]
    NotLeader(Option<Peer>),
    #[error("EpochNotMatch {0}")]
    EpochNotMatch(Vec<Region>),
    #[error("UpdateRegion {0}")]
    UpdateRegion(RegionInfo),
    #[error("{0}")]
    ImportJobFailed(String),
    #[error("{0}")]
    ImportSSTJobFailed(String),
    #[error("{0}")]
    PrepareRangeJobFailed(String),
    #[error("{0}")]
    ResourceTemporarilyUnavailable(String),
    #[error("{0}")]
    Security(String),
}

pub type Result<T> = result::Result<T, Error>;

impl From<errorpb::Error> for Error {
    fn from(mut err: errorpb::Error) -> Self {
        if err.has_not_leader() {
            let mut error = err.take_not_leader();
            if error.has_leader() {
                Error::NotLeader(Some(error.take_leader()))
            } else {
                Error::NotLeader(None)
            }
        } else if err.has_epoch_not_match() {
            let mut error = err.take_epoch_not_match();
            Error::EpochNotMatch(error.take_current_regions().to_vec())
        } else {
            Error::TikvRPC(err)
        }
    }
}

#[test]
fn test_description() {
    assert_eq!(
        Error::from(GrpcError::QueueShutdown).to_string(),
        GrpcError::QueueShutdown.to_string()
    );
}
