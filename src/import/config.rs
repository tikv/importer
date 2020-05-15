// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::error::Error;
use std::path::Path;
use std::result::Result;

use serde::{Deserialize, Serialize};

use engine::rocks::DBCompressionType;
use tikv::config::{log_level_serde, DbConfig, MetricConfig};
use tikv_util::config::{ReadableDuration, ReadableSize};
use security::SecurityConfig;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "kebab-case")]
pub struct TiKvConfig {
    #[serde(with = "log_level_serde")]
    pub log_level: slog::Level,
    pub log_file: String,
    pub log_rotation_timespan: ReadableDuration,
    pub storage: StorageConfig,
    pub server: tikv::server::Config,
    pub metric: MetricConfig,
    pub status_server_address: Option<String>,
    pub rocksdb: DbConfig,
    pub security: SecurityConfig,
    pub import: Config,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "kebab-case")]
pub struct StorageConfig {
    pub data_dir: String,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub import_dir: String,
    pub num_threads: usize,
    pub num_import_jobs: usize,
    pub num_import_sst_jobs: usize,
    pub max_prepare_duration: ReadableDuration,
    pub region_split_size: ReadableSize,
    pub stream_channel_window: usize,
    pub max_open_engines: usize,
    pub upload_speed_limit: ReadableSize,
    pub min_available_ratio: f64,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            import_dir: "/tmp/tikv/import".to_owned(),
            num_threads: 16,
            num_import_jobs: 24,
            num_import_sst_jobs: 2, // this field is useless, kept just to satisfy `deny_unknown_fields`
            max_prepare_duration: ReadableDuration::minutes(5),
            region_split_size: ReadableSize::mb(512),
            stream_channel_window: 128,
            max_open_engines: 8,
            upload_speed_limit: ReadableSize::mb(512),
            min_available_ratio: 0.05,
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.num_threads == 0 {
            return Err("import.num_threads can not be 0".into());
        }
        if self.num_import_jobs == 0 {
            return Err("import.num_import_jobs can not be 0".into());
        }
        if self.num_import_sst_jobs == 0 {
            return Err("import.num_import_sst_jobs can not be 0".into());
        }
        if self.region_split_size.0 == 0 {
            return Err("import.region_split_size can not be 0".into());
        }
        if self.stream_channel_window == 0 {
            return Err("import.stream_channel_window can not be 0".into());
        }
        if self.max_open_engines == 0 {
            return Err("import.max_open_engines can not be 0".into());
        }
        if self.upload_speed_limit.0 == 0 {
            return Err("import.upload_speed_limit cannot be 0".into());
        }
        if self.min_available_ratio < 0.0 {
            return Err("import.min_available_ratio can not less than 0.02".into());
        }
        Ok(())
    }
}

impl Default for TiKvConfig {
    fn default() -> Self {
        let default_compression_per_level = [
            DBCompressionType::Lz4,
            DBCompressionType::No,
            DBCompressionType::No,
            DBCompressionType::No,
            DBCompressionType::No,
            DBCompressionType::No,
            DBCompressionType::Lz4,
        ];
        let mut rocksdb = DbConfig::default();
        rocksdb.defaultcf.write_buffer_size = ReadableSize::gb(1);
        rocksdb.defaultcf.max_write_buffer_number = 8;
        rocksdb.defaultcf.compression_per_level = default_compression_per_level;
        rocksdb.writecf.compression_per_level = default_compression_per_level;

        Self {
            log_level: slog::Level::Info,
            log_file: "".to_owned(),
            log_rotation_timespan: ReadableDuration::hours(24),
            server: tikv::server::Config {
                grpc_concurrency: 16,
                ..Default::default()
            },
            status_server_address: None,
            metric: MetricConfig::default(),
            rocksdb,
            security: SecurityConfig::default(),
            import: Config::default(),
            storage: StorageConfig::default(),
        }
    }
}

impl TiKvConfig {
    pub fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        // FIXME: DbConfig::validate is private.
        // self.rocksdb.validate()?;
        self.server.validate()?;
        self.security.validate()?;
        self.import.validate()?;
        Ok(())
    }

    pub fn from_file(path: &Path) -> Self {
        (|| -> Result<Self, Box<dyn Error>> {
            let s = std::fs::read_to_string(&path)?;
            Ok(toml::from_str(&s)?)
        })()
        .unwrap_or_else(|e| {
            panic!(
                "invalid auto generated configuration file {}, err {}",
                path.display(),
                e
            );
        })
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: "./".to_owned(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_unrecognized_config() {
        let res = toml::from_str::<TiKvConfig>("log-level = 'info'\n");
        assert!(res.is_ok());

        let res = toml::from_str::<TiKvConfig>("not-log-level = 'info'\n");
        assert!(res.is_err());
    }
}
