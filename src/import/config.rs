// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::error::Error;
use std::result::Result;

use serde::{Serialize, Deserialize};

use tikv_util::config::{ReadableDuration, ReadableSize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
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

// TODO: Decouple our program config from tikv::config.

impl From<tikv::import::Config> for Config {
    fn from(cfg: tikv::import::Config) -> Self {
        Self {
            import_dir: cfg.import_dir,
            num_threads: cfg.num_threads,
            num_import_jobs: cfg.num_import_jobs,
            num_import_sst_jobs: cfg.num_import_sst_jobs,
            max_prepare_duration: cfg.max_prepare_duration,
            region_split_size: cfg.region_split_size,
            stream_channel_window: cfg.stream_channel_window,
            max_open_engines: cfg.max_open_engines,
            upload_speed_limit: cfg.upload_speed_limit,
            min_available_ratio: cfg.min_available_ratio,
        }
    }
}

impl Default for Config {
    fn default() -> Config {
        Config {
            import_dir: "/tmp/tikv/import".to_owned(),
            num_threads: 8,
            num_import_jobs: 8,
            num_import_sst_jobs: 2,
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
