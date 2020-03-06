// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(slice_patterns)]
#![feature(proc_macro_hygiene)]

#[macro_use]
extern crate slog_global;

use std::env;
use std::path::Path;

use cmd::setup::*;
use cmd::signal_handler;

use clap::{crate_authors, crate_version, App, Arg, ArgMatches};

use cmd::fatal;
use tikv_importer::import::{ImportKVServer, TiKvConfig};
use tikv_util::{self as tikv_util, check_environment_variables, logger};

/// Returns the importer version information.
fn importer_version_info() -> String {
    let fallback = "Unknown (env var does not exist when building)";
    format!(
        "\nRelease Version:   {}\
         \nGit Commit Hash:   {}\
         \nGit Commit Branch: {}\
         \nUTC Build Time:    {}\
         \nRust Version:      {}",
        env!("CARGO_PKG_VERSION"),
        option_env!("TIKV_BUILD_GIT_HASH").unwrap_or(fallback),
        option_env!("TIKV_BUILD_GIT_BRANCH").unwrap_or(fallback),
        option_env!("TIKV_BUILD_TIME").unwrap_or(fallback),
        option_env!("TIKV_BUILD_RUSTC_VERSION").unwrap_or(fallback),
    )
}

/// Prints the tikv version information to the standard output.
fn log_importer_info() {
    info!("Welcome to TiKV Importer.");
    for line in importer_version_info().lines() {
        info!("{}", line);
    }
    info!("");
}

fn main() {
    let matches = App::new("TiKV Importer")
        .about("The importer server for TiKV")
        .author(crate_authors!())
        .version(crate_version!())
        .long_version(&*importer_version_info())
        .arg(
            Arg::with_name("config")
                .short("C")
                .long("config")
                .value_name("FILE")
                .help("Set the configuration")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("log-file")
                .long("log-file")
                .takes_value(true)
                .value_name("FILE")
                .help("Set the log file"),
        )
        .arg(
            Arg::with_name("log-level")
                .long("log-level")
                .takes_value(true)
                .value_name("LEVEL")
                .possible_values(&["trace", "debug", "info", "warn", "error", "off"])
                .help("Set the log level"),
        )
        .arg(
            Arg::with_name("addr")
                .short("A")
                .long("addr")
                .takes_value(true)
                .value_name("IP:PORT")
                .help("Set the listening address"),
        )
        .arg(
            Arg::with_name("import-dir")
                .long("import-dir")
                .takes_value(true)
                .value_name("PATH")
                .help("Set the directory to store importing kv data"),
        )
        .arg(
            Arg::with_name("status-server")
                .long("status-server")
                .takes_value(true)
                .value_name("IP:PORT")
                .help("set the status server address"),
        )
        .get_matches();

    let config = setup_config(&matches);

    // FIXME: Shouldn't need to construct tikv::config::TiKvConfig to use initial_logger.
    let mut tikv_config = tikv::config::TiKvConfig::default();
    tikv_config.log_level = config.log_level;
    tikv_config.log_file = config.log_file.clone();
    tikv_config.log_rotation_timespan = config.log_rotation_timespan.clone();
    initial_logger(&tikv_config);

    tikv_util::set_panic_hook(false, &config.storage.data_dir);

    initial_metric(&config.metric, None);
    log_importer_info();
    check_environment_variables();

    if tikv_util::panic_mark_file_exists(&config.storage.data_dir) {
        fatal!(
            "panic_mark_file {} exists, there must be something wrong with the db.",
            tikv_util::panic_mark_file_path(&config.storage.data_dir).display()
        );
    }

    run_import_server(&config);
}

fn overwrite_config_with_cmd_args(config: &mut TiKvConfig, matches: &ArgMatches<'_>) {
    if let Some(level) = matches.value_of("log-level") {
        config.log_level = logger::get_level_by_string(level).unwrap();
    }
    if let Some(file) = matches.value_of("log-file") {
        config.log_file = file.to_owned();
    }
    if let Some(addr) = matches.value_of("addr") {
        config.server.addr = addr.to_owned();
    }
    if let Some(import_dir) = matches.value_of("import-dir") {
        config.import.import_dir = import_dir.to_owned();
    }
    if let Some(status_server_address) = matches.value_of("status-server") {
        config.status_server_address = Some(status_server_address.to_owned())
    }
}

fn setup_config(matches: &ArgMatches<'_>) -> TiKvConfig {
    let mut config = matches
        .value_of_os("config")
        .map_or_else(TiKvConfig::default, |path| {
            TiKvConfig::from_file(Path::new(&path))
        });

    overwrite_config_with_cmd_args(&mut config, matches);

    // importer did not expose the advertise-addr yet.
    // don't let it fail the validation.
    config.server.advertise_addr = "192.0.2.0:0".to_owned();

    if let Err(e) = config.validate() {
        fatal!("invalid configuration: {:?}", e);
    }
    info!(
        "using config";
        "config" => serde_json::to_string(&config).unwrap(),
    );

    // FIXME: DbConfig::write_into_metrics is private.
    // config.rocksdb.write_into_metrics();

    config
}

fn run_import_server(config: &TiKvConfig) {
    let mut server = ImportKVServer::new(config);
    server.start();
    info!("import server started");
    signal_handler::wait_for_signal(None);
    server.shutdown();
    info!("import server shutdown");
}
