use std::env;
use std::path::{Path, PathBuf};

const DEFAULT_DATA_DIR: &str = "/var/lib/fantom-raft-engine";

pub struct PathConfig {
    pub data_dir: PathBuf,
}

pub fn get_path_config() -> PathConfig {
    match env::var("SAWTOOTH_RAFT_HOME") {
        Ok(prefix) => PathConfig {
            data_dir: Path::new(&prefix).join("data").to_path_buf(),
        },
        Err(_) => PathConfig {
            data_dir: Path::new(DEFAULT_DATA_DIR).to_path_buf(),
        },
    }
}
