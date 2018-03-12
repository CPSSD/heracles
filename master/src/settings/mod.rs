//! Module owning the global settings struct, as well as functions for initialising it.

use std::collections::HashMap;
use std::sync::RwLock;

use config::Config;
use config;
use failure::*;

lazy_static! {
    pub static ref SETTINGS: RwLock<Config> = RwLock::new(Config::default());
}

pub fn init() -> Result<(), Error> {
    info!("Initialising settings.");
    let mut settings = SETTINGS.write().unwrap();
    // Set the default values before merging in external sources.
    set_defaults(&mut settings)?;

    // False because we have defaults and it doesn't matter if no config file is provided.
    // This will match "Settings.json", "Settings.toml", "Settings.hjson", and "Settings.yaml".
    settings.merge(config::File::with_name("Settings").required(false))?;

    debug!(
        "{:?}",
        settings
            .clone()
            .try_into::<HashMap<String, String>>()
            .unwrap()
    );

    Ok(())
}

fn set_defaults(settings: &mut Config) -> Result<&mut Config, Error> {
    settings.set_default("task_input_size", 67_108_864)?; // 64 MiB
    Ok(settings)
}
