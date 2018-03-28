//! Module owning the global settings struct, as well as functions for initialising it.

use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::sync::RwLock;

use clap::ArgMatches;
use config;
use config::Config;
use failure::*;

lazy_static! {
    pub static ref SETTINGS: RwLock<Config> = RwLock::new(Config::default());
}

pub fn init<'a>(opts: &ArgMatches<'a>) -> Result<(), Error> {
    info!("Initialising settings.");
    let mut settings = SETTINGS.write().unwrap();
    // Set the default values before merging in external sources.
    set_defaults(&mut settings)?;

    // False because we have defaults and it doesn't matter if no config file is provided.
    // This will match "Settings.json", "Settings.toml", "Settings.hjson", and "Settings.yaml".
    settings.merge(config::File::with_name("Settings").required(false))?;

    // We read command line options after reading the config file so that the command line has
    // priority.
    set_options(&mut settings, opts)?;

    debug!(
        "{:?}",
        settings
            .clone()
            .try_into::<HashMap<String, String>>()
            .unwrap()
    );

    Ok(())
}

/// Read through the command line arguments and assign settings from there.
fn set_options<'a>(settings: &mut Config, opts: &ArgMatches<'a>) -> Result<(), Error> {
    if let Some(value) = opts.value_of("input_chunk_size") {
        let v = value
            .parse::<i64>()
            .context(SettingsErrorKind::OptionParseFailed)?;
        settings.set("input_chunk_size", v)?;
    }
    if let Some(value) = opts.value_of("broker.address") {
        settings.set("broker.address", value)?;
    }
    if let Some(value) = opts.value_of("server_port") {
        let v = value
            .parse::<i64>()
            .context(SettingsErrorKind::OptionParseFailed)?;
        settings.set("server.port", v)?;
    }
    if let Some(value) = opts.value_of("broker.queue_name") {
        settings.set("broker.queue_name", value)?;
    }
    Ok(())
}

fn set_defaults(settings: &mut Config) -> Result<(), Error> {
    settings.set_default("broker.queue_name", "heracles_tasks")?;
    settings.set_default("input_chunk_size", 67_108_864_i64)?; // 64 MiB
    settings.set_default("server.port", 8081)?;
    settings.set_default("server.thread_pool_size", 8)?;
    Ok(())
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Fail)]
pub enum SettingsErrorKind {
    #[fail(display = "Failed to parse command line option.")]
    OptionParseFailed,
}

#[derive(Debug)]
pub struct SettingsError {
    inner: Context<SettingsErrorKind>,
}

impl SettingsError {
    pub fn kind(&self) -> SettingsErrorKind {
        *self.inner.get_context()
    }
}

impl Fail for SettingsError {
    fn cause(&self) -> Option<&Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl Display for SettingsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl From<SettingsErrorKind> for SettingsError {
    fn from(kind: SettingsErrorKind) -> SettingsError {
        SettingsError {
            inner: Context::new(kind),
        }
    }
}

impl From<Context<SettingsErrorKind>> for SettingsError {
    fn from(inner: Context<SettingsErrorKind>) -> SettingsError {
        SettingsError { inner }
    }
}
