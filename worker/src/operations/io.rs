use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::Path;

use failure::*;

#[cfg(test)]
use mocktopus;
#[cfg(test)]
use mocktopus::macros::*;

pub fn read<P: AsRef<Path>>(path: P) -> Result<String, Error> {
    let file = File::open(&path).context(format!(
        "unable to open file {}",
        path.as_ref().to_string_lossy()
    ))?;

    let mut buf_reader = BufReader::new(file);
    let mut value = String::new();
    buf_reader.read_to_string(&mut value).context(format!(
        "unable to read content of {}",
        path.as_ref().to_string_lossy()
    ))?;

    Ok(value)
}

#[cfg_attr(test, mockable)]
pub fn write<P: AsRef<Path>>(path: P, data: &[u8]) -> Result<(), Error> {
    let mut file = File::create(&path).context(format!(
        "unable to create file {}",
        path.as_ref().to_string_lossy()
    ))?;
    file.write_all(data).context(format!(
        "unable to write content to {}",
        path.as_ref().to_string_lossy()
    ))?;

    Ok(())
}
