extern crate chrono;
extern crate clap;
extern crate failure;
extern crate fern;
#[macro_use]
extern crate log;

use failure::Error;

fn main() {
    if let Err(err) = run() {
        error!("{}", err);
        for cause in err.causes().skip(1) {
            error!("Caused by: {}", cause);
        }
        std::process::exit(1);
    }
}

fn run() -> Result<(), Error> {
    init_logger()?;

    Ok(())
}

fn init_logger() -> Result<(), Error> {
    let base_config = fern::Dispatch::new();

    let file_config = fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                chrono::Utc::now().format("[%Y-%m-%d][%H:%M:%S]"),
                record.target(),
                record.level(),
                message
            ))
        })
        .level(log::LevelFilter::Debug)
        .chain(fern::log_file("heracles-master.log")?);

    let stderr_config = fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                chrono::Utc::now().format("[%Y-%m-%d][%H:%M:%S]"),
                record.target(),
                record.level(),
                message
            ))
        })
        .level(log::LevelFilter::Info)
        .chain(std::io::stderr());

    base_config.chain(file_config).chain(stderr_config).apply()?;

    Ok(())
}
