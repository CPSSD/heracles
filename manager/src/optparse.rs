use clap::{App, Arg, ArgMatches};

use super::VERSION;

pub fn parse_cmd_options<'a>() -> ArgMatches<'a> {
    App::new("heracles_manager")
        .version(VERSION.unwrap_or("unknown"))
        .author("Heracles Authors <heracles@cpssd.net>")
        .about("Scheduling service for the Heracles network.")
        .arg(
            Arg::with_name("input_chunk_size")
                .help("The size (in MiB) of the chunks created from the input files.")
                .long("input-chunk-size")
                .long_help(
                    "The size (in MiB) of the chunks created from the input files.
Each chunk corresponds to one map task, so this can be used to scale the job.",
                )
                .takes_value(true),
        )
        .arg(
            Arg::with_name("broker_address")
                .help("The address of the broker server the manager should connect to.")
                .long("broker-address")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("server_port")
                .help("Port on which the gRPC server is running")
                .long("server-port")
                .takes_value(true),
        )
        .get_matches()
}
