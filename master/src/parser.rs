use clap::{App, Arg, ArgMatches};

pub fn parse_command_line<'a>() -> ArgMatches<'a> {
    App::new("master")
        .version(crate_version!())
        .author("Cerberus Authors <cerberus@cpssd.net>")
        .about(
            "Responsible for scheduling MapReduce jobs and managing workers",
        )
        .arg(
            Arg::with_name("port")
                .long("port")
                .short("p")
                .help(
                    "Port which the master will use to communicate with the client and workers",
                )
                .takes_value(true)
                .required(false),
        )
        .get_matches()
}