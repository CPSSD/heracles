use std::hash;
use std::io::{stdin, stdout};

use chrono::prelude::*;
use clap::{App, Arg, ArgMatches, SubCommand};
use uuid::Uuid;

use super::VERSION;
use emitter::IntermediateVecEmitter;
use errors::*;
use io::*;
use mapper::Map;
use partition::{HashPartitioner, Partition, PartitionInputPairs};
use reducer::Reduce;
use serialise::{FinalOutputObject, FinalOutputObjectEmitter, IntermediateOutputObject,
                IntermediateOutputObjectEmitter};

/// `UserImplRegistry` tracks the user's implementations of Map, Reduce, etc.
///
/// The user should use the `UserImplRegistryBuilder` to create this and then pass it in to `run`.
pub struct UserImplRegistry<'a, M, R>
where
    M: Map + 'a,
    R: Reduce + 'a,
{
    mapper: &'a M,
    reducer: &'a R,
}

/// `UserImplRegistryBuilder` is used to create a `UserImplRegistry`.
pub struct UserImplRegistryBuilder<'a, M, R>
where
    M: Map + 'a,
    R: Reduce + 'a,
{
    mapper: Option<&'a M>,
    reducer: Option<&'a R>,
}

impl<'a, M, R> Default for UserImplRegistryBuilder<'a, M, R>
where
    M: Map + 'a,
    R: Reduce + 'a,
{
    fn default() -> UserImplRegistryBuilder<'a, M, R> {
        UserImplRegistryBuilder {
            mapper: None,
            reducer: None,
        }
    }
}

impl<'a, M, R> UserImplRegistryBuilder<'a, M, R>
where
    M: Map + 'a,
    R: Reduce + 'a,
{
    pub fn new() -> UserImplRegistryBuilder<'a, M, R> {
        Default::default()
    }

    pub fn mapper(&mut self, mapper: &'a M) -> &mut UserImplRegistryBuilder<'a, M, R> {
        self.mapper = Some(mapper);
        self
    }

    pub fn reducer(&mut self, reducer: &'a R) -> &mut UserImplRegistryBuilder<'a, M, R> {
        self.reducer = Some(reducer);
        self
    }

    pub fn build(&self) -> Result<UserImplRegistry<'a, M, R>> {
        let mapper = self.mapper
            .chain_err(|| "Error building UserImplRegistry: No Mapper provided")?;
        let reducer = self.reducer
            .chain_err(|| "Error building UserImplRegistry: No Reducer provided")?;

        Ok(UserImplRegistry { mapper, reducer })
    }
}

/// `parse_command_line` uses `clap` to parse the command-line arguments passed to the payload.
///
/// The output of this function is required by the `run` function, to decide what subcommand to
/// run.
pub fn parse_command_line<'a>() -> ArgMatches<'a> {
    let current_time = Utc::now();
    let id = Uuid::new_v4();
    let payload_name = format!("{}_{}", current_time.format("%+"), id);
    let app = App::new(payload_name)
        .version(VERSION.unwrap_or("unknown"))
        .subcommand(
            SubCommand::with_name("map").arg(
                Arg::with_name("partition_count")
                    .long("partition_count")
                    .required(true)
                    .takes_value(true),
            ),
        )
        .subcommand(SubCommand::with_name("reduce"))
        .subcommand(SubCommand::with_name("sanity-check"));
    app.get_matches()
}

/// `run` begins the primary operations of the payload, and delegates to sub-functions.
///
/// # Arguments
///
/// `matches` - The output of the `parse_command_line` function.
/// `registry` - The output of the `register_mapper_reducer` function.
pub fn run<M, R>(matches: &ArgMatches, registry: &UserImplRegistry<M, R>) -> Result<()>
where
    M: Map,
    R: Reduce,
    <M as Map>::Key: hash::Hash,
{
    match matches.subcommand_name() {
        Some("map") => run_map(
            registry.mapper,
            matches
                .subcommand_matches("map")
                .unwrap()
                .value_of("partition_count")
                .unwrap()
                .parse::<u64>()
                .unwrap(),
        ),
        Some("reduce") => run_reduce(registry.reducer),
        Some("sanity-check") => {
            run_sanity_check();
            Ok(())
        }
        None => {
            eprintln!("{}", matches.usage());
            Ok(())
        }
        // This won't ever be reached, due to clap checking invalid commands before this.
        _ => unreachable!(),
    }
}

fn run_map<M>(mapper: &M, partition_count: u64) -> Result<()>
where
    M: Map,
    <M as Map>::Key: hash::Hash,
{
    let mut source = stdin();
    let mut sink = stdout();
    let input_kv = read_map_input(&mut source).chain_err(|| "Error getting input to map.")?;

    let mut pairs_vec: Vec<(M::Key, M::Value)> = Vec::new();

    mapper
        .map(input_kv, IntermediateVecEmitter::new(&mut pairs_vec))
        .chain_err(|| "Error running map operation.")?;

    let mut output_object = IntermediateOutputObject::<M::Key, M::Value>::default();

    HashPartitioner::new(partition_count)
        .partition(
            PartitionInputPairs::new(pairs_vec),
            IntermediateOutputObjectEmitter::new(&mut output_object),
        )
        .chain_err(|| "Error partitioning map output")?;

    write_map_output(&mut sink, &output_object)
        .chain_err(|| "Error writing map output to stdout.")?;
    Ok(())
}

fn run_reduce<R: Reduce>(reducer: &R) -> Result<()> {
    let mut source = stdin();
    let mut sink = stdout();
    let input_kv = read_reduce_input(&mut source).chain_err(|| "Error getting input to reduce.")?;
    let mut output_object = FinalOutputObject::<R::Value>::default();

    reducer
        .reduce(input_kv, FinalOutputObjectEmitter::new(&mut output_object))
        .chain_err(|| "Error running reduce operation.")?;

    write_reduce_output(&mut sink, &output_object)
        .chain_err(|| "Error writing reduce output to stdout.")?;
    Ok(())
}

fn run_sanity_check() {
    println!("sanity located");
}
