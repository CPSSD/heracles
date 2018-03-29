extern crate cerberus;
#[macro_use]
extern crate error_chain;

use cerberus::*;

struct TestMapper;
impl Map for TestMapper {
    type Key = String;
    type Value = String;
    fn map<E>(&self, input: MapInputKV, mut emitter: E) -> Result<()>
    where
        E: EmitIntermediate<Self::Key, Self::Value>,
    {
        for word in input.value.split_whitespace() {
            emitter.emit(word.to_owned(), "test".to_owned())?;
        }
        Ok(())
    }
}

struct TestReducer;
impl Reduce for TestReducer {
    type Value = String;
    fn reduce<E>(&self, input: ReduceInputKV<Self::Value>, mut emitter: E) -> Result<()>
    where
        E: EmitFinal<Self::Value>,
    {
        emitter.emit(input.values.iter().fold(String::new(), |acc, x| acc + x))?;
        Ok(())
    }
}

fn run() -> Result<()> {
    let test_mapper = TestMapper;
    let test_reducer = TestReducer;

    let matches = cerberus::parse_command_line();

    let registry = UserImplRegistryBuilder::new()
        .mapper(&test_mapper)
        .reducer(&test_reducer)
        .build()
        .chain_err(|| "Error building UserImplRegistry.")?;

    cerberus::run(&matches, &registry)
}

// Macro to generate a quick error_chain main function.
// https://github.com/rust-lang-nursery/error-chain/blob/master/examples/quickstart.rs
quick_main!(run);
