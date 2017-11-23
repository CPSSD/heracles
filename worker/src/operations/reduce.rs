use std::collections::HashMap;
use std::io::BufReader;
use std::io::prelude::*;
use std::fs;
use std::fs::File;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;

use serde_json;
use serde_json::Value;
use serde_json::Value::Array;

use errors::*;
use cerberus_proto::worker as pb;
use master_interface::MasterInterface;
use operations::operation_handler;
use operations::operation_handler::OperationState;
use util::output_error;

/// `ReduceOperation` stores the input for a reduce operation for a single key.
struct ReduceOperation {
    intermediate_key: String,
    input: String,
}

#[derive(Serialize)]
struct ReduceInput {
    pub key: String,
    pub values: Vec<Value>,
}

struct ReduceOptions {
    reducer_file_path: String,
    output_directory: String,
}

/// `ReduceOperationQueue` is a struct used for storing and executing a queue of `ReduceOperations`.
#[derive(Default)]
pub struct ReduceOperationQueue {
    queue: Vec<ReduceOperation>,
}

impl ReduceOperationQueue {
    pub fn new() -> Self {
        Default::default()
    }

    fn run_reducer(
        &self,
        reduce_options: &ReduceOptions,
        reduce_operation: &ReduceOperation,
        mut child: Child,
    ) -> Result<()> {
        if let Some(stdin) = child.stdin.as_mut() {
            stdin
                .write_all(reduce_operation.input.as_bytes())
                .chain_err(|| "Error writing to payload stdin.")?;
        } else {
            return Err("Error accessing stdin of payload binary.".into());
        }

        let output = child.wait_with_output().chain_err(
            || "Error waiting for payload result.",
        )?;

        let output_str = String::from_utf8(output.stdout).chain_err(
            || "Error accessing payload output.",
        )?;

        let reduce_results: Value = serde_json::from_str(&output_str).chain_err(
            || "Error parsing reduce results.",
        )?;
        let reduce_results_pretty: String = serde_json::to_string_pretty(&reduce_results)
            .chain_err(|| "Error prettifying reduce results")?;

        let mut file_path = PathBuf::new();
        file_path.push(reduce_options.output_directory.clone());
        file_path.push(reduce_operation.intermediate_key.clone());


        let mut file = File::create(file_path).chain_err(
            || "Failed to create reduce output file.",
        )?;

        file.write_all(reduce_results_pretty.as_bytes()).chain_err(
            || "Failed to write to reduce output file.",
        )?;
        Ok(())
    }

    fn perform_reduce_operation(
        &mut self,
        reduce_options: &ReduceOptions,
        reduce_operation: &ReduceOperation,
    ) -> Result<()> {
        let child = Command::new(reduce_options.reducer_file_path.to_owned())
            .arg("reduce")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .chain_err(|| "Failed to start reduce operation process.")?;

        fs::create_dir_all(reduce_options.output_directory.to_owned())
            .chain_err(|| "Failed to create output directory")?;

        self.run_reducer(reduce_options, reduce_operation, child)
            .chain_err(|| "Error running reducer.")?;

        Ok(())
    }

    fn perform_next_reduce_operation(&mut self, reduce_options: &ReduceOptions) -> Result<()> {
        if let Some(reduce_operation) = self.queue.pop() {
            self.perform_reduce_operation(reduce_options, &reduce_operation)
                .chain_err(|| "Error performing reduce operation.")?;
        }
        Ok(())
    }

    fn is_queue_empty(&self) -> bool {
        self.queue.is_empty()
    }

    fn set_queue(&mut self, new_queue: Vec<ReduceOperation>) {
        self.queue = new_queue;
    }
}

fn log_reduce_operation_err(err: Error, operation_state_arc: &Arc<Mutex<OperationState>>) {
    output_error(&err.chain_err(|| "Error running reduce operation."));
    operation_handler::set_failed_status(operation_state_arc);
}

fn send_reduce_result(
    master_interface_arc: &Arc<Mutex<MasterInterface>>,
    result: pb::ReduceResult,
) -> Result<()> {
    let master_interface = master_interface_arc.lock().unwrap();

    master_interface.return_reduce_result(result).chain_err(
        || "Error sending reduce result to master.",
    )?;
    Ok(())
}

fn create_reduce_operations(
    reduce_request: &pb::PerformReduceRequest,
) -> Result<Vec<ReduceOperation>> {
    let mut reduce_map: HashMap<String, Vec<Value>> = HashMap::new();

    for reduce_input_file in reduce_request.get_input_file_paths() {
        let file = File::open(reduce_input_file).chain_err(
            || "Couldn't open input file.",
        )?;

        let mut buf_reader = BufReader::new(file);
        let mut reduce_input = String::new();
        buf_reader.read_to_string(&mut reduce_input).chain_err(
            || "Couldn't read map input file.",
        )?;

        let parse_value: Value = serde_json::from_str(&reduce_input).chain_err(
            || "Error parsing map response.",
        )?;

        if let Array(ref pairs) = parse_value {
            for pair in pairs {
                let key = pair["key"].as_str().chain_err(
                    || "Error parsing reduce input.",
                )?;

                let value = pair["value"].clone();
                if value.is_null() {
                    return Err("Error parsing reduce input.".into());
                }

                let reduce_array = reduce_map.entry(key.to_owned()).or_insert_with(Vec::new);
                reduce_array.push(value);
            }
        }
    }

    let mut reduce_operations: Vec<ReduceOperation> = Vec::new();
    for (intermediate_key, reduce_array) in reduce_map {
        let reduce_input = ReduceInput {
            key: intermediate_key.to_owned(),
            values: reduce_array,
        };
        let reduce_operation = ReduceOperation {
            intermediate_key: intermediate_key.to_owned(),
            input: serde_json::to_string(&reduce_input).chain_err(
                || "Error seralizing reduce operation input.",
            )?,
        };
        reduce_operations.push(reduce_operation);
    }
    Ok(reduce_operations)
}

// Public api for performing a reduce task.
// TODO(#287): Refactor the perform_reduce and do_perform_reduce functions into more logical pieces
pub fn perform_reduce(
    reduce_request: &pb::PerformReduceRequest,
    operation_state_arc: &Arc<Mutex<OperationState>>,
    reduce_queue_arc: Arc<Mutex<ReduceOperationQueue>>,
    master_interface_arc: Arc<Mutex<MasterInterface>>,
) -> Result<()> {
    {
        let mut operation_state = operation_state_arc.lock().unwrap();
        operation_state.initial_cpu_time = operation_handler::get_cpu_time();
    }

    info!(
        "Performing reduce operation. reducer={}",
        reduce_request.reducer_file_path
    );

    if operation_handler::get_worker_status(operation_state_arc) == pb::WorkerStatus::BUSY {
        warn!("Reduce operation requested while worker is busy");
        return Err("Worker is busy.".into());
    }
    operation_handler::set_busy_status(operation_state_arc);

    let result = do_perform_reduce(
        reduce_request,
        Arc::clone(operation_state_arc),
        reduce_queue_arc,
        master_interface_arc,
    );
    if let Err(err) = result {
        log_reduce_operation_err(err, operation_state_arc);
        return Err("Error starting reduce operation.".into());
    }

    Ok(())
}

// Internal implementation for performing a reduce task.
fn do_perform_reduce(
    reduce_request: &pb::PerformReduceRequest,
    operation_state_arc: Arc<Mutex<OperationState>>,
    reduce_queue_arc: Arc<Mutex<ReduceOperationQueue>>,
    master_interface_arc: Arc<Mutex<MasterInterface>>,
) -> Result<()> {
    let reduce_operations = create_reduce_operations(reduce_request).chain_err(
        || "Error creating reduce operations from input.",
    )?;

    {
        let mut reduce_queue = reduce_queue_arc.lock().unwrap();
        reduce_queue.set_queue(reduce_operations);
    }

    let reduce_options = ReduceOptions {
        reducer_file_path: reduce_request.get_reducer_file_path().to_owned(),
        output_directory: reduce_request.get_output_directory().to_owned(),
    };

    run_reduce_queue(
        reduce_options,
        operation_state_arc,
        reduce_queue_arc,
        master_interface_arc,
    );

    Ok(())
}

fn run_reduce_queue(
    reduce_options: ReduceOptions,
    operation_state_arc: Arc<Mutex<OperationState>>,
    reduce_queue_arc: Arc<Mutex<ReduceOperationQueue>>,
    master_interface_arc: Arc<Mutex<MasterInterface>>,
) {
    let initial_cpu_time = operation_state_arc.lock().unwrap().initial_cpu_time;

    thread::spawn(move || {
        let mut reduce_err = None;
        loop {
            let mut reduce_queue = reduce_queue_arc.lock().unwrap();

            if reduce_queue.is_queue_empty() {
                break;
            } else {
                let result = reduce_queue.perform_next_reduce_operation(&reduce_options);
                if let Err(err) = result {
                    reduce_err = Some(err);
                    break;
                }
            }
        }

        if let Some(err) = reduce_err {
            let mut response = pb::ReduceResult::new();
            response.set_status(pb::ResultStatus::FAILURE);
            response.set_failure_details(operation_handler::failure_details_from_error(&err));

            let result = send_reduce_result(&master_interface_arc, response);
            if let Err(err) = result {
                error!("Error sending reduce failed: {}", err);
            }

            log_reduce_operation_err(err, &operation_state_arc);
        } else {
            let mut response = pb::ReduceResult::new();
            response.set_status(pb::ResultStatus::SUCCESS);
            response.set_cpu_time(operation_handler::get_cpu_time() - initial_cpu_time);
            let result = send_reduce_result(&master_interface_arc, response);

            match result {
                Ok(_) => {
                    operation_handler::set_complete_status(&operation_state_arc);
                    info!("Reduce operation completed sucessfully.");
                }
                Err(err) => {
                    error!("Error sending reduce result: {}", err);
                    operation_handler::set_failed_status(&operation_state_arc);
                }
            }
        }
    });
}