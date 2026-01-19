use crossbeam_channel::unbounded;
use serde_json::Number;
use serde_json::Value;
use std::fs::File;
use std::io::{self, BufRead};
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic;
use std::sync::atomic::AtomicUsize;
use std::thread;
use std::thread::JoinHandle;
use std::time;
use std::time::Instant;

static ATOMIC: AtomicUsize = AtomicUsize::new(0);

const NUMBER_OF_WORKER_THREADS: u8 = 10;
#[derive(Debug, PartialEq, Clone)]
enum FieldType {
    Int64,
    Float64,
    String,
    Record,
    Bool,
}

type Schema = Vec<FieldSchema>;

#[derive(Debug, PartialEq, Clone)]
struct FieldSchema {
    fieldype: FieldType,
    name: String,
    repeated: bool,
    required: bool,
    schema: Schema,
}

fn match_number_type(number: &Number) -> Option<FieldType> {
    if number.is_f64() {
        Some(FieldType::Float64)
    } else if number.is_u64() {
        Some(FieldType::Int64)
    } else if number.is_i64() {
        Some(FieldType::Int64)
    } else {
        None
    }
}

#[allow(unused)]
fn match_field_type(v: &Value) -> (Option<FieldType>, bool) {
    match v {
        Value::Null => (None, false),
        Value::Bool(b) => (Some(FieldType::Bool), false),
        Value::Number(number) => (match_number_type(number), false),
        Value::String(s) => (Some(FieldType::String), false),
        Value::Object(m) => (Some(FieldType::Record), false),
        Value::Array(a) => match &a[0] {
            Value::Null => (None, true),
            Value::Bool(b) => (Some(FieldType::Bool), true),
            Value::Number(number) => (match_number_type(number), true),
            Value::String(s) => (Some(FieldType::String), true),
            Value::Object(m) => (Some(FieldType::Record), true),
            _ => (None, true),
        },
    }
}

fn traverse_value_map(shared_state: Arc<RwLock<Schema>>, value: &Value) {
    if let Value::Object(m) = value {
        for (key, val) in m {
            if !exists(Arc::clone(&shared_state), key.to_string()) {
                let (fieldtype, repeated) = match_field_type(val);
                if let Some(ft) = fieldtype {
                    if ft == FieldType::Record && !repeated {
                        let nested_schema: Schema = vec![];

                        let shared_sub_state = Arc::new(RwLock::new(nested_schema));

                        traverse_value_map(Arc::clone(&shared_sub_state), val);

                        let field = FieldSchema {
                            name: key.clone(),
                            fieldype: ft,
                            repeated,
                            required: false,
                            schema: shared_sub_state.read().unwrap().clone(),
                        };
                        append(Arc::clone(&shared_state), field);
                    } else if ft == FieldType::Record && repeated {
                        let nested_schema: Schema = vec![];

                        let shared_sub_state = Arc::new(RwLock::new(nested_schema));

                        traverse_value_map(Arc::clone(&shared_sub_state), &val[0]);

                        let field = FieldSchema {
                            name: key.clone(),
                            fieldype: ft,
                            repeated,
                            required: false,
                            schema: shared_sub_state.read().unwrap().clone(),
                        };
                        append(Arc::clone(&shared_state), field);
                    } else {
                        let empty_schema: Schema = vec![];

                        let field = FieldSchema {
                            name: key.clone(),
                            fieldype: ft,
                            repeated,
                            required: false,
                            schema: empty_schema,
                        };
                        append(Arc::clone(&shared_state), field);
                    }
                }
            }
        }
    }
}

fn exists(shared_state: Arc<RwLock<Schema>>, name: String) -> bool {
    let schema = shared_state.read().unwrap();
    let mut exists = false;
    for field in schema.iter() {
        if field.name == name {
            exists = true;
        }
    }
    exists
}

fn append(shared_state: Arc<RwLock<Schema>>, new_field: FieldSchema) {
    let mut schema = shared_state.write().unwrap();
    let mut exists = false;
    for field in schema.iter() {
        if field.name == new_field.name {
            exists = true;
        }
    }
    if !exists {
        schema.push(new_field);
    }
}

fn process_line(shared_state: Arc<RwLock<Schema>>, line: String) {
    let value: Value = serde_json::from_str(&line).unwrap();

    traverse_value_map(shared_state, &value);
}

fn main() {
    let now = Instant::now();

    let file_names = vec![
        "./ndjson/benchmark/test1.ndjson",
        "./ndjson/benchmark/test2.ndjson",
        "./ndjson/benchmark/test3.ndjson",
        "./ndjson/benchmark/test4.ndjson",
        "./ndjson/benchmark/test5.ndjson",
    ];

    let mut sender_thread_handles: Vec<JoinHandle<()>> = vec![];
    let mut receiver_thread_handles: Vec<JoinHandle<()>> = vec![];

    let schema: Schema = vec![];

    let shared_state = Arc::new(RwLock::new(schema));

    let (tx, rx) = unbounded::<String>();

    println!("Spawning producer threads");
    for name in file_names {
        let tx_handle = tx.clone();
        let handle = thread::spawn(move || {
            let file = File::open(name).expect("must work");
            let lines = io::BufReader::new(file).lines();
            for line in lines.map_while(Result::ok) {
                tx_handle.send(line).unwrap();
            }
        });
        sender_thread_handles.push(handle);
    }

    let ten_millis = time::Duration::from_millis(10);
    thread::sleep(ten_millis);

    println!("Spawning consumer threads");
    for i in 0..NUMBER_OF_WORKER_THREADS {
        let rx_handle = rx.clone();
        let shared_state_clone = Arc::clone(&shared_state);
        let handle = thread::spawn(move || {
            while !rx_handle.is_empty() {
                let line = rx_handle.recv().unwrap();
                process_line(Arc::clone(&shared_state_clone), line);
                let _counter = ATOMIC.fetch_add(1, atomic::Ordering::Relaxed);
            }
            println!("Consumer thread {}: channel is empty", i);
        });
        receiver_thread_handles.push(handle);
    }

    println!("Joining producer threads");
    for handle in sender_thread_handles {
        handle.join().unwrap();
    }

    println!("Joining consumer threads");

    for handle in receiver_thread_handles {
        handle.join().unwrap();
    }

    let a = ATOMIC.load(atomic::Ordering::Relaxed);
    println!("Atomic count: {}", a);

    println!(
        "Elapsed time in milliseconds: {}",
        now.elapsed().as_millis()
    );

    let schema = shared_state.read().unwrap();
    println!("Schema: {:?}", schema);
}
