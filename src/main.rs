use crossbeam_channel::unbounded;
use serde_json::Value;
use std::fs::File;
use std::io::{self, BufRead};
use std::time::Instant;
use std::time;
use std::sync::Arc;
use std::sync::RwLock;
use std::thread;
use std::thread::JoinHandle;

const NUMBER_OF_WORKER_THREADS : u8 = 5;
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

#[allow(unused)]
fn match_field_type(v: &Value) -> (Option<FieldType>, bool) {
    match v {
        Value::Null => (None, false),
        Value::Bool(b) => (Some(FieldType::Bool), false),
        Value::Number(number) => {
            if number.is_f64() {
                (Some(FieldType::Float64), false)
            } else if number.is_u64() {
                (Some(FieldType::Int64), false)
            } else if number.is_i64() {
                (Some(FieldType::Int64), false)
            } else {
                (None, false)
            }
        }
        Value::String(s) => (Some(FieldType::String), false),
        Value::Object(m) => (Some(FieldType::Record), false),

        Value::Array(a) => match &a[0] {
            Value::Null => (None, true),
            Value::Bool(b) => (Some(FieldType::Bool), true),
            Value::Number(number) => {
                if number.is_f64() {
                    (Some(FieldType::Float64), true)
                } else if number.is_u64() {
                    (Some(FieldType::Int64), true)
                } else if number.is_i64() {
                    (Some(FieldType::Int64), true)
                } else {
                    (None, true)
                }
            }
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
                let (fieldtype, repeated) = match_field_type(&val);
                if let Some(ft) = fieldtype {
                    if ft == FieldType::Record && !repeated {
                        let nested_schema: Schema = vec![];

                        let shared_sub_state = Arc::new(RwLock::new(nested_schema));

                        traverse_value_map(Arc::clone(&shared_sub_state), &val);

                        let field = FieldSchema {
                            name: key.clone(),
                            fieldype: ft,
                            repeated: repeated,
                            required: false,
                            schema: shared_sub_state.read().unwrap().clone(),
                        };
                        append(Arc::clone(&shared_state), field);
                    } else if ft == FieldType::Record && !repeated {
                        let nested_schema: Schema = vec![];

                        let shared_sub_state = Arc::new(RwLock::new(nested_schema));

                        traverse_value_map(Arc::clone(&shared_sub_state), &val[0]);

                        let field = FieldSchema {
                            name: key.clone(),
                            fieldype: ft,
                            repeated: repeated,
                            required: false,
                            schema: shared_sub_state.read().unwrap().clone(),
                        };
                        append(Arc::clone(&shared_state), field);
                    } else {
                        let empty_schema: Schema = vec![];

                        let field = FieldSchema {
                            name: key.clone(),
                            fieldype: ft,
                            repeated: repeated,
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

    let file_names = vec!["./ndjson/benchmark/test1.ndjson","./ndjson/benchmark/test2.ndjson","./ndjson/benchmark/test3.ndjson","./ndjson/benchmark/test4.ndjson","./ndjson/benchmark/test5.ndjson"];

    let mut sender_thread_handles: Vec<JoinHandle<()>> = vec![];
    let mut receiver_thread_handles: Vec<JoinHandle<()>> = vec![];

    let schema: Schema = vec![];

    let shared_state = Arc::new(RwLock::new(schema));

    let (tx, rx) = unbounded::<String>();

    for name in file_names {
        let tx_handle = tx.clone();
        let handle = thread::spawn(move || {
            let file = File::open(name).expect("must work");
            let lines = io::BufReader::new(file).lines();
            for line in lines.map_while(Result::ok) {
                _ = tx_handle.send(line).unwrap();
            }
        });
        sender_thread_handles.push(handle);
    }

    let ten_millis = time::Duration::from_millis(10);
    thread::sleep(ten_millis);
  

    for _ in 0..NUMBER_OF_WORKER_THREADS {
        let rx_handle = rx.clone();
        let shared_state_clone = Arc::clone(&shared_state);
        let handle = thread::spawn(move || {
            
            while !rx_handle.is_empty() {
                let line = rx_handle.recv().unwrap();
             
                process_line(Arc::clone(&shared_state_clone), line);
            }
        });
        receiver_thread_handles.push(handle);
    }

    for handle in sender_thread_handles {
        handle.join().unwrap();
    }
    
    for handle in receiver_thread_handles {
        handle.join().unwrap();
    }
    println!("Elapsed time in milliseconds: {}", now.elapsed().as_millis());

    let schema = shared_state.read().unwrap();
    println!("Schema: {:?}", schema);
    
}
