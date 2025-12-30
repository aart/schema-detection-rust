use crossbeam_channel::unbounded;
use serde_json::Value;
use std::fs::File;
use std::io::{self, BufRead};
use std::thread;
use std::thread::JoinHandle;
use std::sync::Mutex;
use std::sync::Arc;




#[derive(Debug)]
enum FieldType {
    Int64,
    Float64,
    String,
    Record,
    Bool,
}

type Schema = Vec<FieldSchema>;

#[derive(Debug)]
struct FieldSchema {
    fieldype: FieldType,
    name: String,
    repeated: bool,
    required: bool,
    schema: Schema,
}

const DATA: &str = r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#;

#[allow(unused)]
fn match_field_type(v: &Value) -> (Option<FieldType>, bool) {
    match v {
        Value::Null => (None,false),
        Value::Bool(b) => (Some(FieldType::Bool),false),
        Value::Number(number) => {
            if number.is_f64() {
                (Some(FieldType::Float64),false)
            } else if number.is_u64() {
                (Some(FieldType::Int64),false)
            } else if number.is_i64() {
                (Some(FieldType::Int64),false)
            } else {
               ( None,false)
            }
        }
        Value::String(s) => (Some(FieldType::String),false),
        Value::Object(m) => (Some(FieldType::Record),false),

        Value::Array(a) => match &a[0] {
            Value::Null => (None,true),
            Value::Bool(b) => (Some(FieldType::Bool),true),
            Value::Number(number) => {
                if number.is_f64() {
                    (Some(FieldType::Float64),true)
                } else if number.is_u64() {
                    (Some(FieldType::Int64),true)
                } else if number.is_i64() {
                    (Some(FieldType::Int64),true)
                } else {
                    (None,true)
                }
            }
            Value::String(s) => (Some(FieldType::String),true),
            Value::Object(m) => (Some(FieldType::Record),true),
            _ => (None,true)
        },
    }
}

fn traverse_value_map(schema: Schema, value: &Value) {
    if let Value::Object(m) = value {
        for (key, val) in m {

            let (fieldtype,repeated) = match_field_type(&val);

            if let Some(ft) = fieldtype {

            let empty_schema:Schema = vec![];
            let field = FieldSchema{name:key.clone(), fieldype:ft, repeated:repeated, required: false,schema:empty_schema};
            println!("{:?}",field)
            }            
        }
    }
}

fn main() {
    let value: Value = serde_json::from_str(DATA).unwrap();
    let schema:Schema = vec![];  

    traverse_value_map(schema,&value);

    let names = vec!["data_1.ndjson", "data_2.ndjson"];

    let mut handles: Vec<JoinHandle<()>> = vec![];

    let schema :Schema = vec![];

    let _shared_state = Arc::new(Mutex::new(schema));

    // let (tx, rx) = unbounded();

    for name in names {
        let handler = thread::spawn(move || {
            let file = File::open(name).expect("must work");
            let lines = io::BufReader::new(file).lines();
            for line in lines.map_while(Result::ok) {
                println!("{}", line); // TODO
            }
        });
        handles.push(handler);
    }

    for handler in handles {
        handler.join().unwrap();
    }
}
