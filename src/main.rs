mod parser;

use std::{error::Error, io::Write};

use crate::parser::statement;

#[derive(Debug, Clone, PartialEq)]
enum Statement {
    Select(Vec<String>),
}

struct Table {
    schema: Vec<RowSchema>,
    data: Vec<String>,
}

struct RowSchema {
    name: String,
}

fn exec_select(table: &Table, rows: &[String]) -> Result<String, Box<dyn Error>> {
    let Some(indices) = rows
        .iter()
        .map(|row| {
            table
                .schema
                .iter()
                .enumerate()
                .find(|(_, s)| s.name == *row)
                .map(|(i, _)| i)
        })
        .collect::<Option<Vec<_>>>()
    else {
        return Err("Column not found".into());
    };

    let mut buf = vec![];
    let count = table.data.len() / table.schema.len();
    let stride = table.schema.len();
    for row in 0..count {
        for col in &indices {
            if let Some(cell) = table.data.get(col + row * stride) {
                write!(&mut buf, "{cell},")?;
            }
        }
        writeln!(&mut buf, "")?;
    }
    Ok(String::from_utf8(buf)?)
}

fn main() {
    let table = Table {
        schema: vec![
            RowSchema {
                name: "id".to_string(),
            },
            RowSchema {
                name: "data".to_string(),
            },
        ],
        data: vec![
            "1".to_string(),
            "Hello".to_string(),
            "2".to_string(),
            "World".to_string(),
            "3".to_string(),
            "!".to_string(),
        ],
    };

    let src = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "SELECT id, data FROM table".to_string());

    let stmt = statement(&src).unwrap().1;
    match stmt {
        Statement::Select(ref rows) => {
            let output = exec_select(&table, rows);
            match output {
                Ok(out) => println!("Result: \n{out}"),
                Err(e) => eprintln!("Error: {e}"),
            }
        }
    }
}
