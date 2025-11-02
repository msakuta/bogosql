mod csv;
mod parser;
mod select;

use std::{collections::HashMap, fs::read_dir};

use nom::Finish;

use crate::{
    parser::statement,
    select::{SelectStmt, exec_select},
};

#[derive(Debug, Clone, PartialEq)]
enum Statement {
    Select(SelectStmt),
}

type Database = HashMap<String, Table>;

#[derive(Debug)]
struct Table {
    name: String,
    schema: Vec<RowSchema>,
    data: Vec<String>,
}

impl Table {
    fn get(&self, row: usize, col: usize) -> Option<&String> {
        let cols = self.schema.len();
        self.data.get(col + row * cols)
    }
}

#[derive(Debug)]
struct RowSchema {
    name: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = HashMap::new();

    for entry in read_dir("data")? {
        if let Ok(f) = entry
            && let Ok(t) = f.file_type()
            && t.is_file()
        {
            let str = std::fs::read_to_string(f.path())?;
            let csv = crate::csv::parse_csv(&str)?;
            let schema = csv
                .first()
                .ok_or_else(|| "CSV needs at least 1 line for the header".to_string())?
                .iter()
                .map(|r| RowSchema {
                    name: r.trim().to_string(),
                })
                .collect::<Vec<_>>();
            let mut data = vec![];
            for record in &csv[1..] {
                if record.len() == 0 {
                    continue;
                }
                if record.len() != schema.len() {
                    return Err(format!(
                        "error processing file {file:?}: CSV needs the same number of columns as the header",
                        file = f.path().to_str()
                    )
                    .into());
                }
                for cell in record {
                    data.push(cell.trim().to_string());
                }
            }
            let path = f.path();
            let Some(name) = path.file_stem() else {
                continue;
            };
            let table_name = name.to_string_lossy().to_string();
            db.insert(
                table_name.clone(),
                Table {
                    name: table_name,
                    schema,
                    data,
                },
            );
        }
    }

    let src = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "SELECT id, data FROM phonebook".to_string());

    let (rest, stmt) = statement(&src).finish().unwrap();

    if rest != "" {
        return Err(format!("SQL has not finished: extra string: \"{rest}\"").into());
    }

    match stmt {
        Statement::Select(ref rows) => {
            let mut buf = vec![];
            let _ = exec_select(&mut buf, &db, rows)?;
            let out = String::from_utf8(buf)?;
            println!("Result: \n{out}");
        }
    }

    Ok(())
}
