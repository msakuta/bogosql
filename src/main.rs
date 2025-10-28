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
            let mut csv = csv::Reader::from_path(f.path())?;
            let schema = csv
                .headers()?
                .iter()
                .map(|r| RowSchema {
                    name: r.trim().to_string(),
                })
                .collect::<Vec<_>>();
            let mut data = vec![];
            for record in csv.records() {
                if let Ok(r) = record {
                    for cell in r.iter() {
                        data.push(cell.trim().to_string());
                    }
                }
            }
            let path = f.path();
            let Some(name) = path.file_stem() else {
                continue;
            };
            db.insert(name.to_string_lossy().to_string(), Table { schema, data });
        }
    }

    let src = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "SELECT id, data FROM main".to_string());

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
