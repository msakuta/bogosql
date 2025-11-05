mod csv;
mod eval;
mod parser;
mod select;

use std::{collections::HashMap, error::Error, fs::read_dir};

use nom::Finish;

use clap::Parser;

use crate::{
    parser::statement,
    select::{CsvOutput, SelectStmt, exec_select, format_select},
};

#[derive(Debug, Clone, PartialEq)]
enum Statement {
    Select(SelectStmt),
}

type Database = HashMap<String, Table>;

#[derive(Debug, PartialEq)]
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

#[derive(Debug, PartialEq)]
struct RowSchema {
    name: String,
}

fn make_table(name: &str, csv: &str) -> Result<Table, Box<dyn Error>> {
    let csv = crate::csv::parse_csv(&csv)?;
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
            return Err("CSV needs the same number of columns as the header".into());
        }
        for cell in record {
            data.push(cell.trim().to_string());
        }
    }
    Ok(Table {
        name: name.to_string(),
        schema,
        data,
    })
}

#[derive(Parser)]
struct Args {
    #[clap(default_value = "SELECT * FROM phonebook", help = "SQL string")]
    query: String,
    #[clap(short, long, default_value = "false", help = "Format output in CSV")]
    output_csv: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let mut db = HashMap::new();

    for entry in read_dir("data")? {
        if let Ok(f) = entry
            && let Ok(t) = f.file_type()
            && t.is_file()
        {
            let str = std::fs::read_to_string(f.path())?;
            let path = f.path();
            let Some(name) = path.file_stem() else {
                continue;
            };
            let table_name = name.to_string_lossy().to_string();
            let table = make_table(&table_name, &str).map_err(|e| {
                format!(
                    "error processing file {file:?}: {e}",
                    file = f.path().to_str()
                )
            })?;
            db.insert(table_name, table);
        }
    }

    let (rest, stmt) = statement(&args.query).finish().unwrap();

    if rest != "" {
        return Err(format!("SQL has not finished: extra string: \"{rest}\"").into());
    }

    match stmt {
        Statement::Select(ref rows) => {
            if args.output_csv {
                let mut buf = CsvOutput(vec![]);
                let _ = exec_select(&mut buf, &db, rows)?;
                let out = String::from_utf8(buf.0)?;
                println!("Result: \n{out}");
            } else {
                let mut buf: Vec<u8> = vec![];
                format_select(&mut buf, &db, rows)?;
                let out = String::from_utf8(buf)?;
                println!("Result: \n{out}");
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_make_table() {
        let csv = r#"id,name
1, a
2, b
3, c
"#;
        let table_name = "a";
        let table = make_table(table_name, csv).unwrap();
        assert_eq!(
            table,
            Table {
                name: table_name.to_string(),
                schema: vec![
                    RowSchema {
                        name: "id".to_string()
                    },
                    RowSchema {
                        name: "name".to_string()
                    }
                ],
                data: ["1", "a", "2", "b", "3", "c"]
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect(),
            }
        )
    }
}
