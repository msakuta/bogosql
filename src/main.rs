mod csv;
mod eval;
mod parser;
mod select;
mod table;

use std::{collections::HashMap, error::Error, fs::read_dir};

use nom::Finish;

use clap::Parser;

use crate::{
    parser::statement,
    select::{CsvOutput, SelectStmt, exec_select, format_select},
    table::{Table, make_table},
};

#[derive(Debug, Clone, PartialEq)]
enum Statement {
    Select(SelectStmt),
}

type Database = HashMap<String, Table>;

#[derive(Parser)]
struct Args {
    #[clap(default_value = "SELECT * FROM phonebook", help = "SQL string")]
    query: String,
    #[clap(short, long, default_value = "false", help = "Format output in CSV")]
    output_csv: bool,
}

fn main() -> Result<(), Box<dyn Error>> {
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
