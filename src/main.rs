mod parser;

use std::{collections::HashMap, error::Error, io::Write};

use crate::parser::statement;

#[derive(Debug, Clone, PartialEq)]
enum Statement {
    Select(SelectStmt),
}

#[derive(Debug, Clone, PartialEq)]
struct SelectStmt {
    cols: Vec<String>,
    table: String,
    condition: Option<(Term, Term)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Term {
    Column(String),
    StrLiteral(String),
}

type Database = HashMap<String, Table>;

struct Table {
    schema: Vec<RowSchema>,
    data: Vec<String>,
}

struct RowSchema {
    name: String,
}

fn exec_select(db: &Database, sql: &SelectStmt) -> Result<String, Box<dyn Error>> {
    let Some(table) = db.get(&sql.table) else {
        return Err(format!("Table {} not found", sql.table).into());
    };
    let cols = sql
        .cols
        .iter()
        .map(|col| {
            table
                .schema
                .iter()
                .enumerate()
                .find(|(_, s)| s.name == *col)
                .map(|(i, _)| i)
                .ok_or_else(|| format!("Column \"{}\" not found", col))
        })
        .collect::<Result<Vec<_>, String>>()?;

    let mut buf = vec![];
    let count = table.data.len() / table.schema.len();
    let stride = table.schema.len();
    for row in 0..count {
        if let Some(cond) = &sql.condition {
            match cond {
                (Term::Column(lhs), Term::StrLiteral(rhs)) => {
                    let lhs_idx = table
                        .schema
                        .iter()
                        .enumerate()
                        .find(|(_, c)| c.name == *lhs)
                        .ok_or_else(|| format!("Column \"{}\" not found", lhs))?
                        .0;
                    let lhs_val = table
                        .data
                        .get(lhs_idx + row * stride)
                        .ok_or_else(|| "Column index not found")?;
                    if lhs_val != rhs {
                        continue;
                    }
                }
                (Term::StrLiteral(lhs), Term::Column(rhs)) => {
                    let rhs_idx = table
                        .schema
                        .iter()
                        .enumerate()
                        .find(|(_, c)| c.name == *rhs)
                        .ok_or_else(|| format!("Column \"{}\" not found", rhs))?
                        .0;
                    let rhs_val = table
                        .data
                        .get(rhs_idx + row * stride)
                        .ok_or_else(|| "Column index not found")?;
                    if lhs != rhs_val {
                        continue;
                    }
                }
                _ => {
                    return Err(
                        "Where clause can only be column = 'literal' or 'literal' = column".into(),
                    );
                }
            }
        }
        for col in &cols {
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

    let phonebook = Table {
        schema: vec![
            RowSchema {
                name: "id".to_string(),
            },
            RowSchema {
                name: "name".to_string(),
            },
            RowSchema {
                name: "phone".to_string(),
            },
        ],
        data: vec![
            "101".to_string(),
            "Ada".to_string(),
            "002-2232-4564".to_string(),
            "102".to_string(),
            "Alan".to_string(),
            "004-3515-1622".to_string(),
        ],
    };

    let authors = Table {
        schema: vec![
            RowSchema {
                name: "id".to_string(),
            },
            RowSchema {
                name: "name".to_string(),
            },
        ],
        data: vec![
            "1".to_string(),
            "Asimov".to_string(),
            "2".to_string(),
            "Heinlein".to_string(),
        ],
    };

    let books = Table {
        schema: vec![
            RowSchema {
                name: "id".to_string(),
            },
            RowSchema {
                name: "name".to_string(),
            },
            RowSchema {
                name: "author".to_string(),
            },
        ],
        data: vec![
            "101".to_string(),
            "I, Robot".to_string(),
            "1".to_string(),
            "102".to_string(),
            "Cave of Steel".to_string(),
            "1".to_string(),
            "201".to_string(),
            "Moon's Harsh Mistress".to_string(),
            "2".to_string(),
        ],
    };

    let mut db = HashMap::new();
    db.insert("main".to_string(), table);
    db.insert("phonebook".to_string(), phonebook);
    db.insert("authors".to_string(), authors);
    db.insert("books".to_string(), books);

    let src = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "SELECT id, data FROM main".to_string());

    let stmt = dbg!(statement(&src).unwrap().1);
    match stmt {
        Statement::Select(ref rows) => {
            let output = exec_select(&db, rows);
            match output {
                Ok(out) => println!("Result: \n{out}"),
                Err(e) => eprintln!("Error: {e}"),
            }
        }
    }
}
