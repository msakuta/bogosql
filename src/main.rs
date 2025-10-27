mod parser;

use std::{collections::HashMap, error::Error, io::Write};

use nom::Finish;

use crate::parser::statement;

#[derive(Debug, Clone, PartialEq)]
enum Statement {
    Select(SelectStmt),
}

#[derive(Debug, Clone, PartialEq)]
struct SelectStmt {
    cols: Vec<String>,
    table: String,
    join: Option<JoinClause>,
    condition: Option<(Term, Term)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct JoinClause {
    table: String,
    condition: (Term, Term),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Term {
    Column(String),
    StrLiteral(String),
}

type Database = HashMap<String, Table>;

#[derive(Debug)]
struct Table {
    schema: Vec<RowSchema>,
    data: Vec<String>,
}

#[derive(Debug)]
struct RowSchema {
    name: String,
}

#[derive(Debug, Clone, Copy)]
enum TableSide {
    First,
    Second,
}

fn exec_select(
    out: &mut impl Write,
    db: &Database,
    sql: &SelectStmt,
) -> Result<(), Box<dyn Error>> {
    let Some(table) = db.get(&sql.table) else {
        return Err(format!("Table {} not found", sql.table).into());
    };

    if let Some(join) = &sql.join {
        let Some(joined_table) = db.get(&join.table) else {
            return Err(format!("Table {} not found", join.table).into());
        };

        let (Term::Column(lhs), Term::Column(rhs)) = &join.condition else {
            return Err(
                "JOIN's ON condition must have association between tables, not a literal".into(),
            );
        };

        println!("table schema: {:?}", table.schema);
        println!("joined table schema: {:?}", joined_table.schema);

        let cols = sql
            .cols
            .iter()
            .map(|col| {
                table
                    .schema
                    .iter()
                    .enumerate()
                    .find(|(_, s)| s.name == *col)
                    .map(|(i, _)| (TableSide::First, i))
                    .or_else(|| {
                        joined_table
                            .schema
                            .iter()
                            .enumerate()
                            .find(|(_, c)| c.name == *col)
                            .map(|(i, _)| (TableSide::Second, i))
                    })
                    .ok_or_else(|| format!("Column \"{}\" not found", col))
            })
            .collect::<Result<Vec<_>, String>>()?;

        let lhs_idx = table
            .schema
            .iter()
            .enumerate()
            .find(|(_, c)| c.name == *lhs)
            .map(|(i, _)| (TableSide::First, i))
            .or_else(|| {
                joined_table
                    .schema
                    .iter()
                    .enumerate()
                    .find(|(_, c)| c.name == *lhs)
                    .map(|(i, _)| (TableSide::Second, i))
            })
            .ok_or_else(|| format!("Neither table has column {lhs} in join clause"))?;

        let rhs_idx = table
            .schema
            .iter()
            .enumerate()
            .find(|(_, c)| c.name == *rhs)
            .map(|(i, _)| (TableSide::First, i))
            .or_else(|| {
                joined_table
                    .schema
                    .iter()
                    .enumerate()
                    .find(|(_, c)| c.name == *rhs)
                    .map(|(i, _)| (TableSide::Second, i))
            })
            .ok_or_else(|| format!("Neither table has column {rhs} in join clause"))?;

        let count = table.data.len() / table.schema.len();
        let stride = table.schema.len();
        let joined_count = joined_table.data.len() / joined_table.schema.len();
        let joined_stride = joined_table.schema.len();
        for row in 0..count {
            for joined_row in 0..joined_count {
                let lhs_val = match lhs_idx.0 {
                    TableSide::First => table.data.get(lhs_idx.1 + row * stride),
                    TableSide::Second => joined_table
                        .data
                        .get(lhs_idx.1 + joined_row * joined_stride),
                };

                let rhs_val = match rhs_idx.0 {
                    TableSide::First => table.data.get(rhs_idx.1 + row * stride),
                    TableSide::Second => joined_table
                        .data
                        .get(rhs_idx.1 + joined_row * joined_stride),
                };

                if lhs_val != rhs_val {
                    continue;
                }

                for col in &cols {
                    let cell = match col.0 {
                        TableSide::First => table.data.get(col.1 + row * stride),
                        TableSide::Second => {
                            joined_table.data.get(col.1 + joined_row * joined_stride)
                        }
                    };
                    if let Some(cell) = cell {
                        write!(out, "{cell},")?;
                    }
                }
                writeln!(out, "")?;
            }
            continue;
        }

        return Ok(());
    }

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
                write!(out, "{cell},")?;
            }
        }
        writeln!(out, "")?;
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
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
                name: "author_id".to_string(),
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
                name: "book_id".to_string(),
            },
            RowSchema {
                name: "title".to_string(),
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
