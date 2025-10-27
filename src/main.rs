mod parser;
mod select;

use std::collections::HashMap;

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

#[derive(Debug)]
struct RowSchema {
    name: String,
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
