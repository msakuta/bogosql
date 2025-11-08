use std::error::Error;

use wasm_bindgen::prelude::*;

use bogosql::{Database, format_select, make_table, statement};

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    pub(crate) fn log(s: &str);
}

#[wasm_bindgen]
pub fn run_query(src: &str) -> Result<String, JsValue> {
    Ok(run_query_impl(src).map_err(|e| JsValue::from_str(&e.to_string()))?)
}

fn run_query_impl(src: &str) -> Result<String, Box<dyn Error>> {
    let mut db = Database::new();

    for (file, csv) in [
        ("authors", include_str!("../../data/authors.csv")),
        ("books", include_str!("../../data/books.csv")),
        ("characters", include_str!("../../data/characters.csv")),
        ("phonebook", include_str!("../../data/phonebook.csv")),
    ] {
        let table = make_table(file, csv)?;

        db.insert(file.to_string(), table);
    }

    let (_, bogosql::Statement::Select(query)) = statement(src).map_err(|e| e.to_string())?;

    let mut buf = vec![0u8; 0];
    format_select(&mut buf, &db, &query)?;
    let res = String::from_utf8(buf)?;

    Ok(res)
}
