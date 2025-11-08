use std::error::Error;

#[derive(Debug, PartialEq)]
pub struct Table {
    pub name: String,
    pub schema: Vec<RowSchema>,
    pub data: Vec<String>,
}

impl Table {
    pub fn get(&self, row: usize, col: usize) -> Option<&String> {
        let cols = self.schema.len();
        self.data.get(col + row * cols)
    }
}

#[derive(Debug, PartialEq)]
pub struct RowSchema {
    pub name: String,
}

pub fn make_table(name: &str, csv: &str) -> Result<Table, Box<dyn Error>> {
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
