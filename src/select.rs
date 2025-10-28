use std::{error::Error, io::Write};

use crate::{Database, Table};

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub cols: Cols,
    pub table: String,
    pub join: Option<JoinClause>,
    pub condition: Option<Condition>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Cols {
    Wildcard,
    List(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinClause {
    pub table: String,
    pub condition: Condition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Condition {
    Eq(Term, Term),
    Ne(Term, Term),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Term {
    Column(String),
    StrLiteral(String),
}

#[derive(Debug, Clone, Copy)]
struct ColRef<'a> {
    table: &'a Table,
    /// If the query has joins, this index is incremented for each join.
    joindex: usize,
    col: usize,
}

impl<'a> ColRef<'a> {
    fn new(table: &'a Table, joindex: usize, col: usize) -> Self {
        Self {
            table,
            joindex,
            col,
        }
    }

    fn get(&self, row_indices: &[usize]) -> Option<&String> {
        row_indices
            .get(self.joindex)
            .and_then(|row| self.table.get(*row, self.col))
    }
}

fn find_col<'a>(table: &'a Table, joindex: usize, name: &str) -> Option<ColRef<'a>> {
    table
        .schema
        .iter()
        .enumerate()
        .find(|(_, s)| s.name == name)
        .map(|(i, _)| ColRef::new(table, joindex, i))
}

pub fn exec_select(
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

        let (lhs, rhs) = match &join.condition {
            Condition::Eq(Term::Column(lhs), Term::Column(rhs)) => (lhs, rhs),
            Condition::Ne(Term::Column(lhs), Term::Column(rhs)) => (lhs, rhs),
            _ => {
                return Err(
                    "JOIN's ON condition must have association between tables, not a literal"
                        .into(),
                );
            }
        };

        println!("table schema: {:?}", table.schema);
        println!("joined table schema: {:?}", joined_table.schema);

        let cols = match &sql.cols {
            Cols::Wildcard => table
                .schema
                .iter()
                .enumerate()
                .map(|(i, _)| ColRef::new(table, 0, i))
                .chain(
                    joined_table
                        .schema
                        .iter()
                        .enumerate()
                        .map(|(i, _)| ColRef::new(joined_table, 1, i)),
                )
                .collect(),
            Cols::List(cols) => cols
                .iter()
                .map(|col| {
                    find_col(table, 0, col)
                        .or_else(|| find_col(joined_table, 1, col))
                        .ok_or_else(|| format!("Column \"{}\" not found", col))
                })
                .collect::<Result<Vec<_>, String>>()?,
        };

        let lhs_idx = find_col(table, 0, lhs)
            .or_else(|| find_col(joined_table, 1, lhs))
            .ok_or_else(|| format!("Neither table has column {lhs} in join clause"))?;

        let rhs_idx = find_col(table, 0, rhs)
            .or_else(|| find_col(joined_table, 1, rhs))
            .ok_or_else(|| format!("Neither table has column {rhs} in join clause"))?;

        let count = table.data.len() / table.schema.len();
        let joined_count = joined_table.data.len() / joined_table.schema.len();
        for row in 0..count {
            for joined_row in 0..joined_count {
                let row_cursor = [row, joined_row];
                let lhs_val = lhs_idx.get(&row_cursor);
                let rhs_val = rhs_idx.get(&row_cursor);

                if matches!(join.condition, Condition::Eq(_, _)) ^ (lhs_val == rhs_val) {
                    continue;
                }

                for col in &cols {
                    let cell = col.get(&row_cursor);
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

    let cols = match &sql.cols {
        Cols::Wildcard => table.schema.iter().enumerate().map(|(i, _)| i).collect(),
        Cols::List(cols) => cols
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
            .collect::<Result<Vec<_>, String>>()?,
    };

    let count = table.data.len() / table.schema.len();
    let stride = table.schema.len();
    for row in 0..count {
        if let Some(cond) = &sql.condition {
            match cond {
                Condition::Eq(Term::Column(lhs), Term::StrLiteral(rhs))
                | Condition::Ne(Term::Column(lhs), Term::StrLiteral(rhs)) => {
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
                    if !matches!(cond, Condition::Eq(_, _)) ^ (lhs_val != rhs) {
                        continue;
                    }
                }
                Condition::Eq(Term::StrLiteral(lhs), Term::Column(rhs))
                | Condition::Ne(Term::StrLiteral(lhs), Term::Column(rhs)) => {
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
                    if !matches!(cond, Condition::Eq(_, _)) ^ (lhs != rhs_val) {
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
