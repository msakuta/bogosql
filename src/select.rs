use std::{error::Error, io::Write};

use crate::{Database, Table};

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub cols: Cols,
    pub table: String,
    pub join: Vec<JoinClause>,
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

fn find_col<'a>(tables: &[&'a Table], name: &str) -> Option<ColRef<'a>> {
    tables.iter().enumerate().find_map(|(joindex, table)| {
        table
            .schema
            .iter()
            .enumerate()
            .find(|(_, s)| s.name == name)
            .map(|(i, _)| ColRef::new(table, joindex, i))
    })
}

/// Increment the row cursor, similar to the add arithmetics.
/// Returns true while the incremented value is valid
fn incr_row_cursor(row_cursor: &mut [usize], row_counts: &[usize]) -> bool {
    let mut carry = true;
    let mut digit = row_cursor.len() - 1;
    while carry {
        let row_index = row_cursor.get_mut(digit).unwrap();
        let row_count = *row_counts.get(digit).unwrap();
        carry = row_count <= *row_index + 1;
        if carry {
            if let Some(prev) = digit.checked_sub(1) {
                *row_index = 0;
                digit = prev;
                continue;
            } else {
                return false;
            }
        }
        *row_index = (*row_index + 1) % row_count;
    }
    true
}

pub fn exec_select(
    out: &mut impl Write,
    db: &Database,
    sql: &SelectStmt,
) -> Result<(), Box<dyn Error>> {
    let Some(table) = db.get(&sql.table) else {
        return Err(format!("Table {} not found", sql.table).into());
    };

    if !sql.join.is_empty() {
        let joined_tables = std::iter::once(Ok(table))
            .chain(sql.join.iter().map(|join| {
                db.get(&join.table)
                    .ok_or_else(|| format!("Table {} not found", join.table))
            }))
            .collect::<Result<Vec<_>, String>>()?;

        let join_conditions = sql
            .join
            .iter()
            .map(|join| {
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

                let lhs_idx = find_col(&joined_tables, lhs)
                    .ok_or_else(|| format!("Neither table has column {lhs} in join clause"))?;

                let rhs_idx = find_col(&joined_tables, rhs)
                    .ok_or_else(|| format!("Neither table has column {rhs} in join clause"))?;

                Ok((lhs_idx, rhs_idx))
            })
            .collect::<Result<Vec<_>, String>>()?;

        let cols = match &sql.cols {
            Cols::Wildcard => joined_tables
                .iter()
                .enumerate()
                .map(|(joindex, table)| {
                    table
                        .schema
                        .iter()
                        .enumerate()
                        .map(move |(i, _)| ColRef::new(table, joindex, i))
                })
                .flatten()
                .collect(),
            Cols::List(cols) => cols
                .iter()
                .map(|col| {
                    find_col(&joined_tables, col)
                        .ok_or_else(|| format!("Column \"{}\" not found", col))
                })
                .collect::<Result<Vec<_>, String>>()?,
        };

        let row_counts = joined_tables
            .iter()
            .map(|table| table.data.len() / table.schema.len())
            .collect::<Vec<_>>();
        let mut row_cursor = vec![0; joined_tables.len()];

        loop {
            if sql
                .join
                .iter()
                .zip(join_conditions.iter())
                .any(|(join, (lhs, rhs))| {
                    let lhs_val = lhs.get(&row_cursor);
                    let rhs_val = rhs.get(&row_cursor);
                    matches!(join.condition, Condition::Eq(_, _)) ^ (lhs_val == rhs_val)
                })
            {
                if !incr_row_cursor(&mut row_cursor, &row_counts) {
                    break;
                } else {
                    continue;
                }
            }

            for col in &cols {
                let cell = col.get(&row_cursor);
                if let Some(cell) = cell {
                    write!(out, "{cell},")?;
                }
            }
            writeln!(out, "")?;

            if !incr_row_cursor(&mut row_cursor, &row_counts) {
                break;
            }
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
