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
    List(Vec<Column>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinClause {
    pub kind: JoinKind,
    pub table: String,
    pub condition: Condition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Condition {
    Eq(Term, Term),
    Ne(Term, Term),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Term {
    Column(Column),
    StrLiteral(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub table: Option<String>,
    pub column: String,
}

#[cfg(test)]
impl Column {
    pub fn new(column: impl Into<String>) -> Self {
        Self {
            table: None,
            column: column.into(),
        }
    }
}

impl std::fmt::Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref table) = self.table {
            write!(f, "{}.{}", table, self.column)
        } else {
            write!(f, "{}", self.column)
        }
    }
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

    fn get(&self, row_indices: &[RowCursor]) -> Option<&String> {
        row_indices
            .get(self.joindex)
            .and_then(|row| row.row)
            .and_then(|row| self.table.get(row, self.col))
    }
}

fn find_col<'a>(tables: &[&'a Table], column: &Column) -> Option<ColRef<'a>> {
    if let Some(ref table_name) = column.table {
        let (joindex, table) = tables
            .iter()
            .enumerate()
            .find(|(_, t)| t.name == *table_name)?;
        return table
            .schema
            .iter()
            .enumerate()
            .find(|(_, s)| s.name == column.column)
            .map(|(i, _)| ColRef::new(table, joindex, i));
    }
    tables
        .iter()
        .enumerate()
        .fold(None, |mut acc, (joindex, table)| {
            let candidate = table
                .schema
                .iter()
                .enumerate()
                .find(|(_, s)| s.name == column.column)
                .map(|(i, _)| ColRef::new(table, joindex, i));
            if candidate.is_some() {
                if acc.is_some() {
                    panic!("Column name {}", column.column);
                } else {
                    acc = candidate;
                }
            }
            acc
        })
}

#[derive(Clone, Copy, Debug)]
struct RowCursor {
    row: Option<usize>,
    shown: bool,
}

impl RowCursor {
    fn new() -> Self {
        Self {
            row: Some(0),
            shown: false,
        }
    }
}

/// Increment the row cursor, similar to the add arithmetics.
/// Returns true while the incremented value is valid
fn incr_row_cursor(row_cursor: &mut [RowCursor], row_counts: &[usize]) -> bool {
    let mut carry = true;
    let mut digit = row_cursor.len() - 1;
    while carry {
        let row_index = row_cursor.get_mut(digit).unwrap();
        let row_count = *row_counts.get(digit).unwrap();
        carry = row_index.row.is_none(); //row_count <= row_index.map_or(0, |ri| ri + 1);
        if carry {
            if let Some(prev) = digit.checked_sub(1) {
                // Reset the cursor for this table
                row_index.row = Some(0);
                row_index.shown = false;
                digit = prev;
                continue;
            } else {
                return false;
            }
        }
        row_index.row = if let Some(ref ri) = row_index.row
            && *ri + 1 < row_count
        {
            Some(*ri + 1)
        } else {
            None
        }
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

        let join_allow_none = std::iter::once(false)
            .chain(
                sql.join
                    .iter()
                    .map(|join| matches!(join.kind, JoinKind::Left)),
            )
            .collect::<Vec<_>>();

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
                .map(|(table_idx, table)| {
                    table
                        .schema
                        .iter()
                        .enumerate()
                        .map(move |(i, _)| ColRef::new(table, table_idx, i))
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
        let mut row_cursor = vec![RowCursor::new(); joined_tables.len()];

        loop {
            if sql
                .join
                .iter()
                .zip(join_conditions.iter())
                .all(|(join, (lhs, rhs))| {
                    let Some(lhs_val) = lhs.get(&row_cursor) else {
                        return join_allow_none[lhs.joindex] && !row_cursor[lhs.joindex].shown;
                    };
                    let Some(rhs_val) = rhs.get(&row_cursor) else {
                        return join_allow_none[lhs.joindex] && !row_cursor[rhs.joindex].shown;
                    };
                    // println!(
                    //     "{row_cursor:?}: {}.{}={lhs_val} {}.{}={rhs_val}, is_condition: {}, eq: {}, res: {}",
                    //     lhs.table.name,
                    //     lhs.table.schema[lhs.col].name,
                    //     rhs.table.name,
                    //     rhs.table.schema[rhs.col].name,
                    //     !matches!(join.condition, Condition::Eq(_, _)),
                    //     (lhs_val == rhs_val),
                    //     !matches!(join.condition, Condition::Eq(_, _)) ^ (lhs_val == rhs_val)
                    // );
                    !matches!(join.condition, Condition::Eq(_, _)) ^ (lhs_val == rhs_val)
                })
                && row_cursor.iter().any(|row| row.row.is_some())
            {
                for rc in row_cursor.iter_mut() {
                    rc.shown = true;
                }
                for col in &cols {
                    let cell = col.get(&row_cursor);
                    if let Some(cell) = cell {
                        write!(out, "{cell},")?;
                    } else {
                        write!(out, ",")?;
                    }
                }
                writeln!(out, "")?;
            }

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
                    .find(|(_, s)| s.name == col.column)
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
                        .find(|(_, c)| c.name == lhs.column)
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
                        .find(|(_, c)| c.name == rhs.column)
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
