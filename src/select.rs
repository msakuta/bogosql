use std::{error::Error, io::Write};

use crate::{
    Database, Table,
    eval::{EvalError, coerce_bool, eval_expr},
};

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub cols: Cols,
    pub table: String,
    pub join: Vec<JoinClause>,
    pub condition: Option<Expr>,
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
    pub condition: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BinOp {
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
    And,
    Or,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UniOp {
    Not,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    Column(Column),
    StrLiteral(String),
    Binary {
        op: BinOp,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
    },
    Unary {
        op: UniOp,
        operand: Box<Expr>,
    },
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
pub(crate) struct RowCursor {
    pub row: Option<usize>,
    pub shown: bool,
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
            if sql.join.iter().all(|join| {
                let val = eval_expr(&join.condition, &joined_tables, &row_cursor);
                match val {
                    Ok(val) => val == "1" || val.eq_ignore_ascii_case("true"),
                    Err(EvalError::CursorNone(table_idx)) => {
                        join_allow_none[table_idx] && !row_cursor[table_idx].shown
                    }
                    _ => false,
                }
            }) && row_cursor.iter().any(|row| row.row.is_some())
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
            let val = eval_expr(
                cond,
                &[table],
                &[RowCursor {
                    row: Some(row),
                    shown: false,
                }],
            )?;
            if !coerce_bool(&val) {
                continue;
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
