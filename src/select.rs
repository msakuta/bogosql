use std::{collections::HashMap, error::Error, io::Write};

use crate::{
    Database, Table,
    eval::{EvalError, coerce_bool, eval_expr},
};

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub cols: Vec<ColSpecifier>,
    pub table: TableSpecifier,
    pub join: Vec<JoinClause>,
    pub condition: Option<Expr>,
    pub ordering: Option<OrderBy>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableSpecifier {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColSpecifier {
    Wildcard,
    Name(Column),
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    pub kind: JoinKind,
    pub table: TableSpecifier,
    pub condition: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderBy {
    pub col: Column,
    pub ordering: Ordering,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ordering {
    Asc,
    Desc,
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

struct QueryContext<'a> {
    sql: &'a SelectStmt,
    tables: Vec<&'a Table>,
    aliases: HashMap<&'a String, usize>,
}

fn find_col<'a>(ctx: &QueryContext<'a>, column: &Column) -> Option<ColRef<'a>> {
    if let Some(ref table_name) = column.table {
        let (joindex, table) = ctx
            .aliases
            .get(table_name)
            .and_then(|i| Some((*i, *ctx.tables.get(*i)?)))
            .or_else(|| {
                ctx.tables
                    .iter()
                    .enumerate()
                    .find(|(_, t)| t.name == *table_name)
                    .map(|(i, t)| (i, *t))
            })?;
        return table
            .schema
            .iter()
            .enumerate()
            .find(|(_, s)| s.name == column.column)
            .map(|(i, _)| ColRef::new(table, joindex, i));
    }
    ctx.tables
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

fn extend_colspecs<'a>(
    ctx: &'a QueryContext<'a>,
    colspecs: &'a [ColSpecifier],
) -> Result<Vec<ColRef<'a>>, Box<dyn Error>> {
    let mut ret = vec![];
    for col_spec in colspecs {
        match col_spec {
            ColSpecifier::Wildcard => {
                for (i, table) in ctx.tables.iter().enumerate() {
                    for (j, _col) in table.schema.iter().enumerate() {
                        ret.push(ColRef {
                            table: *table,
                            joindex: i,
                            col: j,
                        });
                    }
                }
            }
            ColSpecifier::Name(col) => {
                let col = find_col(ctx, col).ok_or_else(|| format!("Column {} not found", col))?;
                ret.push(col);
            }
        }
    }
    Ok(ret)
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

pub trait QueryOutput {
    fn output(&mut self, row: &[String]) -> Result<(), Box<dyn Error>>;
}

pub struct CsvOutput(pub Vec<u8>);

impl QueryOutput for CsvOutput {
    fn output(&mut self, row: &[String]) -> Result<(), Box<dyn Error>> {
        for cell in row {
            write!(&mut self.0, "{},", cell)?;
        }
        writeln!(&mut self.0, "")?;
        Ok(())
    }
}

struct BufferOutput(Vec<Vec<String>>);

impl QueryOutput for BufferOutput {
    fn output(&mut self, row: &[String]) -> Result<(), Box<dyn Error>> {
        self.0.push(row.to_vec());
        Ok(())
    }
}

pub fn exec_select(
    out: &mut impl QueryOutput,
    db: &Database,
    sql: &SelectStmt,
) -> Result<(), Box<dyn Error>> {
    let Some(table) = db.get(&sql.table.name) else {
        return Err(format!("Table {} not found", sql.table.name).into());
    };

    let mut aliases = HashMap::new();
    if let Some(ref alias) = sql.table.alias {
        aliases.insert(alias, 0);
    }

    let joined_tables = std::iter::once(Ok((table, &sql.table.alias)))
        .chain(sql.join.iter().map(|join| {
            Ok((
                db.get(&join.table.name)
                    .ok_or_else(|| format!("Table {} not found", join.table.name))?,
                &join.table.alias,
            ))
        }))
        .enumerate()
        .map(|(i, join)| match join {
            Ok((table, alias)) => {
                if let Some(alias) = alias {
                    aliases.insert(alias, i);
                }
                Ok(table)
            }
            Err(e) => Err(e),
        })
        .collect::<Result<Vec<_>, String>>()?;

    let ctx = QueryContext {
        sql,
        tables: joined_tables,
        aliases,
    };

    if let Some(ref order_by) = sql.ordering {
        let mut buf = BufferOutput(vec![]);
        let mut subsql = sql.clone();
        let mut cols = extend_colspecs(&ctx, &sql.cols)?;
        let col_ref = find_col(&ctx, &order_by.col)
            .ok_or_else(|| format!("Column {} not found in ORDER BY clause", order_by.col))?;
        let col_idx = cols.len();
        cols.push(col_ref);
        subsql.ordering = None;
        exec_select_sub(&mut buf, &ctx, &cols)?;

        buf.0.sort_by(move |lhs, rhs| {
            let res = lhs[col_idx].cmp(&rhs[col_idx]);
            if order_by.ordering == Ordering::Desc {
                res.reverse()
            } else {
                res
            }
        });

        for row in buf.0 {
            out.output(&row[..row.len() - 1])?;
        }

        return Ok(());
    }

    let cols = extend_colspecs(&ctx, &ctx.sql.cols)?;

    exec_select_sub(out, &ctx, &cols)
}

fn exec_select_sub(
    out: &mut impl QueryOutput,
    ctx: &QueryContext,
    cols: &[ColRef],
) -> Result<(), Box<dyn Error>> {
    let join_allow_none = std::iter::once(false)
        .chain(
            ctx.sql
                .join
                .iter()
                .map(|join| matches!(join.kind, JoinKind::Left)),
        )
        .collect::<Vec<_>>();

    let row_counts = ctx
        .tables
        .iter()
        .map(|table| table.data.len() / table.schema.len())
        .collect::<Vec<_>>();
    let mut row_cursor = vec![RowCursor::new(); ctx.tables.len()];

    let has_left_join = join_allow_none.iter().any(|a| *a);

    loop {
        let bi = row_cursor
            .iter()
            .zip(join_allow_none.iter())
            .map(|(r, a)| (r.row.is_none(), *a, !r.shown))
            .collect::<Vec<_>>();
        dbg!(&row_cursor, &bi);
        if ctx.sql.join.iter().all(|join| {
            let val = eval_expr(&join.condition, &ctx.tables, &row_cursor);
            match val {
                Ok(val) => coerce_bool(&val),
                Err(EvalError::CursorNone(table_idx)) => {
                    dbg!(join_allow_none[table_idx]) && !row_cursor[table_idx].shown
                }
                _ => false,
            }
        }) || has_left_join
            && row_cursor.iter().zip(join_allow_none.iter()).all(|(r, a)| {
                if *a {
                    r.row.is_none() && !r.shown
                } else {
                    r.row.is_some()
                }
            })
        {
            for rc in row_cursor.iter_mut() {
                rc.shown = true;
            }
            let values = cols
                .iter()
                .map(|c| {
                    c.get(&row_cursor)
                        .cloned()
                        .unwrap_or_else(|| "".to_string())
                })
                .collect::<Vec<_>>();
            out.output(&values)?;
        }

        if !incr_row_cursor(&mut row_cursor, &row_counts) {
            break;
        }
    }

    Ok(())
}
