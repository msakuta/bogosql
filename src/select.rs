use std::{collections::HashMap, error::Error, io::Write};

use crate::{
    Table,
    db::Database,
    eval::{AggregateResult, EvalError, aggregate_expr, coerce_bool, eval_expr, find_aggregate_fn},
};

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub cols: Vec<ColSpecifier>,
    pub table: TableSpecifier,
    pub join: Vec<JoinClause>,
    pub condition: Option<Expr>,
    pub ordering: Option<OrderBy>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableSpecifier {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColSpecifier {
    Wildcard,
    Expr(Expr),
}

impl std::fmt::Display for ColSpecifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Expr(ex) => ex.fmt(f),
            Self::Wildcard => write!(f, "*"),
        }
    }
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
    pub expr: Expr,
    pub ordering: Ordering,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ordering {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
    And,
    Or,
}

impl std::fmt::Display for BinOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Add => f.write_str("+"),
            Self::Sub => f.write_str("-"),
            Self::Mul => f.write_str("*"),
            Self::Div => f.write_str("/"),
            Self::Eq => f.write_str("="),
            Self::Ne => f.write_str("<>"),
            Self::Lt => f.write_str("<"),
            Self::Gt => f.write_str(">"),
            Self::Le => f.write_str("<="),
            Self::Ge => f.write_str(">="),
            Self::And => f.write_str("AND"),
            Self::Or => f.write_str("OR"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UniOp {
    Not,
}

impl std::fmt::Display for UniOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Not => f.write_str("NOT"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    Column(Column),
    ColIdx(usize),
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
    AggregateFn {
        name: String,
        args: Vec<ColSpecifier>,
    },
}

impl std::fmt::Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Column(col) => f.write_str(&col.column),
            Self::ColIdx(idx) => write!(f, "{idx}"),
            Self::StrLiteral(lit) => write!(f, "'{lit}'"),
            Self::Binary { op, lhs, rhs } => {
                write!(f, "({lhs} {op} {rhs})")
            }
            Self::Unary { op, operand } => {
                write!(f, "{op} {operand}")
            }
            Self::AggregateFn { name, args } => {
                write!(f, "{name}(")?;
                for (i, arg) in args.iter().enumerate() {
                    if i != 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{arg}")?;
                }
                write!(f, ")")?;
                Ok(())
            }
        }
    }
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
pub(crate) struct ColRef<'a> {
    table: &'a Table,
    /// If the query has joins, this index is incremented for each join.
    pub joindex: usize,
    col: usize,
}

impl<'a> ColRef<'a> {
    pub fn new(table: &'a Table, joindex: usize, col: usize) -> Self {
        Self {
            table,
            joindex,
            col,
        }
    }

    pub fn get(&self, row_indices: &[RowCursor]) -> Result<&String, EvalError> {
        let row = row_indices
            .get(self.joindex)
            .ok_or_else(|| EvalError::ColNotFound(self.joindex.to_string()))?
            .row
            .ok_or_else(|| EvalError::CursorNone(self.joindex))?;
        self.table
            .get(row, self.col)
            .ok_or_else(|| EvalError::RowNotFound(row))
    }
}

#[derive(Clone)]
pub(crate) struct QueryContext<'a> {
    sql: &'a SelectStmt,
    tables: Vec<&'a Table>,
    aliases: HashMap<&'a String, usize>,
}

impl<'a> QueryContext<'a> {
    pub fn find_col(&self, column: &Column) -> Option<ColRef<'a>> {
        if let Some(ref table_name) = column.table {
            let (joindex, table) = self
                .aliases
                .get(table_name)
                .and_then(|i| Some((*i, *self.tables.get(*i)?)))
                .or_else(|| {
                    self.tables
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
        self.tables
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
}

fn extend_colspecs<'a>(
    ctx: &'a QueryContext<'a>,
    colspecs: &'a [ColSpecifier],
) -> Result<(Vec<Expr>, Vec<String>), Box<dyn Error>> {
    let mut exprs = vec![];
    let mut header = vec![];
    for col_spec in colspecs {
        match col_spec {
            ColSpecifier::Wildcard => {
                for (_i, table) in ctx.tables.iter().enumerate() {
                    for (_j, col) in table.schema.iter().enumerate() {
                        exprs.push(Expr::Column(Column {
                            table: Some(table.name.clone()),
                            column: col.name.to_string(),
                        }));
                        header.push(col.name.clone());
                    }
                }
            }
            ColSpecifier::Expr(expr) => {
                exprs.push(expr.clone());
                header.push(expr.to_string());
            }
        }
    }
    Ok((exprs, header))
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

#[derive(Default, Debug)]
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

#[derive(Default, Debug)]
struct BufferOutput(Vec<Vec<String>>);

impl BufferOutput {
    fn format(&self, f: &mut impl Write) -> std::io::Result<()> {
        let num_cols = self.0.first().map_or(0, |header| header.len());
        let col_widths: Vec<_> = (0..num_cols)
            .map(|col| self.0.iter().map(|row| row[col].len()).max().unwrap_or(0))
            .collect();
        if let Some(header) = self.0.first() {
            for (i, cell) in header.iter().enumerate() {
                write!(f, "{:width$}", cell, width = col_widths[i])?;
                if i != header.len() - 1 {
                    write!(f, " | ")?;
                }
            }
            writeln!(f, "")?;
            for (i, col_width) in col_widths.iter().enumerate() {
                for _ in 0..col_width + 1 {
                    write!(f, "-")?;
                }
                if i != col_widths.len() - 1 {
                    write!(f, "+-")?;
                }
            }
            writeln!(f, "")?;
        }
        for row in &self.0[1..] {
            for (i, cell) in row.iter().enumerate() {
                write!(f, "{:width$}", cell, width = col_widths[i])?;
                if i != row.len() - 1 {
                    write!(f, " | ")?;
                }
            }
            writeln!(f, "")?;
        }
        Ok(())
    }
}

impl QueryOutput for BufferOutput {
    fn output(&mut self, row: &[String]) -> Result<(), Box<dyn Error>> {
        self.0.push(row.to_vec());
        Ok(())
    }
}

pub fn format_select(
    out: &mut impl Write,
    db: &Database,
    sql: &SelectStmt,
) -> Result<(), Box<dyn Error>> {
    let mut buf = BufferOutput::default();
    exec_select(&mut buf, db, sql)?;
    buf.format(out)?;
    Ok(())
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
        let (mut cols, names) = extend_colspecs(&ctx, &sql.cols)?;

        out.output(&names)?;

        let col_idx = cols.len();
        cols.push(order_by.expr.clone());
        subsql.ordering = None;
        subsql.limit = None;
        subsql.offset = None;
        let subctx = QueryContext {
            sql: &subsql,
            ..ctx.clone()
        };
        exec_select_sub(&mut buf, &subctx, &cols)?;

        buf.0.sort_by(move |lhs, rhs| {
            let res = lhs[col_idx].cmp(&rhs[col_idx]);
            if order_by.ordering == Ordering::Desc {
                res.reverse()
            } else {
                res
            }
        });

        if let Some(limit) = sql.limit {
            let offset = sql.offset.unwrap_or(0);
            for row in buf.0.iter().skip(offset).take(limit) {
                out.output(&row[..row.len() - 1])?;
            }
        } else {
            for row in buf.0 {
                out.output(&row[..row.len() - 1])?;
            }
        }

        return Ok(());
    }

    let (cols, names) = extend_colspecs(&ctx, &ctx.sql.cols)?;

    out.output(&names)?;

    exec_select_sub(out, &ctx, &cols)
}

fn exec_select_sub(
    out: &mut impl QueryOutput,
    ctx: &QueryContext,
    cols: &[Expr],
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

    // Returns whether to print the row. Most of the combinations in a join is typically filtered out.
    let check_print = |row_cursor: &[RowCursor]| -> Result<bool, Box<dyn Error>> {
        let join_cond = if ctx.sql.join.is_empty() {
            row_cursor.iter().all(|r| r.row.is_some())
        } else {
            ctx.sql.join.iter().all(|join| {
                let val = eval_expr(
                    &join.condition,
                    &cols,
                    ctx,
                    &row_cursor,
                    &AggregateResult::default(),
                );
                match val {
                    Ok(val) => coerce_bool(&val),
                    Err(EvalError::CursorNone(table_idx)) => {
                        join_allow_none[table_idx] && !row_cursor[table_idx].shown
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
        };
        let res = join_cond
            && ctx
                .sql
                .condition
                .as_ref()
                .map_or(Ok::<bool, Box<dyn Error>>(true), |cond| {
                    Ok(coerce_bool(&eval_expr(
                        cond,
                        cols,
                        ctx,
                        row_cursor,
                        &AggregateResult::default(),
                    )?))
                })?;
        Ok(res)
    };

    if let Some(_addr) = cols.iter().find_map(|col| find_aggregate_fn(col, ctx)) {
        let mut results = AggregateResult::default();
        loop {
            for col in cols {
                if check_print(&row_cursor)? {
                    let _ = aggregate_expr(col, cols, ctx, &row_cursor, &mut results)
                        .inspect_err(|e| println!("Error from aggregate_expr: {e}"))?;
                }
            }
            if !incr_row_cursor(&mut row_cursor, &row_counts)
                || row_cursor.iter().any(|c| c.row.is_none())
            {
                break;
            }
        }
        let values = cols
            .iter()
            .map(|ex| match eval_expr(ex, cols, ctx, &row_cursor, &results) {
                Ok(res) => Ok(res),
                Err(EvalError::CursorNone(_)) => Ok("".to_string()),
                Err(e) => Err(e),
            })
            .collect::<Result<Vec<_>, _>>()
            .inspect_err(|e| println!("Cell eval error: {e}"))?;
        out.output(&values)?;
        return Ok(());
    }

    let offset = ctx.sql.offset.unwrap_or(0);
    let mut printed_rows = 0;
    loop {
        if let Some(limit) = ctx.sql.limit
            && offset + limit <= printed_rows
        {
            break;
        }
        if check_print(&row_cursor)? {
            for rc in row_cursor.iter_mut() {
                rc.shown = true;
            }
            let aggregates = AggregateResult::default();
            let values = cols
                .iter()
                .map(
                    |ex| match eval_expr(ex, cols, ctx, &row_cursor, &aggregates) {
                        Ok(res) => Ok(res),
                        Err(EvalError::CursorNone(_)) => Ok("".to_string()),
                        Err(e) => Err(e),
                    },
                )
                .collect::<Result<Vec<_>, _>>()
                .inspect_err(|e| println!("Cell eval error: {e}"))?;
            if offset <= printed_rows {
                out.output(&values)?;
            }
            printed_rows += 1;
        }

        if !incr_row_cursor(&mut row_cursor, &row_counts) {
            break;
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::*;
    use crate::{Statement, make_table, parser::statement};
    use nom::Finish;

    #[test]
    fn test_sql() {
        let csv = r#"id,name
1, a
2, b
3, c
"#;
        let mut db = HashMap::new();
        let table_name = "t".to_string();
        let table = make_table(&table_name, csv).unwrap();
        db.insert(table_name.clone(), table);
        let sql = "SELECT * FROM t";
        let (_, stmt) = statement(&sql).finish().unwrap();
        let mut buf = BufferOutput(vec![]);
        match stmt {
            Statement::Select(stmt) => exec_select(&mut buf, &db, &stmt).unwrap(),
        }
        assert_eq!(
            buf.0,
            vec![
                vec!["id", "name"],
                vec!["1", "a"],
                vec!["2", "b"],
                vec!["3", "c"]
            ]
        )
    }

    #[test]
    fn test_where() {
        let csv = r#"id,name
1, a
2, b
3, c
"#;
        let mut db = HashMap::new();
        let table_name = "t".to_string();
        let table = make_table(&table_name, csv).unwrap();
        db.insert(table_name.clone(), table);
        let sql = "SELECT * FROM t WHERE id = '1'";
        let (_, stmt) = statement(&sql).finish().unwrap();
        let mut buf = BufferOutput(vec![]);
        match stmt {
            Statement::Select(stmt) => exec_select(&mut buf, &db, &stmt).unwrap(),
        }
    }
}
