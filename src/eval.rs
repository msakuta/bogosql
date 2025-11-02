use crate::{
    Table,
    select::{BinOp, Expr, RowCursor, UniOp},
};

#[derive(Clone, Debug)]
pub(crate) enum EvalError {
    ColNotFound(String),
    RowNotFound(usize),
    CursorNone(usize),
}

impl std::fmt::Display for EvalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ColNotFound(col) => write!(f, "Column {col} not found"),
            Self::RowNotFound(row) => write!(f, "Row {row} is out of bound"),
            Self::CursorNone(table) => write!(f, "Table index {table} has None cursor"),
        }
    }
}

impl std::error::Error for EvalError {}

pub(crate) fn eval_expr(
    expr: &Expr,
    tables: &[&Table],
    row_cursor: &[RowCursor],
) -> Result<String, EvalError> {
    match expr {
        Expr::Column(col) => {
            if let Some((table_idx, table)) = col.table.as_ref().and_then(|table_name| {
                tables
                    .iter()
                    .enumerate()
                    .find(|(_, t)| t.name == *table_name)
            }) {
                let row = row_cursor[table_idx]
                    .row
                    .ok_or_else(|| EvalError::CursorNone(table_idx))?;

                table
                    .schema
                    .iter()
                    .enumerate()
                    .find_map(|(i, c)| {
                        if c.name == col.column {
                            Some(
                                table
                                    .get(row, i)
                                    .cloned()
                                    .ok_or_else(|| EvalError::RowNotFound(i)),
                            )
                        } else {
                            None
                        }
                    })
                    .ok_or_else(|| EvalError::ColNotFound(col.column.clone()))?
            } else {
                tables
                    .iter()
                    .enumerate()
                    .find_map(|(table_idx, t)| {
                        t.schema.iter().enumerate().find_map(|(i, c)| {
                            if c.name == col.column {
                                Some(
                                    row_cursor[table_idx]
                                        .row
                                        .and_then(|r| t.get(r, i))
                                        .cloned()
                                        .ok_or_else(|| EvalError::RowNotFound(i)),
                                )
                            } else {
                                None
                            }
                        })
                    })
                    .ok_or_else(|| EvalError::ColNotFound(col.column.clone()))?
            }
        }
        Expr::StrLiteral(lit) => Ok(lit.clone()),
        Expr::Binary { op, lhs, rhs } => {
            let lhs = eval_expr(lhs, tables, row_cursor)?;
            let rhs = eval_expr(rhs, tables, row_cursor)?;
            let res = match op {
                BinOp::Eq => lhs == rhs,
                BinOp::Ne => lhs != rhs,
                BinOp::Lt => lhs < rhs,
                BinOp::Gt => lhs > rhs,
                BinOp::Le => lhs <= rhs,
                BinOp::Ge => lhs >= rhs,
                BinOp::And => coerce_bool(&lhs) && coerce_bool(&rhs),
                BinOp::Or => coerce_bool(&lhs) || coerce_bool(&rhs),
            };
            Ok((if res { "1" } else { "0" }).to_string())
        }
        Expr::Unary { op, operand } => {
            let val = eval_expr(operand, tables, row_cursor)?;
            let res = match op {
                UniOp::Not => !coerce_bool(&val),
            };
            Ok((if res { "1" } else { "0" }).to_string())
        }
    }
}

pub(crate) fn coerce_bool(val: &str) -> bool {
    val == "1" || val.eq_ignore_ascii_case("true")
}
