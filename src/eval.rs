use std::collections::HashMap;

use crate::select::{BinOp, Expr, QueryContext, RowCursor, UniOp};

#[derive(Clone, Debug)]
pub(crate) enum EvalError {
    ColNotFound(String),
    RowNotFound(usize),
    CursorNone(usize),
    /// When an aggregate function like count is called in scalar context
    AggregateCall(String),
    /// When a type coercion fails from the first type to the second
    Coerce(String, String),
}

impl std::fmt::Display for EvalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ColNotFound(col) => write!(f, "Column {col} not found"),
            Self::RowNotFound(row) => write!(f, "Row {row} is out of bound"),
            Self::CursorNone(table) => write!(f, "Table index {table} has None cursor"),
            Self::AggregateCall(name) => {
                write!(f, "Aggregate function {name} is called in scalar context")
            }
            Self::Coerce(from, to) => write!(f, "Coercion from {from} to {to}"),
        }
    }
}

impl std::error::Error for EvalError {}

pub(crate) fn eval_expr(
    expr: &Expr,
    cols: &[Expr],
    ctx: &QueryContext,
    row_cursor: &[RowCursor],
    aggregates: &AggregateResult,
) -> Result<String, EvalError> {
    match expr {
        Expr::Column(col) => {
            let col = ctx
                .find_col(col)
                .ok_or_else(|| EvalError::ColNotFound(col.column.clone()))?;
            col.get(&row_cursor).cloned()
        }
        Expr::ColIdx(i) => {
            let col = cols
                .get(
                    i.checked_sub(1)
                        .ok_or_else(|| EvalError::ColNotFound(format!("{i}")))?,
                )
                .ok_or_else(|| EvalError::ColNotFound(format!("{i}")))?;
            eval_expr(col, cols, ctx, row_cursor, aggregates)
        }
        Expr::StrLiteral(lit) => Ok(lit.clone()),
        Expr::Binary { op, lhs, rhs } => {
            let lhs = eval_expr(lhs, cols, ctx, row_cursor, aggregates)?;
            let rhs = eval_expr(rhs, cols, ctx, row_cursor, aggregates)?;
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
            let val = eval_expr(operand, cols, ctx, row_cursor, aggregates)?;
            let res = match op {
                UniOp::Not => !coerce_bool(&val),
            };
            Ok((if res { "1" } else { "0" }).to_string())
        }
        Expr::AggregateFn { name, .. } => aggregates
            .get(&(expr as *const _ as usize))
            .and_then(|entry| {
                Some(match name.to_ascii_lowercase().as_str() {
                    "count" => entry.count.to_string(),
                    "sum" => entry.sum.to_string(),
                    "avg" => (entry.sum / entry.count as f64).to_string(),
                    _ => return None,
                })
            })
            .ok_or_else(|| EvalError::AggregateCall(name.clone())),
    }
}

/// Mapping from node address to the amount
pub(crate) type AggregateResult = HashMap<usize, AggregateEntry>;

#[derive(Clone, Copy, Default)]
pub(crate) struct AggregateEntry {
    sum: f64,
    count: usize,
}

pub(crate) fn aggregate_expr(
    expr: &Expr,
    cols: &[Expr],
    ctx: &QueryContext,
    row_cursor: &[RowCursor],
    results: &mut AggregateResult,
) -> Result<String, Box<dyn std::error::Error>> {
    match expr {
        Expr::ColIdx(i) => {
            let col = cols
                .get(
                    i.checked_sub(1)
                        .ok_or_else(|| EvalError::ColNotFound(format!("{i}")))?,
                )
                .ok_or_else(|| EvalError::ColNotFound(format!("{i}")))?;
            if expr as *const _ == col as *const _ {
                return Err("Recurse".into());
            }
            aggregate_expr(col, cols, ctx, row_cursor, results)
        }
        Expr::Binary { op, lhs, rhs } => {
            let lhs = aggregate_expr(lhs, cols, ctx, row_cursor, results)?;
            let rhs = aggregate_expr(rhs, cols, ctx, row_cursor, results)?;
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
            let val = aggregate_expr(operand, cols, ctx, row_cursor, results)?;
            let res = match op {
                UniOp::Not => !coerce_bool(&val),
            };
            Ok((if res { "1" } else { "0" }).to_string())
        }
        Expr::AggregateFn { name, args } => match name.to_ascii_lowercase().as_str() {
            "count" => {
                let entry = results.entry(expr as *const _ as usize);
                let count = &mut entry.or_default().count;
                *count += 1;
                return Ok(count.to_string());
            }
            "sum" | "avg" => {
                let add = eval_expr(&args[0], cols, ctx, row_cursor, results).and_then(|val| {
                    val.parse::<f64>()
                        .map_err(|_| EvalError::Coerce("String".to_string(), "f64".to_string()))
                })?;
                let entry = results.entry(expr as *const _ as usize);
                let values = &mut entry.or_default();
                values.count += 1;
                values.sum += add;
                return Ok((values.sum / values.count as f64).to_string());
            }
            _ => return Err(format!("Unknown function {name}").into()),
        },
        _ => {
            return Ok(eval_expr(expr, cols, ctx, row_cursor, results)?);
        }
    }
}

pub(crate) fn find_aggregate_fn(expr: &Expr, ctx: &QueryContext) -> Option<usize> {
    match expr {
        Expr::AggregateFn { .. } => {
            return Some(expr as *const _ as usize);
        }
        Expr::Binary { lhs, rhs, .. } => {
            find_aggregate_fn(lhs, ctx).or_else(|| find_aggregate_fn(rhs, ctx))
        }
        Expr::Unary { operand, .. } => find_aggregate_fn(operand, ctx),
        _ => None,
    }
}

pub(crate) fn coerce_bool(val: &str) -> bool {
    val == "1" || val.eq_ignore_ascii_case("true")
}
