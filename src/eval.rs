use std::collections::HashMap;

use crate::select::{BinOp, ColSpecifier, Expr, QueryContext, RowCursor, UniOp};

#[derive(Clone, Debug)]
pub(crate) enum EvalError {
    ColNotFound(String),
    RowNotFound(usize),
    CursorNone(usize),
    /// When an aggregate function like count is called in scalar context
    AggregateCall(String),
    /// When a type coercion fails from the first type to the second
    Coerce(String, String),
    /// When an aggregate function does not allow wildcard arguments, e.g. not count(*)
    DisallowedWildcard(String),
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
            Self::DisallowedWildcard(name) => {
                write!(f, "{name} function cannot have wildcard argument")
            }
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
        Expr::AggregateFn { name, .. } => match name.to_ascii_lowercase().as_str() {
            "count" => aggregates
                .count
                .get(&(expr as *const _ as usize))
                .map(|v| v.to_string()),
            "sum" => aggregates
                .sum
                .get(&(expr as *const _ as usize))
                .map(|v| v.to_string()),
            "avg" => aggregates
                .avg
                .get(&(expr as *const _ as usize))
                .map(|entry| (entry.sum / entry.count as f64).to_string()),
            "min" => aggregates
                .min
                .get(&(expr as *const _ as usize))
                .map(|entry| entry.to_string()),
            "max" => aggregates
                .max
                .get(&(expr as *const _ as usize))
                .map(|entry| entry.to_string()),
            _ => return Err(EvalError::AggregateCall(name.clone())),
        }
        .ok_or_else(|| EvalError::AggregateCall(name.clone())),
    }
}

/// Mapping from node address to the accumulator of the aggregate function.
/// Since aggregate functions can appear multiple times in an expression, we cannot allocate a fixed size buffer for
/// accumulating them. So we use hash maps from a unique id of the AST node to the accumulator.
/// The unique id is the memory address, which means the AST is expected to keep an address.
#[derive(Debug, Default)]
pub(crate) struct AggregateResult {
    pub count: HashMap<usize, usize>,
    pub sum: HashMap<usize, f64>,
    pub avg: HashMap<usize, AggregateAvg>,
    pub min: HashMap<usize, f64>,
    pub max: HashMap<usize, f64>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct AggregateAvg {
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
    let eval_col_spec = |name: &str, col_spec: &ColSpecifier| {
        let ex = match col_spec {
            ColSpecifier::Expr(ex) => ex,
            ColSpecifier::Wildcard => {
                return Err(EvalError::DisallowedWildcard(name.to_string()));
            }
        };
        eval_expr(ex, cols, ctx, row_cursor, results).and_then(|val| {
            val.parse::<f64>()
                .map_err(|_| EvalError::Coerce("String".to_string(), "f64".to_string()))
        })
    };

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
                let entry = results.count.entry(expr as *const _ as usize);
                let count = entry.or_default();
                *count += 1;
                return Ok(count.to_string());
            }
            "sum" => {
                let val = eval_col_spec("sum", &args[0])?;
                let entry = results.sum.entry(expr as *const _ as usize);
                let values = entry.or_default();
                *values += val;
                return Ok(values.to_string());
            }
            "avg" => {
                let val = eval_col_spec("avg", &args[0])?;
                let entry = results.avg.entry(expr as *const _ as usize);
                let values = entry.or_default();
                values.count += 1;
                values.sum += val;
                return Ok((values.sum / values.count as f64).to_string());
            }
            "min" => {
                let val = eval_col_spec("min", &args[0])?;
                let entry = results.min.entry(expr as *const _ as usize);
                let values = entry.or_default();
                *values = values.min(val);
                return Ok(values.to_string());
            }
            "max" => {
                let val = eval_col_spec("max", &args[0])?;
                let entry = results.max.entry(expr as *const _ as usize);
                let values = entry.or_default();
                *values = values.max(val);
                return Ok(values.to_string());
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
