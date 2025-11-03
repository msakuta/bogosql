use crate::select::{BinOp, Expr, QueryContext, RowCursor, UniOp};

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
    cols: &[Expr],
    ctx: &QueryContext,
    row_cursor: &[RowCursor],
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
            eval_expr(col, cols, ctx, row_cursor)
        }
        Expr::StrLiteral(lit) => Ok(lit.clone()),
        Expr::Binary { op, lhs, rhs } => {
            let lhs = eval_expr(lhs, cols, ctx, row_cursor)?;
            let rhs = eval_expr(rhs, cols, ctx, row_cursor)?;
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
            let val = eval_expr(operand, cols, ctx, row_cursor)?;
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
