use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{alpha1, alphanumeric1, multispace0, multispace1, none_of},
    combinator::{opt, recognize},
    multi::{fold_many0, many0},
    sequence::{delimited, pair},
};

use crate::{
    Statement,
    select::{BinOp, Cols, Column, Expr, JoinClause, JoinKind, TableSpecifier, UniOp},
};

pub(crate) fn token(i: &str) -> IResult<&str, &str> {
    delimited(
        multispace0,
        recognize(pair(
            alt((alpha1, tag("_"))),
            many0(alt((alphanumeric1, tag("_")))),
        )),
        multispace0,
    )
    .parse(i)
}

pub fn statement(i: &str) -> IResult<&str, Statement> {
    let (r, directive) = token(i)?;
    let (r, stmt) = match directive.to_lowercase().as_str() {
        "select" => {
            let (r, cols) = alt((columns_wildcard, columns)).parse(r)?;

            let (r, table) = from_table(r)?;

            let (r, join) = many0(join).parse(r)?;

            let (r, condition) = opt(where_clause).parse(r)?;

            (
                r,
                Statement::Select(crate::SelectStmt {
                    cols,
                    table,
                    join,
                    condition,
                }),
            )
        }
        _ => {
            return Err(nom::Err::Error(nom::error::Error::new(
                r,
                nom::error::ErrorKind::Verify,
            )));
        }
    };

    Ok((r, stmt))
}

fn from_table(i: &str) -> IResult<&str, TableSpecifier> {
    let (r, _) = delimited(multispace0, tag_no_case("FROM"), multispace0).parse(i)?;
    table_specifier(r)
}

fn table_specifier(r: &str) -> IResult<&str, TableSpecifier> {
    let (r, table) = token(r)?;

    let (r, alias) = opt(pair(
        delimited(multispace0, tag_no_case("AS"), multispace1),
        token,
    ))
    .parse(r)?;

    Ok((
        r,
        TableSpecifier {
            name: table.to_string(),
            alias: alias.map(|(_, name)| name.to_string()),
        },
    ))
}

fn join(i: &str) -> IResult<&str, JoinClause> {
    let (r, kind) = delimited(
        multispace0,
        alt((tag_no_case("INNER"), tag_no_case("LEFT"))),
        multispace1,
    )
    .parse(i)?;

    let (r, _) = delimited(multispace0, tag_no_case("JOIN"), multispace0).parse(r)?;

    let (r, table) = table_specifier(r)?;

    let (r, _) = delimited(multispace0, tag_no_case("ON"), multispace0).parse(r)?;

    let (r, condition) = expression(r)?;

    Ok((
        r,
        JoinClause {
            kind: if kind.eq_ignore_ascii_case("INNER") {
                JoinKind::Inner
            } else {
                JoinKind::Left
            },
            table,
            condition,
        },
    ))
}

fn where_clause(i: &str) -> IResult<&str, Expr> {
    let (r, _) = delimited(multispace0, tag_no_case("WHERE"), multispace0).parse(i)?;

    expression(r)
}

fn expression(i: &str) -> IResult<&str, Expr> {
    logical_ex(i)
}

fn logical_ex(i: &str) -> IResult<&str, Expr> {
    let (r, lhs) = comparison_ex(i)?;

    let (r, res) = fold_many0(
        pair(
            delimited(
                multispace0,
                alt((tag_no_case("AND"), tag_no_case("OR"))),
                multispace0,
            ),
            comparison_ex,
        ),
        move || lhs.clone(),
        |acc, (op, sub_ex)| Expr::Binary {
            op: if op.eq_ignore_ascii_case("AND") {
                BinOp::And
            } else {
                BinOp::Or
            },
            lhs: Box::new(acc),
            rhs: Box::new(sub_ex),
        },
    )
    .parse(r)?;

    Ok((r, res))
}

fn comparison_op(i: &str) -> IResult<&str, BinOp> {
    let (r, op) = delimited(
        multispace0,
        alt((
            // The order matters!
            tag("<="),
            tag(">="),
            tag("<>"),
            tag("="),
            tag("<"),
            tag(">"),
        )),
        multispace0,
    )
    .parse(i)?;

    Ok((
        r,
        match op {
            "=" => BinOp::Eq,
            "<>" => BinOp::Ne,
            "<" => BinOp::Lt,
            ">" => BinOp::Gt,
            "<=" => BinOp::Le,
            ">=" => BinOp::Ge,
            _ => unreachable!(),
        },
    ))
}

fn comparison_ex(i: &str) -> IResult<&str, Expr> {
    let (r, lhs) = term(i)?;

    let Ok((r, op)) = comparison_op(r) else {
        return Ok((r, lhs));
    };

    let (r, rhs) = term(r)?;

    Ok((
        r,
        Expr::Binary {
            op,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
        },
    ))
}

fn not(i: &str) -> IResult<&str, Expr> {
    let (r, _) = delimited(multispace0, tag_no_case("NOT"), multispace0).parse(i)?;

    let (r, res) = comparison_ex(r)?;

    Ok((
        r,
        Expr::Unary {
            op: UniOp::Not,
            operand: Box::new(res),
        },
    ))
}

fn term(i: &str) -> IResult<&str, Expr> {
    if let Ok((r, res)) = not(i) {
        return Ok((r, res));
    }

    if let Ok((r, res)) = parentheses(i) {
        return Ok((r, res));
    }

    if let Ok((r, lit)) = str_literal(i) {
        return Ok((r, Expr::StrLiteral(lit)));
    }

    if let Ok((r, col)) = column(i) {
        return Ok((r, Expr::Column(col)));
    }

    Err(nom::Err::Error(nom::error::Error::new(
        i,
        nom::error::ErrorKind::Verify,
    )))
}

fn parentheses(i: &str) -> IResult<&str, Expr> {
    let (r, _) = delimited(multispace0, tag("("), multispace0).parse(i)?;
    let (r, res) = expression(r)?;
    let (r, _) = delimited(multispace0, tag(")"), multispace0).parse(r)?;
    Ok((r, res))
}

fn str_literal(i: &str) -> IResult<&str, String> {
    let (r, _) = pair(multispace0, tag("'")).parse(i)?;

    let (r, s) = recognize(many0(none_of("'"))).parse(r)?;

    let (r, _) = tag("'").parse(r)?;

    Ok((r, s.to_string()))
}

fn ident(i: &str) -> IResult<&str, String> {
    let (r, id) = token(i)?;

    if id == "FROM" {
        return Err(nom::Err::Error(nom::error::Error::new(
            i,
            nom::error::ErrorKind::Verify,
        )));
    }

    Ok((r, id.to_string()))
}

fn columns_wildcard(i: &str) -> IResult<&str, Cols> {
    let (r, _) = delimited(multispace0, tag("*"), multispace0).parse(i)?;
    Ok((r, Cols::Wildcard))
}

fn columns(i: &str) -> IResult<&str, Cols> {
    let (r, first) = column(i)?;
    let (r, res) = fold_many0(
        pair(delimited(multispace0, tag(","), multispace0), column),
        move || vec![first.clone()],
        |mut acc, (_, token)| {
            acc.push(token);
            acc
        },
    )
    .parse(r)?;
    Ok((r, Cols::List(res)))
}

fn column(i: &str) -> IResult<&str, Column> {
    let (r, table) = opt(pair(ident, delimited(multispace0, tag("."), multispace0))).parse(i)?;
    let (r, column) = ident(r)?;
    Ok((
        r,
        Column {
            table: table.map(|(table, _)| table),
            column,
        },
    ))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{SelectStmt, Statement, select::TableSpecifier};

    impl TableSpecifier {
        pub fn new(name: impl Into<String>) -> Self {
            Self {
                name: name.into(),
                alias: None,
            }
        }

        pub fn new_with_alias(name: impl Into<String>, alias: impl Into<String>) -> Self {
            Self {
                name: name.into(),
                alias: Some(alias.into()),
            }
        }
    }

    #[test]
    fn test_select() {
        let src = "SELECT id, data FROM table";
        assert_eq!(token(src).unwrap().1, "SELECT");
        assert_eq!(
            statement(src).unwrap().1,
            Statement::Select(SelectStmt {
                cols: Cols::List(vec![Column::new("id"), Column::new("data")]),
                table: TableSpecifier::new("table"),
                join: vec![],
                condition: None,
            })
        );
    }

    #[test]
    fn test_alias() {
        let src = "SELECT * FROM table AS t";
        assert_eq!(
            statement(src).unwrap().1,
            Statement::Select(SelectStmt {
                cols: Cols::Wildcard,
                table: TableSpecifier::new_with_alias("table", "t"),
                join: vec![],
                condition: None,
            })
        );
    }

    #[test]
    fn test_select_join() {
        let src = "SELECT id, data FROM table INNER JOIN table2 ON id = id2";
        assert_eq!(token(src).unwrap().1, "SELECT");
        assert_eq!(
            statement(src).unwrap().1,
            Statement::Select(SelectStmt {
                cols: Cols::List(vec![Column::new("id"), Column::new("data")]),
                table: TableSpecifier::new("table"),
                join: vec![JoinClause {
                    table: TableSpecifier::new("table2"),
                    kind: JoinKind::Inner,
                    condition: Expr::Binary {
                        op: BinOp::Eq,
                        lhs: Box::new(Expr::Column(Column::new("id"))),
                        rhs: Box::new(Expr::Column(Column::new("id2")))
                    },
                }],
                condition: None,
            })
        );
    }

    #[test]
    fn test_err() {
        let src = "SELOCT id, data FROM table";
        assert_eq!(
            statement(src),
            Err(nom::Err::Error(nom::error::Error {
                input: "id, data FROM table",
                code: nom::error::ErrorKind::Verify
            }))
        );
    }

    #[test]
    fn test_logical() {
        let src = "'1' AND '2' < '3'";
        assert_eq!(
            expression(src),
            Ok((
                "",
                Expr::Binary {
                    op: BinOp::And,
                    lhs: Box::new(Expr::StrLiteral("1".to_string())),
                    rhs: Box::new(Expr::Binary {
                        op: BinOp::Lt,
                        lhs: Box::new(Expr::StrLiteral("2".to_string())),
                        rhs: Box::new(Expr::StrLiteral("3".to_string())),
                    })
                }
            ))
        );

        let src = "'1' < '2' OR '3'";
        assert_eq!(
            expression(src),
            Ok((
                "",
                Expr::Binary {
                    op: BinOp::Or,
                    lhs: Box::new(Expr::Binary {
                        op: BinOp::Lt,
                        lhs: Box::new(Expr::StrLiteral("1".to_string())),
                        rhs: Box::new(Expr::StrLiteral("2".to_string())),
                    }),
                    rhs: Box::new(Expr::StrLiteral("3".to_string())),
                }
            ))
        );
    }

    #[test]
    fn test_not() {
        let src = "NOT '1'";
        assert_eq!(
            expression(src),
            Ok((
                "",
                Expr::Unary {
                    op: UniOp::Not,
                    operand: Box::new(Expr::StrLiteral("1".to_string()))
                }
            ))
        );
    }

    #[test]
    fn test_paren() {
        let src = "('1' AND '2') <= '3'";
        assert_eq!(
            expression(src),
            Ok((
                "",
                Expr::Binary {
                    op: BinOp::Le,
                    lhs: Box::new(Expr::Binary {
                        op: BinOp::And,
                        lhs: Box::new(Expr::StrLiteral("1".to_string())),
                        rhs: Box::new(Expr::StrLiteral("2".to_string())),
                    }),
                    rhs: Box::new(Expr::StrLiteral("3".to_string())),
                }
            ))
        );

        let src = "'1' > ('2' AND '3')";
        assert_eq!(
            expression(src),
            Ok((
                "",
                Expr::Binary {
                    op: BinOp::Gt,
                    lhs: Box::new(Expr::StrLiteral("1".to_string())),
                    rhs: Box::new(Expr::Binary {
                        op: BinOp::And,
                        lhs: Box::new(Expr::StrLiteral("2".to_string())),
                        rhs: Box::new(Expr::StrLiteral("3".to_string())),
                    }),
                }
            ))
        );
    }
}
