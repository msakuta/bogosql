use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{alpha1, alphanumeric1, multispace0, none_of},
    combinator::{opt, recognize},
    multi::{fold_many0, many0},
    sequence::{delimited, pair},
};

use crate::{
    Statement,
    select::{Cols, Condition, JoinClause, Term},
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

fn from_table(i: &str) -> IResult<&str, String> {
    let (r, _) = delimited(multispace0, tag_no_case("FROM"), multispace0).parse(i)?;

    let (r, table) = token(r)?;

    Ok((r, table.to_string()))
}

fn join(i: &str) -> IResult<&str, JoinClause> {
    let (r, _) = delimited(multispace0, tag_no_case("INNER JOIN"), multispace0).parse(i)?;

    let (r, table) = ident(r)?;

    let (r, _) = delimited(multispace0, tag_no_case("ON"), multispace0).parse(r)?;

    let (r, condition) = conditional(r)?;

    Ok((r, JoinClause { table, condition }))
}

fn where_clause(i: &str) -> IResult<&str, Condition> {
    let (r, _) = delimited(multispace0, tag_no_case("WHERE"), multispace0).parse(i)?;

    conditional(r)
}

fn conditional(i: &str) -> IResult<&str, Condition> {
    let (r, lhs) = term(i)?;

    let (r, op) = delimited(multispace0, alt((tag("="), tag("<>"))), multispace0).parse(r)?;

    let (r, rhs) = term(r)?;

    Ok((
        r,
        match op {
            "=" => Condition::Eq(lhs, rhs),
            "<>" => Condition::Ne(lhs, rhs),
            _ => unreachable!(),
        },
    ))
}

fn term(i: &str) -> IResult<&str, Term> {
    if let Ok((r, lit)) = str_literal(i) {
        return Ok((r, Term::StrLiteral(lit)));
    }

    if let Ok((r, id)) = ident(i) {
        return Ok((r, Term::Column(id)));
    }

    Err(nom::Err::Error(nom::error::Error::new(
        i,
        nom::error::ErrorKind::Verify,
    )))
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
    let (r, first) = ident(i)?;
    let (r, res) = fold_many0(
        pair(delimited(multispace0, tag(","), multispace0), ident),
        move || vec![first.to_string()],
        |mut acc, (_, token)| {
            acc.push(token.to_string());
            acc
        },
    )
    .parse(r)?;
    Ok((r, Cols::List(res)))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{SelectStmt, Statement};

    #[test]
    fn test_select() {
        let src = "SELECT id, data FROM table";
        assert_eq!(token(src).unwrap().1, "SELECT");
        assert_eq!(
            statement(src).unwrap().1,
            Statement::Select(SelectStmt {
                cols: Cols::List(vec!["id".to_string(), "data".to_string()]),
                table: "table".to_string(),
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
                cols: Cols::List(vec!["id".to_string(), "data".to_string()]),
                table: "table".to_string(),
                join: vec![JoinClause {
                    table: "table2".to_string(),
                    condition: Condition::Eq(
                        Term::Column("id".to_string()),
                        Term::Column("id2".to_string())
                    ),
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
}
