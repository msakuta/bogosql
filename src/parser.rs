use nom::{
    IResult, Parser,
    bytes::complete::tag,
    character::complete::{alphanumeric1, multispace0},
    multi::fold_many0,
    sequence::{delimited, pair},
};

use crate::Statement;

pub(crate) fn token(i: &str) -> IResult<&str, &str> {
    let (r, ident) = delimited(multispace0, alphanumeric1, multispace0).parse(i)?;
    Ok((r, ident))
}

pub fn statement(i: &str) -> IResult<&str, Statement> {
    let (r, directive) = token(i)?;
    let (r, stmt) = match directive.to_lowercase().as_str() {
        "select" => {
            let (r, rows) = rows(r)?;
            (r, Statement::Select(rows))
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

fn rows(i: &str) -> IResult<&str, Vec<String>> {
    let (r, first) = token(i)?;
    let (r, res) = fold_many0(
        pair(delimited(multispace0, tag(","), multispace0), token),
        move || vec![first.to_string()],
        |mut acc, (_, token)| {
            acc.push(token.to_string());
            acc
        },
    )
    .parse(r)?;
    Ok((r, res))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::Statement;

    #[test]
    fn test_select() {
        let src = "SELECT id, data FROM table";
        assert_eq!(token(src).unwrap().1, "SELECT");
        assert_eq!(
            statement(src).unwrap().1,
            Statement::Select(vec!["id".to_string(), "data".to_string()])
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
