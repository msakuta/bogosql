//! A parse for CSVs.
//! Why do I make such reinvention of wheels?
//! Because [csv](https://docs.rs/csv/latest/csv/) did not support commas in cells,
//! e.g. "I, Robot" would not be possible to put in a cell.
//! It is so frustrating, enraging even, to see such a basic feature is missing in a library,
//! so I wrote my own tiny parser for CSVs.

use nom::{
    Finish, IResult, Parser,
    branch::alt,
    bytes::complete::tag,
    character::complete::{multispace0, none_of},
    combinator::recognize,
    multi::{fold_many0, many0, many1},
    sequence::{delimited, pair},
};

pub fn parse_csv(src: &str) -> Result<Vec<Vec<String>>, String> {
    csv(src)
        .finish()
        .map(|(_, res)| res)
        .map_err(|e| e.to_string())
}

fn csv(i: &str) -> IResult<&str, Vec<Vec<String>>> {
    let (r, first) = line(i)?;
    let (r, res) = fold_many0(
        pair(tag("\n"), line),
        move || vec![first.clone()],
        |mut acc, (_, row)| {
            acc.push(row);
            acc
        },
    )
    .parse(r)?;
    Ok((r, res))
}

fn line(i: &str) -> IResult<&str, Vec<String>> {
    let (r, first) = cell(i)?;
    let (r, res) = fold_many0(
        pair(delimited(multispace0, tag(","), multispace0), cell),
        move || vec![first.clone()],
        |mut acc, (_, token)| {
            acc.push(token);
            acc
        },
    )
    .parse(r)?;
    Ok((r, res))
}

fn cell(i: &str) -> IResult<&str, String> {
    alt((quoted_cell, unquoted_cell)).parse(i)
}

fn unquoted_cell(i: &str) -> IResult<&str, String> {
    let (r, val) = recognize(many1(none_of("\",\n"))).parse(i)?;

    Ok((r, val.to_string()))
}

fn quoted_cell(i: &str) -> IResult<&str, String> {
    let (r, _) = pair(multispace0, tag("\"")).parse(i)?;
    let (r, val) = recognize(many0(none_of("\"\n"))).parse(r)?;
    let (r, _) = tag("\"")(r)?;
    Ok((r, val.to_string()))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_basic() {
        let src = "Hello, world!";
        let res = parse_csv(src).unwrap();
        assert_eq!(res, vec![vec!["Hello", "world!"]]);
    }

    #[test]
    fn test_multi() {
        let src = "Hello, world!\nGoodbye, Joe!";
        let res = parse_csv(src).unwrap();
        assert_eq!(res, vec![vec!["Hello", "world!"], vec!["Goodbye", "Joe!"]]);
    }

    #[test]
    fn test_quoted() {
        let src = r#"1, "I, Robot", 2"#;
        let res = parse_csv(src).unwrap();
        assert_eq!(res, vec![vec!["1", "I, Robot", "2"]]);
    }
}
