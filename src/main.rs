use nom::{
    IResult, Parser,
    bytes::complete::tag,
    character::complete::{alphanumeric1, multispace0},
    multi::fold_many0,
    sequence::{delimited, pair},
};

fn token(i: &str) -> IResult<&str, &str> {
    let (r, ident) = delimited(multispace0, alphanumeric1, multispace0).parse(i)?;
    Ok((r, ident))
}

fn statement(i: &str) -> IResult<&str, Statement> {
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

#[derive(Debug, Clone)]
enum Statement {
    Select(Vec<String>),
}

struct Table {
    schema: Vec<RowSchema>,
    data: Vec<String>,
}

struct RowSchema {
    name: String,
}

fn main() {
    let table = Table {
        schema: vec![
            RowSchema {
                name: "id".to_string(),
            },
            RowSchema {
                name: "data".to_string(),
            },
        ],
        data: vec![
            "1".to_string(),
            "Hello".to_string(),
            "2".to_string(),
            "World".to_string(),
        ],
    };
    let src = "SELECT id, data FROM table";
    println!("{:?}", token(src));
    println!("{:?}", statement(src));
    let src = "SELOCT id, data FROM table";
    println!("{:?}", statement(src));
}
