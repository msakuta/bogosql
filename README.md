# bogosql

A toy reinvention of SQL server in Rust

I am interested in the SQL syntax, because it is so ambiguous that I always get lost in a complex one.
Implementing a parser (and preferably executor) is the best way to understand a syntax, so let's do it.

## How to run

The tables are defined in CSV files in `data` directory, whose file names without extension being table names.

* Install Rust.
* `cargo r -- "SELECT id, name, phone FROM phonebook"`

## Examples

Inner join

```
"SELECT author_id, title, name FROM authors INNER JOIN books ON author_id = author"
```

Result:

```
1,I, Robot,Asimov,
1,Cave of Steel,Asimov,
2,Moon's Harsh Mistress,Heinlein,
```

Multiple joins:

```
"SELECT * FROM authors INNER JOIN books ON author_id = author INNER JOIN pages ON book = book_id"
```

Result:

```
1,Asimov,101,I, Robot,1,101,1,Title,
1,Asimov,101,I, Robot,1,101,2,Preface,
2,Heinlein,201,Moon's Harsh Mistress,2,201,1,Mistress,
2,Heinlein,201,Moon's Harsh Mistress,2,201,2,is,
2,Heinlein,201,Moon's Harsh Mistress,2,201,3,harsh,
```

## Features

Increasingly difficult TODOs

* [x] SELECT
* [x] WHERE a = b
* [x] WHERE a <> b
  * [x] Comparison operators (`<`, `>`, `<=`, `>=`)
  * [x] Logical operators (`AND`, `OR`, `NOT`)
  * [ ] Group operators (`IN`, `NOT IN`)
  * [ ] `BETWEEN` / `NOT BETWEEN`
  * [ ] `LIKE` / `NOT LIKE`
  * [ ] `IS NULL` / `IS NOT NULL`
* [x] INNER JOIN
* [x] LEFT JOIN
* [ ] Aliases (`AS`)
* [ ] Ordering (`ORDER BY col`)
* [ ] LIMIT, OFFSET
* [ ] DISTINCT
* [ ] Expressions
  * [ ] Arithmetic: `+`, `-`, `*`, `/`
  * [x] Parentheses for precedence: `(a + b) * c`
  * [ ] Scalar function calls (`LENGTH`, `UPPER`, `LOWER`)
* [ ] Aggregation and grouping
  * [ ] Aggregate function calls (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`)
  * [ ] `HAVING`
* [ ] RIGHT JOIN
* [ ] CROSS JOIN
* [ ] Subqueries
* [ ] Set operators (`UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT`, `MINUS`)
* [ ] DML
