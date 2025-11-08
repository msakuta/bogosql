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
SELECT author_id, title, name FROM authors INNER JOIN books ON author_id = author
```

Result:

```
author_id | title                        | name
----------+------------------------------+--------------------
1         | I, Robot                     | Issac Asimov
1         | The Caves of Steel           | Issac Asimov
2         | The Moon Is a Harsh Mistress | Robert A. Heinlein
2         | Starship Troopers            | Robert A. Heinlein
```

Left join

```
SELECT * FROM authors LEFT JOIN books ON author_id = author
```
Result:

```
author_id | name               | book_id | title                        | author
----------+--------------------+---------+------------------------------+--------
1         | Issac Asimov       | 101     | I, Robot                     | 1
1         | Issac Asimov       | 201     | The Caves of Steel           | 1
2         | Robert A. Heinlein | 102     | The Moon Is a Harsh Mistress | 2
2         | Robert A. Heinlein | 202     | Starship Troopers            | 2
3         | Arthur C. Clarke   |         |                              |
```

Multiple joins:

```
SELECT * FROM authors INNER JOIN books ON author_id = books.author INNER JOIN characters ON book = book_id
```

Result:

```
author_id | name               | book_id | title                        | author | book | char_id | name
----------+--------------------+---------+------------------------------+--------+------+---------+--------------
1         | Issac Asimov       | 101     | I, Robot                     | 1      | 101  | 1       | Elijah Baley
1         | Issac Asimov       | 101     | I, Robot                     | 1      | 101  | 2       | R. Sammy
2         | Robert A. Heinlein | 102     | The Moon Is a Harsh Mistress | 2      | 102  | 1       | Manuel
2         | Robert A. Heinlein | 102     | The Moon Is a Harsh Mistress | 2      | 102  | 2       | Wyoming
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
* [x] Aliases (`AS`)
* [x] Ordering (`ORDER BY col`)
* [x] LIMIT, OFFSET (screw Oracle DB)
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

## How to build wasm version

I tried very hard to run rollup to bundle Rust-produced Wasm files, because I wanted to use Svelte for tables, and rollup is the default choice of the bunder for Svelte, but it was too difficult that I gave up.
Neither [rollup-wasm]() nor [@wasm-tool/rollup-plugin-rust](https://github.com/wasm-tool/rollup-plugin-rust) worked.

* Make sure to install wasm-pack
* `cd wasm`
* `wasm-pack build --target web --out-dir public/pkg`
* Make sure to install npm & node
* `npx serve`
* Browse `http://localhost:3000`
