# bogosql

A toy reinvention of SQL server in Rust

I am interested in the SQL syntax, because it is so ambiguous that I always get lost in a complex one.
Implementing a parser (and preferably executor) is the best way to understand a syntax, so let's do it.

## How to run

There is a hardcoded tables and there is no way to update them at the moment.

* Install Rust.
* `cargo r -- "SELECT id, name, phone FROM phonebook"`
