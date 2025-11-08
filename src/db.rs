use std::collections::HashMap;

use crate::{SelectStmt, Table};

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStmt),
}

pub type Database = HashMap<String, Table>;
