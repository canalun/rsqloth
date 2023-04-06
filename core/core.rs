use std::collections::HashMap;
use std::io::{Error, ErrorKind};

use regex::Regex;
use sqlparser::ast::{Ident, ObjectName, SetExpr, Values};
use sqlparser::{ast::Statement, dialect::MySqlDialect, parser::Parser};

pub fn format_insert_queries(sql: &str) -> Result<String, Box<dyn std::error::Error>> {
    let dialect = MySqlDialect {};
    let ast = Parser::parse_sql(&dialect, sql)?;

    if !is_insert_only(&ast) {
        return Err(Box::new(Error::new(
            ErrorKind::InvalidInput,
            "can't format queries other than INSERT",
        )));
    }

    let comment_map = extract_comments(sql);

    let mut formatted_queries: Vec<String> = Vec::new();
    for query in ast.iter() {
        if let Statement::Insert {
            or: _,
            into: _,
            table_name,
            columns,
            overwrite: _,
            source,
            partitioned: _,
            after_columns: _,
            table: _,
            on: _,
            returning: _,
        } = query
        {
            if let SetExpr::Values(values) = &*source.body {
                let char_length_matrix = get_char_length_matrix(columns, values);
                let max_char_length_vec = get_max_char_length_vec(&char_length_matrix);
                let formatted_query =
                    generate_formatted_query(table_name, columns, values, &max_char_length_vec);
                formatted_queries.push(formatted_query);
            }
        }
    }

    // comment_map has keys one more than the # of queries
    let mut result = String::from("");
    for i in 0..comment_map.len() {
        let comments = comment_map.get(&i).unwrap();
        for (j, comment) in comments.iter().enumerate() {
            result = result
                + comment
                + (if j == comments.len() - 1 {
                    "\n\n"
                } else {
                    "\n"
                });
        }
        if i < formatted_queries.len() {
            result = result + &formatted_queries[i];
        }
    }

    return Ok(result);
}

fn is_insert_only(ast: &Vec<Statement>) -> bool {
    for query in ast {
        match query {
            Statement::Insert {
                or: _,
                into: _,
                table_name: _,
                columns: _,
                overwrite: _,
                source: _,
                partitioned: _,
                after_columns: _,
                table: _,
                on: _,
                returning: _,
            } => (),
            _ => return false,
        }
    }
    return true;
}

fn extract_comments(sql_with_comment: &str) -> HashMap<usize, Vec<String>> {
    let re = Regex::new(r"(--.*)|(INSERT INTO)").unwrap();

    let mut query_index: usize = 0;
    let mut comment_map: HashMap<usize, Vec<String>> = HashMap::new();
    comment_map.insert(query_index, Vec::new());

    for comment in re.captures_iter(sql_with_comment) {
        if comment[0].starts_with("INSERT INTO") {
            query_index += 1;
            comment_map.insert(query_index, Vec::new());
        } else {
            comment_map
                .get_mut(&query_index)
                .unwrap()
                .push(String::from(&comment[0]));
        }
    }
    return comment_map;
}

fn get_char_length_matrix(columns: &Vec<Ident>, values: &Values) -> Vec<Vec<usize>> {
    let mut char_length_matrix: Vec<Vec<usize>> = Vec::new();

    // length of column name
    let mut char_length_vec: Vec<usize> = Vec::new();
    for column in columns {
        char_length_vec.push(column.to_string().len());
    }
    char_length_matrix.push(char_length_vec);

    // length of value
    for row in values.rows.iter() {
        let mut char_length_vec: Vec<usize> = Vec::new();
        for value in row {
            char_length_vec.push(value.to_string().len())
        }
        char_length_matrix.push(char_length_vec);
    }

    return char_length_matrix;
}

fn get_max_char_length_vec(char_length_matrix: &Vec<Vec<usize>>) -> Vec<usize> {
    let mut max_char_length_vec: Vec<usize> = Vec::new();
    for column in 0..(char_length_matrix[0].len()) {
        let mut max_char_length = 0;
        for row in 0..(char_length_matrix.len()) {
            if max_char_length < char_length_matrix[row][column] {
                max_char_length = char_length_matrix[row][column];
            }
        }
        max_char_length_vec.push(max_char_length);
    }
    return max_char_length_vec;
}

fn generate_formatted_query(
    table_name: &ObjectName,
    columns: &Vec<Ident>,
    values: &Values,
    max_char_length_vec: &Vec<usize>,
) -> String {
    let table_name_part: String = String::from("INSERT INTO ") + &table_name.to_string() + "\n";

    let mut column_name_part: String = String::from("(");
    for (index, column) in columns.iter().enumerate() {
        let adjustment =
            String::from(" ").repeat(max_char_length_vec[index] - column.to_string().len());
        column_name_part = column_name_part + &column.to_string() + &adjustment;
        if index != columns.len() - 1 {
            column_name_part += ","
        }
    }
    column_name_part += ")\n";

    let values_part: &str = "VALUES\n";

    let mut rows_part: String = String::from("");
    for (row_index, row) in values.rows.iter().enumerate() {
        rows_part += "(";
        for (column_index, value) in row.iter().enumerate() {
            let adjustment = String::from(" ")
                .repeat(max_char_length_vec[column_index] - value.to_string().len());
            rows_part = rows_part + &value.to_string() + &adjustment;
            if column_index != row.len() - 1 {
                rows_part += ","
            }
        }
        rows_part += ")";
        if row_index != values.rows.len() - 1 {
            rows_part += ",\n"
        } else {
            rows_part += ";\n\n"
        }
    }

    return String::from("") + &table_name_part + &column_name_part + &values_part + &rows_part;
}
