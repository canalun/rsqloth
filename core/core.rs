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

    let comments_grouped_by_query = extract_comments(sql);

    let mut formatted_queries = ast
        .iter()
        .map(|query| {
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
                    let max_char_length_vec = get_max_char_length_vec(columns, values);
                    let formatted_query =
                        generate_formatted_query(table_name, columns, values, &max_char_length_vec);
                    return formatted_query;
                }
            }
            return String::from("");
        })
        .collect::<Vec<String>>();

    formatted_queries.push(String::from(""));
    // add an extra to formatted queries in order to zip with comment vec
    // that has elements one more than the # of formatted queries.

    let result = comments_grouped_by_query
        .iter()
        .map(|comment_of_query| {
            comment_of_query.iter().enumerate().map(|(i, comment)| {
                if i == comment_of_query.len() - 1 {
                    return String::from(comment) + "\n\n";
                } else {
                    return String::from(comment) + "\n";
                }
            })
        })
        .zip(formatted_queries.iter())
        .map(|(comment_of_query, query)| {
            let mut query_with_comments = comment_of_query.collect::<Vec<String>>();
            query_with_comments.push(query.clone());
            return query_with_comments;
        })
        .flatten()
        .collect::<Vec<String>>()
        .join("\n");

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

// TODO: make it functional
fn extract_comments(sql_with_comment: &str) -> Vec<Vec<String>> {
    let re = Regex::new(r"(--.*)|(INSERT INTO)").unwrap();

    let mut comment_map: Vec<Vec<String>> = vec![vec![]; 1];

    let mut query_index: usize = 0;
    for comment in re.captures_iter(sql_with_comment) {
        if comment[0].starts_with("INSERT INTO") {
            query_index += 1;
            comment_map.push(vec![]);
        } else {
            comment_map[query_index].push(String::from(&comment[0]));
        }
    }
    return comment_map;
}

// TODO: make it functional
fn get_max_char_length_vec(columns: &Vec<Ident>, values: &Values) -> Vec<usize> {
    let char_length_matrix = get_char_length_matrix(columns, values);
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

// TODO: make it functional
fn get_char_length_matrix(columns: &Vec<Ident>, values: &Values) -> Vec<Vec<usize>> {
    let char_length_matrix = columns
        .iter()
        .map(|column| {
            return column.to_string().len();
        })
        .collect::<Vec<usize>>();

    let mut a = values
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|value| value.to_string().len())
                .collect::<Vec<usize>>()
        })
        .collect::<Vec<Vec<usize>>>();

    a.insert(0, char_length_matrix);

    return a;
}

// TODO: make it functional
// construct formatted query from scratch by using ast data
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
