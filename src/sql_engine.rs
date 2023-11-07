use crate::{
    lexer::Lexer,
    parser::{Ast, Op, Parser},
    Db, MasterPageRecord, TableLeafRecord, Value,
};

struct ExecutionContext {
    rows: Option<Vec<TableLeafRecord>>,
    table: Option<MasterPageRecord>,
}

struct QueryPlanner {
    steps: Vec<QueryStep>,
}

impl QueryPlanner {
    fn new() -> Self {
        Self { steps: Vec::new() }
    }

    fn add_step(&mut self, step: QueryStep) {
        self.steps.push(step);
    }

    fn execute(&self, db: &mut Db) {
        let mut execution_context = ExecutionContext {
            table: None,
            rows: None,
        };

        let mut results = Vec::new();

        for step in self.steps.iter() {
            match step {
                QueryStep::SetTable(string) => {
                    let table = db.get_table(string);
                    execution_context.table = Some((*table).clone());
                }
                QueryStep::Where(ident, value) => {
                    let table = execution_context.table.as_ref().unwrap();
                    let col_index = table.get_column_index(ident);

                    // FIXME: This is not to spec! Can be more than one column in an index!
                    if let Some(index) = db.get_index_for_column_and_table(&table.table_name, ident)
                    {
                        execution_context.rows = Some(db.fetch_rows_from_index(&index, value));
                    } else {
                        execution_context.rows = Some(db.get_table_rows(table, &mut None));
                    }

                    execution_context.rows = Some(
                        execution_context
                            .rows
                            .unwrap()
                            .into_iter()
                            .filter(|row| {
                                let record = row;
                                let record_value = &record.values[col_index];
                                record_value == value
                            })
                            .collect::<Vec<TableLeafRecord>>(),
                    );
                }
                QueryStep::Select(columns) => {
                    let table = execution_context.table.as_ref().unwrap();

                    // If we get here and no rows have been fetched, then we need to fetch all the rows
                    if execution_context.rows.is_none() {
                        execution_context.rows = Some(db.get_table_rows(table, &mut None));
                    }

                    let rows = execution_context.rows.as_ref().unwrap();

                    let col_indexes = if columns != &["*".to_string()] {
                        columns
                            .iter()
                            .map(|col_name| {
                                if col_name == "ID" {
                                    -1
                                } else {
                                    table.get_column_index(col_name) as isize
                                }
                            })
                            .collect::<Vec<isize>>()
                    } else {
                        (0..table.columns.len() as isize).collect::<Vec<isize>>()
                    };

                    for record in rows {
                        let mut table_results = Vec::new();
                        for index in &col_indexes {
                            if index == &-1 {
                                table_results.push(Value::Int(record.header.row_id as i64));
                                continue;
                            }
                            let value = record.values[*index as usize].clone();
                            table_results.push(value);
                        }
                        if !table_results.is_empty() {
                            results.push(
                                table_results
                                    .iter()
                                    .map(|v| format!("{}", v))
                                    .collect::<Vec<String>>()
                                    .join("|"),
                            );
                        }
                    }
                }
                QueryStep::Count(what) => {
                    if what != "*" {
                        panic!("Only support count(*) for now");
                    }

                    if execution_context.rows.is_none() {
                        let table = execution_context.table.as_ref().unwrap();
                        execution_context.rows = Some(db.get_table_rows(table, &mut None));
                    }

                    let rows = execution_context.rows.as_ref().unwrap();
                    results.push(format!("{}", rows.len()));
                }
            }
        }

        for result in results {
            println!("{}", result);
        }
    }
}

#[derive(Debug)]
enum QueryStep {
    SetTable(String),
    Where(String, Value),
    Select(Vec<String>),
    Count(String),
}

pub struct SqlEngine {}

impl SqlEngine {
    pub fn new() -> Self {
        Self {}
    }

    pub fn execute(&self, sql: &str, db: &mut Db) {
        let mut lexer = Lexer::new(sql.to_string());
        let mut parser = Parser::new(lexer.lex());
        let ast = parser.parse();

        match ast {
            Ast::StmtList(statements) => self.execute_statements(statements, db),
            _ => panic!("Not implemented"),
        }
    }

    fn execute_statements(&self, stmts: Vec<Ast>, db: &mut Db) {
        for stmt in stmts {
            match stmt {
                Ast::Stmt(stmt) => self.execute_statement(*stmt, db),
                _ => panic!("Not implemented"),
            }
        }
    }

    fn execute_statement(&self, stmt: Ast, db: &mut Db) {
        match stmt {
            Ast::Select {
                result_columns,
                from,
                r#where,
            } => self.execute_select(result_columns, *from, r#where, db),
            _ => panic!("Not implemented {:?}", stmt),
        }
    }

    fn execute_select(
        &self,
        result_columns: Vec<Ast>,
        from: Ast,
        r#where: Option<Box<Ast>>,
        db: &mut Db,
    ) {
        let mut query_plan = QueryPlanner::new();

        let table_name = match from {
            Ast::TableOrSubQuery(node) => match *node {
                Ast::Table(table_name) => table_name,
                _ => panic!("Not implemented {:?}", node),
            },
            _ => panic!("Not implemented {:?}", from),
        };

        query_plan.add_step(QueryStep::SetTable(table_name));

        if let Some(where_clause) = r#where {
            if let Ast::Expr(expr) = *where_clause {
                match *expr {
                    Ast::BinaryOp { op, lhs, rhs } => {
                        let column_name = if let Ast::Expr(lhs) = *lhs {
                            match *lhs {
                                Ast::Identifier(name) => name,
                                _ => panic!("LHS Not implemented {:?}", lhs),
                            }
                        } else {
                            panic!("LHS Not implemented {:?}", lhs);
                        };

                        let value = if let Ast::Expr(rhs) = *rhs {
                            match *rhs {
                                Ast::StringLiteral(value) => value,
                                _ => panic!("RHS Not implemented {:?}", rhs),
                            }
                        } else {
                            panic!("RHS Not implemented {:?}", rhs);
                        };

                        if op != Op::Equal {
                            panic!("Only support equals for now");
                        }

                        query_plan.add_step(QueryStep::Where(column_name, Value::Text(value)));
                    }
                    _ => panic!("Not implemented {:?}", expr),
                }
            } else {
                panic!("Not implemented {:?}", where_clause);
            }
        }

        let mut columns = Vec::new();

        for result in result_columns {
            match result {
                Ast::All => {
                    columns = vec!["*".to_string()];
                    break;
                }
                Ast::Identifier(name) => columns.push(name),
                Ast::Expr(expr) => {
                    if let Ast::Function { name, args } = *expr {
                        if name == "COUNT" && args.first() == Some(&Ast::All) {
                            columns.clear();
                            query_plan.add_step(QueryStep::Count("*".to_string()));
                            break;
                        } else {
                            panic!("function {} not implemented", name);
                        }
                    } else if let Ast::Identifier(name) = *expr {
                        columns.push(name);
                    } else {
                        panic!("Not implemented {:?}", expr);
                    }
                }
                _ => panic!("Not implemented {:?}", result),
            }
        }

        if !columns.is_empty() {
            query_plan.add_step(QueryStep::Select(columns));
        }

        query_plan.execute(db);
    }
}
