use crate::{
    lexer::Lexer,
    parser::{Ast, Parser},
    Db, Table,
};

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
            } => self.execute_select(result_columns, *from, db),
            _ => panic!("Not implemented {:?}", stmt),
        }
    }

    fn execute_select(&self, result_columns: Vec<Ast>, from: Ast, db: &mut Db) {
        match from {
            Ast::TableOrSubQuery(expr) => {
                self.execute_select(result_columns, *expr, db);
            }
            Ast::Table(table_name) => {
                let table = db.get_table(&table_name);

                for result_col in result_columns {
                    match result_col {
                        Ast::Expr(expr) => match *expr {
                            Ast::Function { name, args } => {
                                self.execute_function(name, args, &table, db)
                            }
                            // TODO: Handle multiple columns
                            Ast::Identifier(col_name) => {
                                self.select_column_from_table(&table, &col_name, db);
                            }
                            _ => panic!("Not implemented {:?}", expr),
                        },
                        _ => panic!("Not implemented {:?}", result_col),
                    }
                }
            }
            _ => panic!("Not implemented {:?}", from),
        }
    }

    fn execute_function(&self, name: String, args: Vec<Ast>, table: &Table, db: &mut Db) {
        match name.as_str() {
            "COUNT" => {
                if args.len() > 1 {
                    panic!("Only support count with one argument");
                }

                let arg = &args[0];
                match arg {
                    Ast::All => {
                        let db_page = db.load_table(&table);
                        println!("{}", db_page.records.len());
                    }
                    _ => panic!("Not implemented {:?}", arg),
                }
            }
            _ => panic!("Not implemented {:?}", name),
        }
    }

    fn select_column_from_table(&self, table: &Table, col_name: &str, db: &mut Db) {
        let db_page = db.load_table(table);
        let col_index = table.get_column_index(col_name);

        let mut results = Vec::new();

        for record in db_page.records {
            let value = record.values[col_index].clone();
            results.push(value);
        }

        for result in results {
            println!("{}", result);
        }
    }
}
