use crate::lexer::Token;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Ast {
    All,
    StmtList(Vec<Ast>),
    Stmt(Box<Ast>),
    Select {
        result_columns: Vec<Ast>,
        from: Box<Ast>,
        r#where: Option<Box<Ast>>,
    },
    TableOrSubQuery(Box<Ast>),
    Table(String),
    Expr(Box<Ast>),
    Function {
        name: String,
        args: Vec<Ast>,
    },
    CreateTable {
        name: String,
        column_defs: Vec<Ast>,
    },
    ColumnDef {
        name: String,
        data_type: String,
        constraints: Vec<Constraint>,
    },
    Identifier(String),
    StringLiteral(String),
    BinaryOp {
        op: Op,
        lhs: Box<Ast>,
        rhs: Box<Ast>,
    },
    CreateIndex {
        name: String,
        table_name: String,
        columns: Vec<Ast>,
    },
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Op {
    Equal,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Constraint {
    PrimaryKey,
    AutoIncrement,
    NotNull,
}

#[derive(Debug)]
pub struct Parser {
    tokens: Vec<Token>,
    position: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Parser {
        Parser {
            tokens,
            position: 0,
        }
    }

    pub fn parse(&mut self) -> Ast {
        let statements = self.parse_statements();

        Ast::StmtList(statements)
    }

    fn peek_token(&self) -> &Token {
        if self.position >= self.tokens.len() {
            return &Token::Eof;
        }

        &self.tokens[self.position]
    }

    fn peek_next(&self) -> &Token {
        if self.position + 1 >= self.tokens.len() {
            return &Token::Eof;
        }

        &self.tokens[self.position + 1]
    }

    fn consume(&mut self, token: Token) -> Token {
        match token {
            Token::Identifier(_) => {
                if let Token::Identifier(_) = self.peek_token() {
                    self.position += 1;
                    self.tokens[self.position - 1].clone()
                } else {
                    panic!("Unexpected token: {:?}", self.peek_token());
                }
            }
            _ => {
                if self.peek_token() == &token {
                    self.position += 1;
                    self.tokens[self.position - 1].clone()
                } else {
                    panic!("Unexpected token: {:?}", self.peek_token());
                }
            }
        }
    }

    fn parse_statements(&mut self) -> Vec<Ast> {
        let mut statements = Vec::new();

        loop {
            let statement = self.parse_statement();

            statements.push(statement);

            if *self.peek_token() == Token::Eof {
                break;
            }
        }

        statements
    }

    fn parse_statement(&mut self) -> Ast {
        let statement = match self.peek_token() {
            Token::Select => self.parse_select(),
            Token::Create => self.parse_create(),
            _ => {
                panic!("Unexpected token: {:?}", self.peek_token());
            }
        };

        if self.peek_token() == &Token::Semicolon {
            self.consume(Token::Semicolon);
        }
        Ast::Stmt(Box::new(statement))
    }

    fn parse_select(&mut self) -> Ast {
        let mut result_columns = Vec::new();

        self.consume(Token::Select);

        while self.peek_token() != &Token::From {
            match self.peek_token() {
                Token::Star => {
                    result_columns.push(Ast::All);
                    self.consume(Token::Star);
                }
                _ => {
                    result_columns.push(self.parse_expr());
                    if self.peek_token() == &Token::Comma {
                        self.consume(Token::Comma);
                    }
                }
            }
        }

        let from = self.parse_from();

        let r#where = if self.peek_token() == &Token::Where {
            self.consume(Token::Where);
            let expr = self.parse_expr();
            Some(Box::new(expr))
        } else {
            None
        };

        Ast::Select {
            result_columns,
            from: Box::new(from),
            r#where,
        }
    }

    fn parse_from(&mut self) -> Ast {
        self.consume(Token::From);

        let table_or_subquery = self.parse_table_or_subquery();

        Ast::TableOrSubQuery(Box::new(table_or_subquery))
    }

    fn parse_table_or_subquery(&mut self) -> Ast {
        let identifier = self.consume(Token::Identifier("".to_string()));

        match identifier {
            Token::Identifier(name) => Ast::Table(name.to_string()),
            _ => panic!("Unexpected token: {:?}", identifier),
        }
    }

    fn parse_expr(&mut self) -> Ast {
        match self.peek_token().clone() {
            Token::Identifier(name) => {
                self.consume(Token::Identifier("".to_string()));
                match self.peek_token() {
                    Token::LParen => self.parse_function(name),
                    _ => {
                        if self.peek_token() == &Token::Equals {
                            self.consume(Token::Equals);
                            let rhs = self.parse_expr();
                            Ast::Expr(Box::new(Ast::BinaryOp {
                                op: Op::Equal,
                                lhs: Box::new(Ast::Expr(Box::new(Ast::Identifier(name)))),
                                rhs: Box::new(rhs),
                            }))
                        } else {
                            Ast::Expr(Box::new(Ast::Identifier(name)))
                        }
                    }
                }
            }
            Token::StringLiteral(value) => {
                self.position += 1;
                Ast::Expr(Box::new(Ast::StringLiteral(value.to_string())))
            }
            _ => panic!("Unexpected token: {:?}", self.peek_token()),
        }
    }

    fn parse_function(&mut self, name: String) -> Ast {
        self.consume(Token::LParen);

        let args = self.parse_function_arguments();

        Ast::Expr(Box::new(Ast::Function { name, args }))
    }

    fn parse_function_arguments(&mut self) -> Vec<Ast> {
        let mut args = Vec::new();

        loop {
            match self.peek_token() {
                Token::Star => {
                    args.push(Ast::All);
                    self.consume(Token::Star);
                }
                _ => {
                    args.push(self.parse_expr());
                }
            }

            if self.peek_token() == &Token::Comma {
                self.consume(Token::Comma);
            } else {
                break;
            }
        }

        self.consume(Token::RParen);

        args
    }

    pub fn parse_create(&mut self) -> Ast {
        self.consume(Token::Create);

        match self.peek_token() {
            Token::Table => self.parse_create_table(),
            Token::Index => self.parse_create_index(),
            _ => panic!("Unexpected token: {:?}", self.peek_token()),
        }
    }

    fn parse_create_table(&mut self) -> Ast {
        self.consume(Token::Table);
        let name = self.peek_token().clone();
        let name = match name {
            Token::Identifier(name) => name,
            Token::StringLiteral(name) => name,
            _ => panic!("Unexpected token: {:?}", name),
        };
        self.position += 1;

        if name == "SQLITE_SEQUENCE" {
            return self.sqlite_sequence_hack();
        }

        self.consume(Token::LParen);

        let column_defs = self.parse_column_defs();

        self.consume(Token::RParen);

        Ast::CreateTable { name, column_defs }
    }

    fn parse_create_index(&mut self) -> Ast {
        self.consume(Token::Index);
        let name = self.peek_token().clone();
        let name = match name {
            Token::Identifier(name) => name,
            Token::StringLiteral(name) => name,
            _ => panic!("Unexpected token: {:?}", name),
        };
        self.position += 1;
        self.consume(Token::On);
        let table_name = self.peek_token().clone();
        let table_name = match table_name {
            Token::Identifier(name) => name,
            Token::StringLiteral(name) => name,
            _ => panic!("Unexpected token: {:?}", table_name),
        };
        self.position += 1;
        self.consume(Token::LParen);
        let mut columns = Vec::new();

        loop {
            if self.peek_token() == &Token::RParen {
                break;
            }

            if self.peek_token() == &Token::Comma {
                self.consume(Token::Comma);
            }

            let column = self.peek_token().clone();
            let column = match column {
                Token::Identifier(name) => name,
                Token::StringLiteral(name) => name,
                _ => panic!("Unexpected token: {:?}", column),
            };
            self.position += 1;
            columns.push(Ast::Identifier(column));
        }

        self.consume(Token::RParen);

        Ast::CreateIndex {
            name,
            table_name,
            columns,
        }
    }

    fn sqlite_sequence_hack(&mut self) -> Ast {
        self.consume(Token::LParen);
        self.consume(Token::Identifier("".to_string()));
        self.consume(Token::Comma);
        self.consume(Token::Identifier("".to_string()));
        self.consume(Token::RParen);

        Ast::CreateTable {
            name: "SQLITE_SEQUENCE".to_string(),
            column_defs: vec![
                Ast::ColumnDef {
                    name: "NAME".to_string(),
                    data_type: "TEXT".to_string(),
                    constraints: vec![],
                },
                Ast::ColumnDef {
                    name: "SEQ".to_string(),
                    data_type: "INTEGER".to_string(),
                    constraints: vec![],
                },
            ],
        }
    }

    fn parse_column_defs(&mut self) -> Vec<Ast> {
        let mut column_defs = Vec::new();

        loop {
            let name = self.peek_token().clone();
            let name = match name {
                Token::Identifier(name) => name,
                Token::StringLiteral(name) => name.to_ascii_uppercase(),
                _ => panic!("Unexpected token: {:?}", name),
            };
            self.position += 1;

            let data_type = self.consume(Token::Identifier("".to_string()));
            let data_type = match data_type {
                Token::Identifier(data_type) => data_type,
                _ => panic!("Unexpected token: {:?}", data_type),
            };

            let mut constraints = Vec::new();

            loop {
                match self.peek_token() {
                    Token::Primary => {
                        if self.peek_next() == &Token::Key {
                            constraints.push(Constraint::PrimaryKey);
                            self.consume(Token::Primary);
                            self.consume(Token::Key);
                        }
                    }
                    Token::Not => {
                        if self.peek_next() == &Token::Null {
                            constraints.push(Constraint::NotNull);
                            self.consume(Token::Not);
                            self.consume(Token::Null);
                        }
                    }
                    Token::AutoIncrement => {
                        constraints.push(Constraint::AutoIncrement);
                        self.consume(Token::AutoIncrement);
                    }
                    Token::Comma => break,
                    Token::RParen => break,
                    _ => panic!("Unexpected token: {:?}", self.peek_token()),
                }
            }

            column_defs.push(Ast::ColumnDef {
                name,
                data_type,
                constraints,
            });

            if self.peek_token() == &Token::Comma {
                self.consume(Token::Comma);
            } else {
                break;
            }
        }

        column_defs
    }
}

mod tests {
    #[allow(unused_imports)]
    use super::*;
    #[allow(unused_imports)]
    use crate::lexer::Lexer;

    #[test]
    fn select_from() {
        let input = "SELECT * FROM Employee;";

        let mut lexer = Lexer::new(input.to_string());

        let tokens = lexer.lex();

        let mut parser = Parser::new(tokens);

        let expected = Ast::StmtList(vec![Ast::Stmt(Box::new(Ast::Select {
            result_columns: vec![Ast::All],
            from: Box::new(Ast::TableOrSubQuery(Box::new(Ast::Table(
                "EMPLOYEE".to_string(),
            )))),
            r#where: None,
        }))]);

        let ast = parser.parse();

        assert_eq!(ast, expected);
    }

    #[test]
    fn select_apple_from_fruits() {
        let input = "SELECT apple FROM fruits;";

        let mut lexer = Lexer::new(input.to_string());

        let tokens = lexer.lex();

        let mut parser = Parser::new(tokens);

        let expected = Ast::StmtList(vec![Ast::Stmt(Box::new(Ast::Select {
            result_columns: vec![Ast::Expr(Box::new(Ast::Identifier("APPLE".to_string())))],
            from: Box::new(Ast::TableOrSubQuery(Box::new(Ast::Table(
                "FRUITS".to_string(),
            )))),
            r#where: None,
        }))]);

        let ast = parser.parse();

        assert_eq!(ast, expected);
    }

    #[test]
    fn select_multiple_columns() {
        let input = "SELECT name, color FROM apples;";

        let mut lexer = Lexer::new(input.to_string());

        let tokens = lexer.lex();

        let mut parser = Parser::new(tokens);

        let expected = Ast::StmtList(vec![Ast::Stmt(Box::new(Ast::Select {
            result_columns: vec![
                Ast::Expr(Box::new(Ast::Identifier("NAME".to_string()))),
                Ast::Expr(Box::new(Ast::Identifier("COLOR".to_string()))),
            ],
            from: Box::new(Ast::TableOrSubQuery(Box::new(Ast::Table(
                "APPLES".to_string(),
            )))),
            r#where: None,
        }))]);

        let ast = parser.parse();

        assert_eq!(ast, expected);
    }

    #[test]
    fn select_count() {
        let input = "SELECT COUNT(*) FROM Employee;";

        let mut lexer = Lexer::new(input.to_string());

        let tokens = lexer.lex();

        let mut parser = Parser::new(tokens);

        let expected = Ast::StmtList(vec![Ast::Stmt(Box::new(Ast::Select {
            result_columns: vec![Ast::Expr(Box::new(Ast::Function {
                name: "COUNT".to_string(),
                args: vec![Ast::All],
            }))],
            from: Box::new(Ast::TableOrSubQuery(Box::new(Ast::Table(
                "EMPLOYEE".to_string(),
            )))),
            r#where: None,
        }))]);

        let ast = parser.parse();

        assert_eq!(ast, expected);
    }

    #[test]
    fn create_table() {
        let input = "CREATE TABLE Employee (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);";

        let mut lexer = Lexer::new(input.to_string());

        let tokens = lexer.lex();

        let mut parser = Parser::new(tokens);

        let expected = Ast::StmtList(vec![Ast::Stmt(Box::new(Ast::CreateTable {
            name: "EMPLOYEE".to_string(),
            column_defs: vec![
                Ast::ColumnDef {
                    name: "ID".to_string(),
                    data_type: "INTEGER".to_string(),
                    constraints: vec![Constraint::PrimaryKey, Constraint::AutoIncrement],
                },
                Ast::ColumnDef {
                    name: "NAME".to_string(),
                    data_type: "TEXT".to_string(),
                    constraints: vec![],
                },
            ],
        }))]);

        let ast = parser.parse();

        assert_eq!(ast, expected);
    }

    #[test]
    fn select_from_where() {
        let input = "SELECT name, color FROM apples WHERE color = 'Yellow';";
        let mut lexer = Lexer::new(input.to_string());
        let tokens = lexer.lex();
        let mut parser = Parser::new(tokens);

        let expected = Ast::StmtList(vec![Ast::Stmt(Box::new(Ast::Select {
            result_columns: vec![
                Ast::Expr(Box::new(Ast::Identifier("NAME".to_string()))),
                Ast::Expr(Box::new(Ast::Identifier("COLOR".to_string()))),
            ],
            from: Box::new(Ast::TableOrSubQuery(Box::new(Ast::Table(
                "APPLES".to_string(),
            )))),
            r#where: Some(Box::new(Ast::Expr(Box::new(Ast::BinaryOp {
                op: Op::Equal,
                lhs: Box::new(Ast::Expr(Box::new(Ast::Identifier("COLOR".to_string())))),
                rhs: Box::new(Ast::Expr(Box::new(Ast::StringLiteral(
                    "Yellow".to_string(),
                )))),
            })))),
        }))]);

        let ast = parser.parse();

        assert_eq!(ast, expected);
    }

    #[test]
    fn create_superhero_table() {
        let input = "CREATE TABLE \"superheroes\" (id integer primary key autoincrement, name text not null, eye_color text, hair_color text, appearance_count integer, first_appearance text, first_appearance_year text)";
        let mut lexer = Lexer::new(input.to_string());
        let tokens = lexer.lex();
        let mut parser = Parser::new(tokens);

        let expected = Ast::StmtList(vec![Ast::Stmt(Box::new(Ast::CreateTable {
            name: "superheroes".to_string(),
            column_defs: vec![
                Ast::ColumnDef {
                    name: "ID".to_string(),
                    data_type: "INTEGER".to_string(),
                    constraints: vec![Constraint::PrimaryKey, Constraint::AutoIncrement],
                },
                Ast::ColumnDef {
                    name: "NAME".to_string(),
                    data_type: "TEXT".to_string(),
                    constraints: vec![Constraint::NotNull],
                },
                Ast::ColumnDef {
                    name: "EYE_COLOR".to_string(),
                    data_type: "TEXT".to_string(),
                    constraints: vec![],
                },
                Ast::ColumnDef {
                    name: "HAIR_COLOR".to_string(),
                    data_type: "TEXT".to_string(),
                    constraints: vec![],
                },
                Ast::ColumnDef {
                    name: "APPEARANCE_COUNT".to_string(),
                    data_type: "INTEGER".to_string(),
                    constraints: vec![],
                },
                Ast::ColumnDef {
                    name: "FIRST_APPEARANCE".to_string(),
                    data_type: "TEXT".to_string(),
                    constraints: vec![],
                },
                Ast::ColumnDef {
                    name: "FIRST_APPEARANCE_YEAR".to_string(),
                    data_type: "TEXT".to_string(),
                    constraints: vec![],
                },
            ],
        }))]);

        let ast = parser.parse();
        assert_eq!(ast, expected);
    }

    #[test]
    fn create_table_with_string_literal_column_name() {
        let input = "CREATE TABLE companies\n(\n\tid integer primary key autoincrement\n, \"size range\" text, locality text);";
        let mut lexer = Lexer::new(input.to_string());
        let tokens = lexer.lex();
        let mut parser = Parser::new(tokens);

        let expected = Ast::StmtList(vec![Ast::Stmt(Box::new(Ast::CreateTable {
            name: "COMPANIES".to_string(),
            column_defs: vec![
                Ast::ColumnDef {
                    name: "ID".to_string(),
                    data_type: "INTEGER".to_string(),
                    constraints: vec![Constraint::PrimaryKey, Constraint::AutoIncrement],
                },
                Ast::ColumnDef {
                    name: "SIZE RANGE".to_string(),
                    data_type: "TEXT".to_string(),
                    constraints: vec![],
                },
                Ast::ColumnDef {
                    name: "LOCALITY".to_string(),
                    data_type: "TEXT".to_string(),
                    constraints: vec![],
                },
            ],
        }))]);

        let ast = parser.parse();
        assert_eq!(ast, expected);
    }

    #[test]
    fn sqlite_sequence() {
        let input = "CREATE TABLE sqlite_sequence(name,seq);";
        let mut lexer = Lexer::new(input.to_string());
        let tokens = lexer.lex();
        let mut parser = Parser::new(tokens);

        let expected = Ast::StmtList(vec![Ast::Stmt(Box::new(Ast::CreateTable {
            name: "SQLITE_SEQUENCE".to_string(),
            column_defs: vec![
                Ast::ColumnDef {
                    name: "NAME".to_string(),
                    data_type: "TEXT".to_string(),
                    constraints: vec![],
                },
                Ast::ColumnDef {
                    name: "SEQ".to_string(),
                    data_type: "INTEGER".to_string(),
                    constraints: vec![],
                },
            ],
        }))]);

        let ast = parser.parse();
        assert_eq!(ast, expected);
    }

    #[test]
    fn create_index() {
        let input =
            "CREATE INDEX idx_superheroes_first_appeared ON superheroes (first_appearance);";
        let mut lexer = Lexer::new(input.to_string());
        let tokens = lexer.lex();
        let mut parser = Parser::new(tokens);

        let expected = Ast::StmtList(vec![Ast::Stmt(Box::new(Ast::CreateIndex {
            name: "IDX_SUPERHEROES_FIRST_APPEARED".to_string(),
            table_name: "SUPERHEROES".to_string(),
            // TODO: This isn't exactly true to spec, I'm taking some easier shortcuts to get this challenge done!
            columns: vec![Ast::Identifier("FIRST_APPEARANCE".to_string())],
        }))]);

        let ast = parser.parse();
        assert_eq!(ast, expected);
    }
}
