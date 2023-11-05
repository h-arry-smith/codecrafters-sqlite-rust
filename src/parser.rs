use crate::lexer::Token;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Ast {
    All,
    StmtList(Vec<Ast>),
    Stmt(Box<Ast>),
    Select {
        result_columns: Vec<Ast>,
        from: Box<Ast>,
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
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Constraint {
    PrimaryKey,
    AutoIncrement,
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
        dbg!(&self.tokens);
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
        dbg!(&token);
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
            eprintln!("Parsing statement {}", statements.len() + 1);
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

        self.consume(Token::Semicolon);
        Ast::Stmt(Box::new(statement))
    }

    fn parse_select(&mut self) -> Ast {
        eprintln!("Parsing select");
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

        if self.peek_token() == &Token::Star {
            result_columns.push(Ast::All);
            self.consume(Token::Star);
        }

        let from = self.parse_from();

        Ast::Select {
            result_columns,
            from: Box::new(from),
        }
    }

    fn parse_from(&mut self) -> Ast {
        eprintln!("Parsing from");
        self.consume(Token::From);

        let table_or_subquery = self.parse_table_or_subquery();

        Ast::TableOrSubQuery(Box::new(table_or_subquery))
    }

    fn parse_table_or_subquery(&mut self) -> Ast {
        eprintln!("Parsing table or subquery");
        let identifier = self.consume(Token::Identifier("".to_string()));

        match identifier {
            Token::Identifier(name) => Ast::Table(name.to_string()),
            _ => panic!("Unexpected token: {:?}", identifier),
        }
    }

    fn parse_expr(&mut self) -> Ast {
        eprintln!("Parsing expr");
        let identifier = self.consume(Token::Identifier("".to_string()));

        match identifier {
            Token::Identifier(name) => {
                if self.peek_token() == &Token::LParen {
                    self.parse_function(name)
                } else {
                    Ast::Expr(Box::new(Ast::Identifier(name)))
                }
            }
            _ => panic!("Unexpected token: {:?}", identifier),
        }
    }

    fn parse_function(&mut self, name: String) -> Ast {
        eprintln!("Parsing function");
        self.consume(Token::LParen);

        let args = self.parse_function_arguments();

        Ast::Expr(Box::new(Ast::Function { name, args }))
    }

    fn parse_function_arguments(&mut self) -> Vec<Ast> {
        eprintln!("Parsing function arguments");
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
        eprintln!("Parsing create");
        self.consume(Token::Create);
        self.consume(Token::Table);

        let name = self.consume(Token::Identifier("".to_string()));
        let name = match name {
            Token::Identifier(name) => name,
            _ => panic!("Unexpected token: {:?}", name),
        };

        self.consume(Token::LParen);

        let column_defs = self.parse_column_defs();

        self.consume(Token::RParen);

        Ast::CreateTable { name, column_defs }
    }

    fn parse_column_defs(&mut self) -> Vec<Ast> {
        eprintln!("Parsing column defs");
        let mut column_defs = Vec::new();

        loop {
            let name = self.consume(Token::Identifier("".to_string()));
            let name = match name {
                Token::Identifier(name) => name,
                _ => panic!("Unexpected token: {:?}", name),
            };

            let data_type = self.consume(Token::Identifier("".to_string()));
            let data_type = match data_type {
                Token::Identifier(data_type) => data_type,
                _ => panic!("Unexpected token: {:?}", data_type),
            };

            let mut constraints = Vec::new();

            loop {
                match dbg!(self.peek_token()) {
                    Token::Primary => {
                        if self.peek_next() == &Token::Key {
                            constraints.push(Constraint::PrimaryKey);
                            self.consume(Token::Primary);
                            self.consume(Token::Key);
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
    use super::*;
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
}
