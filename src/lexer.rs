// NOTE: Note to future self, we should have a Token, it is a composite of a TokenType, and some additional
//       metadata.
#[derive(Debug, PartialEq, Eq, Clone)]
#[allow(dead_code)]
pub enum Token {
    // KEYWORDS
    Create,
    Table,
    Select,
    From,

    // PUNCTUATION
    LParen,
    RParen,
    Semicolon,
    Dot,
    Comma,
    Star,

    // IDENTIFIERS
    Identifier(String),

    // CONSTRAINTS
    Primary,
    Key,
    AutoIncrement,

    Eof,
}

#[derive(Debug)]
pub struct Lexer {
    input: String,
    position: usize,
}

impl Lexer {
    pub fn new(input: String) -> Lexer {
        Lexer { input, position: 0 }
    }

    pub fn lex(&mut self) -> Vec<Token> {
        let mut tokens = Vec::new();

        loop {
            let token = self.next_token();

            if token == Token::Eof {
                tokens.push(token);
                break;
            }

            tokens.push(token);
        }

        tokens
    }

    pub fn next_token(&mut self) -> Token {
        if self.position >= self.input.len() {
            return Token::Eof;
        }

        let mut current_char = self.input.chars().nth(self.position).unwrap();

        match current_char {
            '(' => {
                self.position += 1;
                Token::LParen
            }
            ')' => {
                self.position += 1;
                Token::RParen
            }
            ';' => {
                self.position += 1;
                Token::Semicolon
            }
            '.' => {
                self.position += 1;
                Token::Dot
            }
            ',' => {
                self.position += 1;
                Token::Comma
            }
            '-' => {
                self.position += 1;
                current_char = self.input.chars().nth(self.position).unwrap();
                if current_char == '-' {
                    self.position += 1;
                    while current_char != '\n' {
                        self.position += 1;
                        current_char = self.input.chars().nth(self.position).unwrap();
                    }
                    self.next_token()
                } else {
                    panic!("Unexpected character: {}", current_char);
                }
            }
            '*' => {
                self.position += 1;
                Token::Star
            }
            _ => {
                if current_char.is_alphabetic() {
                    let mut identifier = String::new();
                    while current_char.is_alphabetic() || current_char == '_' {
                        identifier.push(current_char);
                        self.position += 1;

                        if self.position >= self.input.len() {
                            break;
                        }

                        current_char = self.input.chars().nth(self.position).unwrap();
                    }
                    match identifier.to_ascii_uppercase().as_str() {
                        "AUTOINCREMENT" => Token::AutoIncrement,
                        "CREATE" => Token::Create,
                        "TABLE" => Token::Table,
                        "PRIMARY" => Token::Primary,
                        "KEY" => Token::Key,
                        "SELECT" => Token::Select,
                        "FROM" => Token::From,
                        _ => Token::Identifier(identifier.to_ascii_uppercase()),
                    }
                } else if current_char.is_whitespace() {
                    self.position += 1;
                    self.next_token()
                } else {
                    panic!("Unexpected character: {}", current_char);
                }
            }
        }
    }
}

mod tests {
    use super::*;

    #[test]
    fn create_table() {
        let input = "CREATE TABLE Employee (
            id INTEGER PRIMARY KEY, -- You might want to include a unique ID for each employee
            name TEXT,
            age INTEGER,
            job_title TEXT
        );";

        let mut lexer = Lexer::new(input.to_string());

        let expected = vec![
            Token::Create,
            Token::Table,
            Token::Identifier("EMPLOYEE".to_string()),
            Token::LParen,
            Token::Identifier("ID".to_string()),
            Token::Identifier("INTEGER".to_string()),
            Token::Primary,
            Token::Key,
            Token::Comma,
            Token::Identifier("NAME".to_string()),
            Token::Identifier("TEXT".to_string()),
            Token::Comma,
            Token::Identifier("AGE".to_string()),
            Token::Identifier("INTEGER".to_string()),
            Token::Comma,
            Token::Identifier("JOB_TITLE".to_string()),
            Token::Identifier("TEXT".to_string()),
            Token::RParen,
            Token::Semicolon,
            Token::Eof,
        ];

        let tokens = lexer.lex();
        assert_eq!(tokens, expected);
    }

    #[test]
    fn select() {
        let input = "SELECT * FROM Employee;";

        let mut lexer = Lexer::new(input.to_string());

        let expected = vec![
            Token::Select,
            Token::Star,
            Token::From,
            Token::Identifier("EMPLOYEE".to_string()),
            Token::Semicolon,
            Token::Eof,
        ];

        let tokens = lexer.lex();
        assert_eq!(tokens, expected);
    }

    #[test]
    fn lower_case_keywords() {
        let input = "select * from Employee;";

        let mut lexer = Lexer::new(input.to_string());

        let expected = vec![
            Token::Select,
            Token::Star,
            Token::From,
            Token::Identifier("EMPLOYEE".to_string()),
            Token::Semicolon,
            Token::Eof,
        ];

        let tokens = lexer.lex();
        assert_eq!(tokens, expected);
    }

    #[test]
    fn select_count() {
        let input = "SELECT COUNT(*) FROM Employee;";

        let mut lexer = Lexer::new(input.to_string());

        let expected = vec![
            Token::Select,
            Token::Identifier("COUNT".to_string()),
            Token::LParen,
            Token::Star,
            Token::RParen,
            Token::From,
            Token::Identifier("EMPLOYEE".to_string()),
            Token::Semicolon,
            Token::Eof,
        ];

        let tokens = lexer.lex();
        assert_eq!(tokens, expected);
    }
}
