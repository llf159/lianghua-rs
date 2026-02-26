#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    Eof,
    Plus,
    Minus,
    Star,
    Slash,
    LParen,
    RParen,
    Comma,
    Semi,
    And,
    Or,
    Not,
    Ident(String),
    Number(f64),
    Gt,
    Ge,
    Lt,
    Le,
    Eq,
    Ne,
    ColonEq,
    Unknown(char),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub kind: TokenKind,
    pub start: usize,
    pub end: usize,
} // 读表达式到枚举

#[derive(Debug)]
pub struct Lexer<'a> {
    input: &'a str,
    pos: usize,
} // 读表达式的标记工具

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }
    // 读字符
    fn peek_char(&self) -> Option<char> {
        self.input.get(self.pos..)?.chars().next()
    }
    // 删字符
    fn pop_char(&mut self) -> Option<char> {
        let ch = self.peek_char()?;
        self.pos += ch.len_utf8();
        Some(ch)
    }

    fn is_ident_str(ch: char) -> bool {
        ch == '_' || ch.is_ascii_alphabetic()
    }

    fn is_ident_continues(ch: char) -> bool {
        ch == '_' || ch.is_ascii_alphanumeric()
    }
    // 读字母组合
    fn read_ident(&mut self) -> String {
        let start = self.pos;
        while matches!(self.peek_char(), Some(ch) if Self::is_ident_continues(ch)) {
            self.pop_char();
        }
        self.input[start..self.pos].to_string()
    }
    // 读数字
    fn read_num(&mut self) -> f64 {
        let start = self.pos;
        let mut seen_dot = false;

        while let Some(ch) = self.peek_char() {
            if ch.is_ascii_digit() {
                self.pop_char();
                continue;
            }

            if ch == '.' && !seen_dot {
                let next_is_digit = self
                    .input
                    .get(self.pos + 1..)
                    .and_then(|s| s.chars().next())
                    .map(|c| c.is_ascii_digit())
                    .unwrap_or(false);

                if !next_is_digit {
                    break;
                }

                seen_dot = true;
                self.pop_char();
                continue;
            }

            break;
        }

        self.input[start..self.pos].parse().unwrap()
    }

    // 读多字符符号
    fn seek_next_char(&self) -> Option<char> {
        let mut it = self.input.get(self.pos..)?.chars();
        it.next()?;
        it.next()
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek_char(), Some(ch) if ch.is_whitespace()) {
            self.pop_char();
        }
    }

    // 具体字符分支
    pub fn next_token(&mut self) -> Token {
        self.skip_ws();
        let start = self.pos;

        match self.peek_char() {
            None => Token {
                kind: TokenKind::Eof,
                start,
                end: start,
            },

            Some(ch) if Self::is_ident_str(ch) => {
                let ident = self.read_ident();
                let upper = ident.to_ascii_uppercase();
                let kind = match upper.as_str() {
                    "AND" => TokenKind::And,
                    "OR" => TokenKind::Or,
                    "NOT" => TokenKind::Not,
                    _ => TokenKind::Ident(ident),
                };
                Token {
                    kind,
                    start,
                    end: self.pos,
                }
            }
            Some(ch) if ch.is_ascii_digit() => {
                let ident: f64 = self.read_num();
                Token {
                    kind: TokenKind::Number(ident),
                    start,
                    end: self.pos,
                }
            }

            Some('+') => {
                self.pop_char();
                Token {
                    kind: TokenKind::Plus,
                    start,
                    end: self.pos,
                }
            }
            Some('-') => {
                self.pop_char();
                Token {
                    kind: TokenKind::Minus,
                    start,
                    end: self.pos,
                }
            }
            Some('*') => {
                self.pop_char();
                Token {
                    kind: TokenKind::Star,
                    start,
                    end: self.pos,
                }
            }
            Some('/') => {
                self.pop_char();
                Token {
                    kind: TokenKind::Slash,
                    start,
                    end: self.pos,
                }
            }

            Some('(') => {
                self.pop_char();
                Token {
                    kind: TokenKind::LParen,
                    start,
                    end: self.pos,
                }
            }
            Some(')') => {
                self.pop_char();
                Token {
                    kind: TokenKind::RParen,
                    start,
                    end: self.pos,
                }
            }

            Some(',') => {
                self.pop_char();
                Token {
                    kind: TokenKind::Comma,
                    start,
                    end: self.pos,
                }
            }
            Some(';') => {
                self.pop_char();
                Token {
                    kind: TokenKind::Semi,
                    start,
                    end: self.pos,
                }
            }

            Some(':') => {
                if self.seek_next_char() == Some('=') {
                    self.pop_char();
                    self.pop_char();
                    Token {
                        kind: TokenKind::ColonEq,
                        start,
                        end: self.pos,
                    }
                } else {
                    self.pop_char();
                    Token {
                        kind: TokenKind::Unknown(':'),
                        start,
                        end: self.pos,
                    }
                }
            }
            Some('>') => {
                if self.seek_next_char() == Some('=') {
                    self.pop_char();
                    self.pop_char();
                    Token {
                        kind: TokenKind::Ge,
                        start,
                        end: self.pos,
                    }
                } else {
                    self.pop_char();
                    Token {
                        kind: TokenKind::Gt,
                        start,
                        end: self.pos,
                    }
                }
            }
            Some('<') => {
                if self.seek_next_char() == Some('=') {
                    self.pop_char();
                    self.pop_char();
                    Token {
                        kind: TokenKind::Le,
                        start,
                        end: self.pos,
                    }
                } else {
                    self.pop_char();
                    Token {
                        kind: TokenKind::Lt,
                        start,
                        end: self.pos,
                    }
                }
            }
            Some('=') => {
                if self.seek_next_char() == Some('=') {
                    self.pop_char();
                    self.pop_char();
                } else {
                    self.pop_char();
                }
                Token {
                    kind: TokenKind::Eq,
                    start,
                    end: self.pos,
                }
            }
            Some('!') => {
                if self.seek_next_char() == Some('=') {
                    self.pop_char();
                    self.pop_char();
                    Token {
                        kind: TokenKind::Ne,
                        start,
                        end: self.pos,
                    }
                } else {
                    self.pop_char();
                    Token {
                        kind: TokenKind::Not,
                        start,
                        end: self.pos,
                    }
                }
            }

            Some(ch) => {
                self.pop_char();
                Token {
                    kind: TokenKind::Unknown(ch),
                    start,
                    end: self.pos,
                }
            }
        }
    }
}
