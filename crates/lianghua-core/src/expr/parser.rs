use crate::expr::lexer::{Lexer, Token, TokenKind};

pub fn lex_all(expr: &str) -> Vec<Token> {
    let mut lx = Lexer::new(expr);
    let mut out = Vec::with_capacity(256);
    loop {
        let tok = lx.next_token();
        let is_eof = tok.kind == TokenKind::Eof;
        out.push(tok);
        if is_eof {
            break;
        }
    }
    out
}

#[derive(Debug, Clone)]
pub struct ParseErr {
    pub msg: String,
    pub idx: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Number(f64),
    Ident(String),
    Call {
        name: String,
        args: Vec<Expr>,
    },
    Unary {
        op: UnaryOp,
        rhs: Box<Expr>,
    },
    Binary {
        op: BinaryOp,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
    },
}
#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOp {
    Neg,
    Not,
}
#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Gt,
    Ge,
    Lt,
    Le,
    Eq,
    Ne,
    And,
    Or,
}

#[derive(Debug, Clone)]
pub struct Parser {
    token: Vec<Token>,
    idx: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Stmt {
    Expr(Expr),
    Assign { name: String, value: Expr },
}

#[derive(Debug, Clone)]
pub struct Stmts {
    pub item: Vec<Stmt>,
}

impl Parser {
    pub fn new(input: Vec<Token>) -> Self {
        Self {
            token: input,
            idx: 0,
        }
    }

    fn peek_kind(&self) -> &TokenKind {
        &self.token[self.idx].kind
    }

    fn current_token(&self) -> &Token {
        &self.token[self.idx]
    }

    fn current_offset(&self) -> usize {
        self.current_token().start
    }

    fn err_here(&self, msg: impl Into<String>) -> ParseErr {
        ParseErr {
            msg: msg.into(),
            idx: self.current_offset(),
        }
    }

    fn token_brief(kind: &TokenKind) -> String {
        match kind {
            TokenKind::Eof => "输入结束".to_string(),
            TokenKind::Plus => "`+`".to_string(),
            TokenKind::Minus => "`-`".to_string(),
            TokenKind::Star => "`*`".to_string(),
            TokenKind::Slash => "`/`".to_string(),
            TokenKind::LParen => "`(`".to_string(),
            TokenKind::RParen => "`)`".to_string(),
            TokenKind::LBracket => "`[`".to_string(),
            TokenKind::RBracket => "`]`".to_string(),
            TokenKind::Comma => "`,`".to_string(),
            TokenKind::Semi => "`;`".to_string(),
            TokenKind::And => "`AND`".to_string(),
            TokenKind::Or => "`OR`".to_string(),
            TokenKind::Not => "`NOT`".to_string(),
            TokenKind::In => "`IN`".to_string(),
            TokenKind::Ident(name) => format!("标识符 `{name}`"),
            TokenKind::Number(num) => format!("数字 `{num}`"),
            TokenKind::Gt => "`>`".to_string(),
            TokenKind::Ge => "`>=`".to_string(),
            TokenKind::Lt => "`<`".to_string(),
            TokenKind::Le => "`<=`".to_string(),
            TokenKind::Eq => "`==`".to_string(),
            TokenKind::Ne => "`!=`".to_string(),
            TokenKind::ColonEq => "`:=`".to_string(),
            TokenKind::Unknown(ch) => format!("未知符号 `{ch}`"),
        }
    }

    fn pop_token(&mut self) -> TokenKind {
        let tok = self.token[self.idx].kind.clone();
        self.idx += 1;
        tok
    }

    fn peek_next_token(&self) -> &TokenKind {
        self.token
            .get(self.idx + 1)
            .map(|t| &t.kind)
            .unwrap_or(&TokenKind::Eof)
    }

    fn parse_expr(&mut self, min_bp: u8) -> Result<Expr, ParseErr> {
        let mut lhs = self.parse_primary()?;

        loop {
            if let Some((l_bp, _r_bp)) = (|kind: &TokenKind| -> Option<(u8, u8)> {
                match kind {
                    TokenKind::In => Some((30, 31)),
                    _ => None,
                }
            })(self.peek_kind())
            {
                if l_bp < min_bp {
                    break;
                }

                self.pop_token();
                lhs = self.parse_in_range_expr(lhs)?;
                continue;
            }

            let Some((l_bp, r_bp, op)) = (|kind: &TokenKind| -> Option<(u8, u8, BinaryOp)> {
                match kind {
                    TokenKind::Ge => Some((30, 31, BinaryOp::Ge)),
                    TokenKind::Gt => Some((30, 31, BinaryOp::Gt)),
                    TokenKind::Le => Some((30, 31, BinaryOp::Le)),
                    TokenKind::Lt => Some((30, 31, BinaryOp::Lt)),
                    TokenKind::Eq => Some((30, 31, BinaryOp::Eq)),
                    TokenKind::Ne => Some((30, 31, BinaryOp::Ne)),

                    TokenKind::Plus => Some((40, 41, BinaryOp::Add)),
                    TokenKind::Minus => Some((40, 41, BinaryOp::Sub)),
                    TokenKind::Star => Some((50, 51, BinaryOp::Mul)),
                    TokenKind::Slash => Some((50, 51, BinaryOp::Div)),

                    TokenKind::And => Some((20, 21, BinaryOp::And)),
                    TokenKind::Or => Some((10, 11, BinaryOp::Or)),

                    _ => None,
                }
            })(self.peek_kind()) else {
                break;
            };
            if l_bp < min_bp {
                break;
            }

            self.pop_token();
            let rhs = self.parse_expr(r_bp)?;
            lhs = Expr::Binary {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }

        Ok(lhs)
    }

    fn parse_in_range_expr(&mut self, lhs: Expr) -> Result<Expr, ParseErr> {
        let include_lower = match self.peek_kind() {
            TokenKind::LBracket => {
                self.pop_token();
                true
            }
            TokenKind::LParen => {
                self.pop_token();
                false
            }
            other => {
                return Err(self.err_here(format!(
                    "`IN` 后需要范围，期望 `[` 或 `(`，当前位置是 {}",
                    Self::token_brief(other)
                )));
            }
        };

        let lower = self.parse_expr(0)?;
        match self.peek_kind() {
            TokenKind::Comma => {
                self.pop_token();
            }
            other => {
                return Err(self.err_here(format!(
                    "`IN` 范围缺少分隔符，期望 `,`，当前位置是 {}",
                    Self::token_brief(other)
                )));
            }
        }

        let upper = self.parse_expr(0)?;
        let include_upper = match self.peek_kind() {
            TokenKind::RBracket => {
                self.pop_token();
                true
            }
            TokenKind::RParen => {
                self.pop_token();
                false
            }
            other => {
                return Err(self.err_here(format!(
                    "`IN` 范围没有正确闭合，期望 `]` 或 `)`，当前位置是 {}",
                    Self::token_brief(other)
                )));
            }
        };

        let lower_cmp = Expr::Binary {
            op: if include_lower {
                BinaryOp::Ge
            } else {
                BinaryOp::Gt
            },
            lhs: Box::new(lhs.clone()),
            rhs: Box::new(lower),
        };
        let upper_cmp = Expr::Binary {
            op: if include_upper {
                BinaryOp::Le
            } else {
                BinaryOp::Lt
            },
            lhs: Box::new(lhs),
            rhs: Box::new(upper),
        };

        Ok(Expr::Binary {
            op: BinaryOp::And,
            lhs: Box::new(lower_cmp),
            rhs: Box::new(upper_cmp),
        })
    }

    fn parse_primary(&mut self) -> Result<Expr, ParseErr> {
        match self.peek_kind() {
            TokenKind::Ident(_) => {
                let name = match self.pop_token() {
                    TokenKind::Ident(name) => name,
                    other => return Err(self.err_here(format!("变量名解析失败，当前位置是 {}", Self::token_brief(&other)))),
                };
                if !matches!(self.peek_kind(), TokenKind::LParen) {
                    return Ok(Expr::Ident(name));
                }
                match self.pop_token() {
                    TokenKind::LParen => {}
                    other => {
                        return Err(self.err_here(format!(
                            "函数 `{name}` 后需要左括号 `(`，当前位置是 {}",
                            Self::token_brief(&other)
                        )));
                    }
                }

                let mut args = Vec::new();

                if matches!(self.peek_kind(), TokenKind::RParen) {
                    self.pop_token();
                    return Ok(Expr::Call { name, args });
                }

                loop {
                    args.push(self.parse_expr(0)?);

                    match self.peek_kind() {
                        TokenKind::Comma => {
                            self.pop_token();
                        }
                        TokenKind::RParen => {
                            self.pop_token();
                            break;
                        }
                        other => {
                            return Err(self.err_here(format!(
                                "函数 `{name}` 的参数列表未正确结束，期望 `,` 或 `)`，当前位置是 {}",
                                Self::token_brief(other)
                            )));
                        }
                    }
                }
                Ok(Expr::Call { name, args })
            }

            TokenKind::Number(_) => match self.pop_token() {
                TokenKind::Number(num) => Ok(Expr::Number(num)),
                other => Err(self.err_here(format!(
                    "数字解析失败，当前位置是 {}",
                    Self::token_brief(&other)
                ))),
            },
            TokenKind::LParen => {
                self.pop_token();
                let inner = self.parse_expr(0)?;
                match self.peek_kind() {
                    TokenKind::RParen => {
                        self.pop_token();
                        Ok(inner)
                    }
                    other => Err(self.err_here(format!(
                        "括号表达式没有闭合，期望 `)`，当前位置是 {}",
                        Self::token_brief(other)
                    ))),
                }
            }
            TokenKind::Minus => {
                self.pop_token();
                let rhs = self.parse_primary()?;
                Ok(Expr::Unary {
                    op: UnaryOp::Neg,
                    rhs: Box::new(rhs),
                })
            }
            TokenKind::Not => {
                self.pop_token();
                let rhs = self.parse_primary()?;
                Ok(Expr::Unary {
                    op: UnaryOp::Not,
                    rhs: Box::new(rhs),
                })
            }

            other => Err(self.err_here(format!(
                "这里不能直接开始一个表达式，当前位置是 {}；期望数字、变量、函数调用、括号表达式、负号 `-` 或 `NOT`",
                Self::token_brief(other)
            ))),
        }
    }

    fn parse_stmt(&mut self) -> Result<Stmt, ParseErr> {
        if matches!(self.peek_kind(), TokenKind::Ident(_)) {
            if matches!(self.peek_next_token(), TokenKind::ColonEq) {
                let name = match self.pop_token() {
                    TokenKind::Ident(x) => x,
                    other => {
                        return Err(self.err_here(format!(
                            "赋值语句左侧需要变量名，当前位置是 {}",
                            Self::token_brief(&other)
                        )));
                    }
                };
                match self.pop_token() {
                    TokenKind::ColonEq => {}
                    _ => {
                        return Err(self.err_here("赋值语句需要使用 `:=`".to_string()));
                    }
                }
                let value = self.parse_expr(0)?;
                return Ok(Stmt::Assign { name, value });
            }
        }
        let expr = self.parse_expr(0)?;
        Ok(Stmt::Expr(expr))
    }

    pub fn parse_main(&mut self) -> Result<Stmts, ParseErr> {
        let mut stmts = Vec::new();

        loop {
            match self.peek_kind() {
                TokenKind::Eof => break,
                TokenKind::Semi => {
                    return Err(self.err_here(
                        "不允许空语句；请删除多余的 `;`，或在两侧补上完整表达式".to_string(),
                    ));
                }
                _ => {}
            }

            stmts.push(self.parse_stmt()?);

            match self.peek_kind() {
                TokenKind::Semi => {
                    self.pop_token();
                }
                TokenKind::Eof => break,
                _ => {
                    return Err(self.err_here(format!(
                        "表达式结尾不完整，期望 `;` 或输入结束，当前位置是 {}",
                        Self::token_brief(self.peek_kind())
                    )));
                }
            }
        }
        Ok(Stmts { item: stmts })
    }
}

#[cfg(test)]
mod tests {
    use super::{Parser, lex_all};

    fn parse_err(input: &str) -> (usize, String) {
        let mut parser = Parser::new(lex_all(input));
        let err = parser.parse_main().expect_err("expected parse error");
        (err.idx, err.msg)
    }

    #[test]
    fn reports_missing_function_closer_clearly() {
        let (idx, msg) = parse_err("max(a, b");
        assert_eq!(idx, 8);
        assert!(msg.contains("函数 `max` 的参数列表未正确结束"));
        assert!(msg.contains("期望 `,` 或 `)`"));
        assert!(msg.contains("输入结束"));
    }

    #[test]
    fn reports_missing_group_closer_clearly() {
        let (idx, msg) = parse_err("(a + 1");
        assert_eq!(idx, 6);
        assert!(msg.contains("括号表达式没有闭合"));
        assert!(msg.contains("期望 `)`"));
        assert!(msg.contains("输入结束"));
    }

    #[test]
    fn reports_unexpected_statement_ending_clearly() {
        let (idx, msg) = parse_err("a b");
        assert_eq!(idx, 2);
        assert!(msg.contains("表达式结尾不完整"));
        assert!(msg.contains("期望 `;` 或输入结束"));
        assert!(msg.contains("标识符 `b`"));
    }

    #[test]
    fn parses_in_range_into_comparison_chain() {
        use super::{BinaryOp, Expr, Stmt};

        let mut parser = Parser::new(lex_all("C IN [MA(C, 5), HHV(C, 20))"));
        let stmts = parser.parse_main().expect("parse should succeed");

        assert_eq!(stmts.item.len(), 1);
        match &stmts.item[0] {
            Stmt::Expr(Expr::Binary { op, lhs, rhs }) => {
                assert_eq!(*op, BinaryOp::And);

                match &**lhs {
                    Expr::Binary {
                        op,
                        lhs: cmp_lhs,
                        rhs: lower,
                    } => {
                        assert_eq!(*op, BinaryOp::Ge);
                        assert_eq!(**cmp_lhs, Expr::Ident("C".to_string()));
                        assert!(matches!(&**lower, Expr::Call { name, .. } if name == "MA"));
                    }
                    other => panic!("unexpected lower comparison: {other:?}"),
                }

                match &**rhs {
                    Expr::Binary {
                        op,
                        lhs: cmp_lhs,
                        rhs: upper,
                    } => {
                        assert_eq!(*op, BinaryOp::Lt);
                        assert_eq!(**cmp_lhs, Expr::Ident("C".to_string()));
                        assert!(matches!(&**upper, Expr::Call { name, .. } if name == "HHV"));
                    }
                    other => panic!("unexpected upper comparison: {other:?}"),
                }
            }
            other => panic!("unexpected stmt: {other:?}"),
        }
    }

    #[test]
    fn reports_missing_in_range_delimiter_clearly() {
        let (idx, msg) = parse_err("C IN [1 2]");
        assert_eq!(idx, 8);
        assert!(msg.contains("`IN` 范围缺少分隔符"));
        assert!(msg.contains("标识符") || msg.contains("数字 `2`"));
    }
}
