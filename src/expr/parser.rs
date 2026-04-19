use crate::expr::lexer::{Lexer, Token, TokenKind};

//循环解析表达式字符到数组
pub fn lex_all(expr: &str) -> Vec<Token> {
    let mut lx = Lexer::new(expr);
    let mut out = Vec::with_capacity(256);
    loop {
        let tok = lx.next_token();
        let is_eof = tok.kind == TokenKind::Eof; // 在tok被消耗之前获取kind
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

// 类型枚举
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Number(f64),
    Ident(String),
    Call {
        name: String,
        args: Vec<Expr>,
    }, // 函数 {函数名, 参数},参数也有可能是表达式,再次嵌套
    Unary {
        op: UnaryOp,
        rhs: Box<Expr>,
    }, // 一元运算符 {运算符, 表达式}
    Binary {
        op: BinaryOp,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
    }, // 二元运算符, 表达式不定长度,用指针装
}
#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOp {
    Neg, // 负号
    Not, // 逻辑非
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

fn infix_bp(kind: &TokenKind) -> Option<(u8, u8, BinaryOp)> {
    // 先套运算,再套比较,最后套逻辑,优先级高的先套括号
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
}

// 储存表达式的结构体
#[derive(Debug, Clone)]
pub struct Parser {
    token: Vec<Token>,
    idx: usize,
}

// 语句和赋值的枚举
#[derive(Debug, Clone, PartialEq)]
pub enum Stmt {
    Expr(Expr), // 名叫Expr的枚举分支,包含一个Expr类型的值
    Assign { name: String, value: Expr },
}

// 储存语句的结构体
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
            TokenKind::Comma => "`,`".to_string(),
            TokenKind::Semi => "`;`".to_string(),
            TokenKind::And => "`AND`".to_string(),
            TokenKind::Or => "`OR`".to_string(),
            TokenKind::Not => "`NOT`".to_string(),
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
        // 吃掉左操作数
        let mut lhs = self.parse_primary()?;

        // 匹配优先级
        loop {
            // 初始化比较表
            let Some((l_bp, r_bp, op)) = infix_bp(self.peek_kind()) else {
                break;
            };
            // 如果优先级比较低,比如先遇到乘法后遇到加法,则把获取到的lhs给乘法
            if l_bp < min_bp {
                break;
            }

            // 通过优先级检查,拼装二元表达式
            self.pop_token();
            // 右边的再次解析,用较大的r_bp,避免同级之间争抢中间操作数,应归属于前者所有
            let rhs = self.parse_expr(r_bp)?;
            lhs = Expr::Binary {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }

        Ok(lhs)
    }

    fn parse_primary(&mut self) -> Result<Expr, ParseErr> {
        match self.peek_kind() {
            // 字符串分支 先判断是不是内置函数
            TokenKind::Ident(_) => {
                let name = match self.pop_token() {
                    TokenKind::Ident(name) => name,
                    other => return Err(self.err_here(format!("变量名解析失败，当前位置是 {}", Self::token_brief(&other)))),
                };
                // 用左括号检查是否是函数
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

                // 函数参数解析
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

            // 数字分支
            TokenKind::Number(_) => match self.pop_token() {
                TokenKind::Number(num) => Ok(Expr::Number(num)),
                other => Err(self.err_here(format!(
                    "数字解析失败，当前位置是 {}",
                    Self::token_brief(&other)
                ))),
            },
            // 左括号分支
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
            // 负号分支
            TokenKind::Minus => {
                self.pop_token();
                let rhs = self.parse_primary()?;
                Ok(Expr::Unary {
                    op: UnaryOp::Neg,
                    rhs: Box::new(rhs),
                })
            }
            // 感叹号分支
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

    // 等号右边表达式判断
    fn parse_stmt(&mut self) -> Result<Stmt, ParseErr> {
        if matches!(self.peek_kind(), TokenKind::Ident(_)) {
            // 检查是否为赋值分支
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
        // 否则走正常表达式分支
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
}
