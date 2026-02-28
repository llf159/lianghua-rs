use crate::expr::lexer::{Lexer, Token, TokenKind};

//循环解析表达式字符到数组
pub fn lex_all(expr: &str) -> Vec<Token> {
    let mut lx = Lexer::new(expr);
    let mut out = Vec::new();
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
                    other => {
                        return Err(ParseErr {
                            msg: format!("错误字符串:{:?}", other),
                            idx: self.idx,
                        });
                    }
                };
                // 用左括号检查是否是函数
                if !matches!(self.peek_kind(), TokenKind::LParen) {
                    return Ok(Expr::Ident(name));
                }
                match self.pop_token() {
                    TokenKind::LParen => {}
                    other => {
                        return Err(ParseErr {
                            msg: format!("错误符号:{:?}, 应该为左括号'('", other),
                            idx: self.idx,
                        });
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
                            return Err(ParseErr {
                                msg: format!("表达式不全,缺少参数或右括号, 错误字符:{:?}", other),
                                idx: self.idx,
                            });
                        }
                    }
                }
                Ok(Expr::Call { name, args })
            }

            // 数字分支
            TokenKind::Number(_) => match self.pop_token() {
                TokenKind::Number(num) => Ok(Expr::Number(num)),
                other => Err(ParseErr {
                    msg: format!("表达式数字错误:{:?}", other),
                    idx: self.idx,
                }),
            },
            // 左括号分支
            TokenKind::LParen => {
                self.pop_token();
                let inner = self.parse_expr(0);
                match self.pop_token() {
                    TokenKind::RParen => inner,
                    _ => Err(ParseErr {
                        msg: "表达式缺少右边括号".to_string(),
                        idx: self.idx,
                    }),
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

            other => Err(ParseErr {
                msg: format!("表达式token处理异常,错误字符:{:?}", other),
                idx: self.idx,
            }),
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
                        return Err(ParseErr {
                            msg: format!("赋值变量名解析错误:{:?}", other),
                            idx: self.idx,
                        });
                    }
                };
                match self.pop_token() {
                    TokenKind::ColonEq => {}
                    _ => {
                        return Err(ParseErr {
                            msg: "未知赋值符号错误".to_string(),
                            idx: self.idx,
                        });
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
                    return Err(ParseErr {
                        msg: "不允许有空语句".to_string(),
                        idx: self.idx,
                    });
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
                    return Err(ParseErr {
                        msg: "未知表达式结尾".to_string(),
                        idx: self.idx,
                    });
                }
            }
        }
        Ok(Stmts { item: stmts })
    }
}
