//! Hand-written recursive-descent parser for the LogQL subset logstream
//! accepts:
//!
//! ```text
//! selector   := '{' matcher (',' matcher)* ','? '}'
//! matcher    := label ('=' | '!=' | '=~' | '!~') string
//! query      := selector filter*
//! filter     := ('|=' | '!=' | '|~' | '!~') string
//! string     := '"' ... '"' (escapes: \" \\) | '`' ... '`' (raw, no escapes)
//! label      := [a-zA-Z_][a-zA-Z0-9_]*
//! ```
//!
//! A stream selector needs at least one matcher — an empty `{}` would mean
//! "every row for every tenant" once translated to SQL, so (like Loki) we
//! reject it outright rather than special-case an unbounded scan.
//!
//! No parser combinator crate: the grammar is small enough that hand
//! rolling it is both less code and gives us exact byte-offset error
//! positions for free.

use std::fmt;

/// Stream-selector matcher operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchOp {
    Eq,
    Neq,
    Match,
    NotMatch,
}

/// One `label <op> "value"` matcher inside `{ ... }`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Matcher {
    pub label: String,
    pub op: MatchOp,
    pub value: String,
}

/// Line-filter operator (applied to the log body, after the selector).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineFilterOp {
    Contains,
    NotContains,
    Match,
    NotMatch,
}

/// One `<op> "value"` line filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineFilter {
    pub op: LineFilterOp,
    pub value: String,
}

/// A fully parsed query: a stream selector plus zero or more line filters,
/// applied left to right.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LogQlQuery {
    pub matchers: Vec<Matcher>,
    pub filters: Vec<LineFilter>,
}

/// Parse error with a byte offset into the original query string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub pos: usize,
    pub msg: String,
}

impl ParseError {
    fn new(pos: usize, msg: impl Into<String>) -> Self {
        Self {
            pos,
            msg: msg.into(),
        }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "logql parse error at byte {}: {}", self.pos, self.msg)
    }
}

impl std::error::Error for ParseError {}

/// Parse a full LogQL query string.
pub fn parse(input: &str) -> Result<LogQlQuery, ParseError> {
    let mut sc = Scanner::new(input);
    sc.skip_ws();
    let matchers = parse_selector(&mut sc)?;

    let mut filters = Vec::new();
    loop {
        sc.skip_ws();
        if sc.eof() {
            break;
        }
        filters.push(parse_filter(&mut sc)?);
    }

    Ok(LogQlQuery { matchers, filters })
}

struct Scanner<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Scanner<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn rest(&self) -> &'a str {
        &self.input[self.pos..]
    }

    fn eof(&self) -> bool {
        self.pos >= self.input.len()
    }

    fn peek(&self) -> Option<char> {
        self.rest().chars().next()
    }

    fn bump(&mut self) -> Option<char> {
        let c = self.peek()?;
        self.pos += c.len_utf8();
        Some(c)
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(c) if c.is_whitespace()) {
            self.bump();
        }
    }

    /// Consume `token` if the input starts with it, returning whether it did.
    fn consume(&mut self, token: &str) -> bool {
        if self.rest().starts_with(token) {
            self.pos += token.len();
            true
        } else {
            false
        }
    }
}

fn parse_selector(sc: &mut Scanner) -> Result<Vec<Matcher>, ParseError> {
    if !sc.consume("{") {
        return Err(ParseError::new(
            sc.pos,
            "expected '{' to start the stream selector",
        ));
    }
    sc.skip_ws();

    if sc.peek() == Some('}') {
        return Err(ParseError::new(
            sc.pos,
            "stream selector must have at least one matcher (empty '{}' is rejected)",
        ));
    }

    let mut matchers = Vec::new();
    loop {
        sc.skip_ws();
        let label = parse_label(sc)?;
        sc.skip_ws();
        let op = parse_match_op(sc)?;
        sc.skip_ws();
        let value = parse_string(sc)?;
        matchers.push(Matcher { label, op, value });

        sc.skip_ws();
        if sc.consume(",") {
            sc.skip_ws();
            if sc.peek() == Some('}') {
                break; // trailing comma before '}'
            }
            continue;
        }
        break;
    }

    sc.skip_ws();
    if !sc.consume("}") {
        return Err(ParseError::new(
            sc.pos,
            "expected ',' or '}' in stream selector",
        ));
    }
    Ok(matchers)
}

fn parse_label(sc: &mut Scanner) -> Result<String, ParseError> {
    let start = sc.pos;
    match sc.peek() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {
            sc.bump();
        }
        _ => return Err(ParseError::new(sc.pos, "expected a label name")),
    }
    while matches!(sc.peek(), Some(c) if c.is_ascii_alphanumeric() || c == '_') {
        sc.bump();
    }
    Ok(sc.input[start..sc.pos].to_string())
}

fn parse_match_op(sc: &mut Scanner) -> Result<MatchOp, ParseError> {
    if sc.consume("=~") {
        Ok(MatchOp::Match)
    } else if sc.consume("!~") {
        Ok(MatchOp::NotMatch)
    } else if sc.consume("!=") {
        Ok(MatchOp::Neq)
    } else if sc.consume("=") {
        Ok(MatchOp::Eq)
    } else {
        Err(ParseError::new(
            sc.pos,
            "expected one of '=', '!=', '=~', '!~'",
        ))
    }
}

fn parse_filter(sc: &mut Scanner) -> Result<LineFilter, ParseError> {
    let op = if sc.consume("|=") {
        LineFilterOp::Contains
    } else if sc.consume("|~") {
        LineFilterOp::Match
    } else if sc.consume("!~") {
        LineFilterOp::NotMatch
    } else if sc.consume("!=") {
        LineFilterOp::NotContains
    } else {
        return Err(ParseError::new(
            sc.pos,
            "expected a line filter ('|=', '!=', '|~', '!~') or end of input",
        ));
    };
    sc.skip_ws();
    let value = parse_string(sc)?;
    Ok(LineFilter { op, value })
}

fn parse_string(sc: &mut Scanner) -> Result<String, ParseError> {
    match sc.peek() {
        Some('"') => parse_double_quoted(sc),
        Some('`') => parse_raw(sc),
        _ => Err(ParseError::new(
            sc.pos,
            "expected a quoted string (\"...\" or `...`)",
        )),
    }
}

fn parse_double_quoted(sc: &mut Scanner) -> Result<String, ParseError> {
    let start = sc.pos;
    sc.bump(); // opening quote
    let mut out = String::new();
    loop {
        match sc.bump() {
            None => return Err(ParseError::new(start, "unterminated string literal")),
            Some('"') => return Ok(out),
            Some('\\') => {
                let esc_pos = sc.pos;
                match sc.bump() {
                    Some('"') => out.push('"'),
                    Some('\\') => out.push('\\'),
                    Some(_) => {
                        return Err(ParseError::new(
                            esc_pos,
                            r#"invalid escape sequence (only \" and \\ are supported)"#,
                        ))
                    }
                    None => return Err(ParseError::new(start, "unterminated string literal")),
                }
            }
            Some(c) => out.push(c),
        }
    }
}

fn parse_raw(sc: &mut Scanner) -> Result<String, ParseError> {
    let start = sc.pos;
    sc.bump(); // opening backtick
    let mut out = String::new();
    loop {
        match sc.bump() {
            None => return Err(ParseError::new(start, "unterminated raw string literal")),
            Some('`') => return Ok(out),
            Some(c) => out.push(c),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_matcher() {
        let q = parse(r#"{service="api"}"#).unwrap();
        assert_eq!(
            q,
            LogQlQuery {
                matchers: vec![Matcher {
                    label: "service".into(),
                    op: MatchOp::Eq,
                    value: "api".into(),
                }],
                filters: vec![],
            }
        );
    }

    #[test]
    fn multiple_matchers_and_ops() {
        let q = parse(r#"{ service = "api" , severity != "debug", host=~"web-.*" }"#).unwrap();
        assert_eq!(q.matchers.len(), 3);
        assert_eq!(q.matchers[0].op, MatchOp::Eq);
        assert_eq!(q.matchers[1].op, MatchOp::Neq);
        assert_eq!(q.matchers[2].op, MatchOp::Match);
        assert_eq!(q.matchers[2].value, "web-.*");
    }

    #[test]
    fn not_match_op() {
        let q = parse(r#"{service!~"api-.*"}"#).unwrap();
        assert_eq!(q.matchers[0].op, MatchOp::NotMatch);
    }

    #[test]
    fn trailing_comma_allowed() {
        let q = parse(r#"{service="api",}"#).unwrap();
        assert_eq!(q.matchers.len(), 1);
    }

    #[test]
    fn line_filters_chain() {
        let q =
            parse(r#"{service="api"} |= "error" != "debug" |~ "^fatal" !~ "ignore.*""#).unwrap();
        assert_eq!(q.filters.len(), 4);
        assert_eq!(q.filters[0].op, LineFilterOp::Contains);
        assert_eq!(q.filters[0].value, "error");
        assert_eq!(q.filters[1].op, LineFilterOp::NotContains);
        assert_eq!(q.filters[1].value, "debug");
        assert_eq!(q.filters[2].op, LineFilterOp::Match);
        assert_eq!(q.filters[2].value, "^fatal");
        assert_eq!(q.filters[3].op, LineFilterOp::NotMatch);
        assert_eq!(q.filters[3].value, "ignore.*");
    }

    #[test]
    fn double_quoted_escapes() {
        let q = parse(r#"{a="say \"hi\" and \\ backslash"}"#).unwrap();
        assert_eq!(q.matchers[0].value, r#"say "hi" and \ backslash"#);
    }

    #[test]
    fn backtick_raw_string_no_escapes() {
        let q = parse(r#"{a=`c:\path\to"thing`}"#).unwrap();
        assert_eq!(q.matchers[0].value, r#"c:\path\to"thing"#);
    }

    #[test]
    fn whitespace_tolerant() {
        let q = parse("  {  service = \"api\"  }   |=  \"x\"  ").unwrap();
        assert_eq!(q.matchers.len(), 1);
        assert_eq!(q.filters.len(), 1);
    }

    #[test]
    fn empty_selector_rejected() {
        let err = parse("{}").unwrap_err();
        assert!(err.msg.contains("at least one matcher"));
    }

    #[test]
    fn missing_opening_brace() {
        let err = parse(r#"service="api"}"#).unwrap_err();
        assert_eq!(err.pos, 0);
    }

    #[test]
    fn missing_closing_brace() {
        let err = parse(r#"{service="api""#).unwrap_err();
        assert!(err.msg.contains("','") || err.msg.contains("'}'"));
    }

    #[test]
    fn unterminated_string() {
        let err = parse(r#"{service="api}"#).unwrap_err();
        assert!(err.msg.contains("unterminated"));
    }

    #[test]
    fn invalid_escape() {
        let err = parse(r#"{service="a\qb"}"#).unwrap_err();
        assert!(err.msg.contains("invalid escape"));
    }

    #[test]
    fn invalid_operator() {
        let err = parse(r#"{service>"api"}"#).unwrap_err();
        assert!(err.msg.contains("expected one of"));
    }

    #[test]
    fn label_cannot_start_with_digit() {
        let err = parse(r#"{1abc="x"}"#).unwrap_err();
        assert!(err.msg.contains("label name"));
    }

    #[test]
    fn trailing_garbage_after_query() {
        let err = parse(r#"{service="api"} garbage"#).unwrap_err();
        assert!(err.msg.contains("line filter"));
    }

    #[test]
    fn unquoted_value_rejected() {
        let err = parse(r#"{service=api}"#).unwrap_err();
        assert!(err.msg.contains("quoted string"));
    }
}
