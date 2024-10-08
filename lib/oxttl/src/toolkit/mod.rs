//! oxttl parsing toolkit.
//!
//! Provides the basic code to write plain Rust lexers and parsers able to read files chunk by chunk.

mod chunker;
mod error;
mod lexer;
mod parser;

pub use self::chunker::{get_ntriples_file_chunks, get_turtle_file_chunks};
pub use self::error::{TextPosition, TurtleParseError, TurtleSyntaxError};
pub use self::lexer::{Lexer, TokenRecognizer, TokenRecognizerError};
#[cfg(feature = "async-tokio")]
pub use self::parser::FromTokioAsyncReadIterator;
pub use self::parser::{
    FromReadIterator, FromSliceIterator, Parser, RuleRecognizer, RuleRecognizerError,
};
