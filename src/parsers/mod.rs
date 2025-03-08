//! Parsers for different input formats

use crate::vew::{TableRow, VewError};

pub mod delimited;
pub mod inference;
pub mod visually_aligned;

/// Trait for parsing input strings into table rows
pub trait Parser: Send + Sync {
    /// Parses one row of input into a row (does not distinguish between header or data)
    fn parse(&self, input: &str) -> Result<TableRow, VewError>;
    #[cfg(test)]
    /// Returns the delimiter used by the parser
    fn delimiter(&self) -> Option<&str>;
}
