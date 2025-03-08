//! Parses an input where cells are separated by a delimiter

use log::trace;

use crate::vew::{TableRow, VewError};

use super::Parser;

/// Parser for character separated input
pub struct DelimitedParser {
    delimiter: String,
}

impl DelimitedParser {
    pub fn new(delimiter: String) -> Self {
        Self { delimiter }
    }
}

impl Parser for DelimitedParser {
    fn parse(&self, input: &str) -> Result<TableRow, VewError> {
        if !input.contains(&self.delimiter) {
            return Err(VewError::NoDelimiterFound);
        }
        let table_row = TableRow::new(
            input
                .split(&self.delimiter)
                .map(|s| s.to_string())
                .collect(),
        );
        trace!("Parsed row: {:?}", table_row);
        Ok(table_row)
    }

    #[cfg(test)]
    fn delimiter(&self) -> Option<&str> {
        Some(&self.delimiter)
    }
}
