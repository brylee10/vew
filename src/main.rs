//! `vew` (sounds like "view", but typeable with one hand) is a CLI tool for viewing simple tabular
//! data in a graphical user interface for easier interactivity.
//!
//! `vew` can be used to view, filter, and sort the contents of visually aligned tables (e.g. outputs of `ps`, `netstat`, `ls`) or
//! character delimited files (e.g. CSV, TSV, colon delimited)
//!
//! # Example usage:
//!
//! ## Inferred mode with visually aligned tables
//! Typical "nicely formatted" visually aligned tables
//! ```sh
//! # Convert well-formed textual shell command outputs into a table (these output as "standard" tables)
//! ps aux | vew
//! # `lsof` is block bufferred when connected to a pipe, use `unbuffer` to line buffer
//! unbuffer lsof -i | vew
//! # View filesystem resource usage
//! df | vew
//! du | vew
//! ```
//!
//! Note: if taking input from a pipe (e.g. `lsof -i | vew`), note that certain commands are
//! block buffered when the stdout is not an interactive terminal (they are otherwise line buffered
//! when connected to a terminal). This means the output may not be immediately sent to `vew` via the pipe.
//! Using `unbuffer` on Mac or `stdbuf -oL` can enable line buffering.
//!
//! Using header inference to skip metadata using `vew -i`
//! ```sh
//! # `ls` provides a summary statistic at the beginning of the command which is not part of the table
//! # `ls` does not provide a header headers, so add placeholders too
//! ls -al . | vew -i -p
//!
//! # `top` has metadata at the beginning and end of the command which is not part of the table
//! # `top` also typically polls and refreshes periodically. Instead, only display the first sample.
//! top -l 1 | vew -i
//!
//! # `netstat` has spaces in the headers, must use other rows to infer the columns
//! # `netstat` also has a non header title before the
//! netstat -an | vew -i
//! ```
//!
//! ## Inferred mode with CSV
//! Provide a valid CSV and `vew` can infer the file type as a CSV and parse it
//! ```sh
//! //! # View the contents of a CSV file
//! cat data.csv | vew
//! ```
//!
//! ## Explicit delimiter
//! Parse a `:` separated file. Similar to the unix `column` command which can parse a delimiter
//! separated file (while also skipping rows without the delimiter).
//! ```sh
//! # `/etc/passwd` does not have a header, so we must provide a placeholder header
//! # it also has comments which should be ignored. The separator is a colon (:)
//! cat /etc/passwd | vew -d ':'
//! ```
//!
//! # Modes
//! `vew` supports an "inference" mode or an explicit delimiter mode.
//!
//! In "inference" mode, which is the default, `vew` can take either a CSV or
//! visually aligned table (e.g. output of `ps`, `ls`, `netstat`, etc.). `vew` will use the presence of a comma delimiter to
//! differentiate between the two types of tables. For visually aligned tables, `vew` further uses heuristics to infer which
//! row is the header, the column widths, etc. `vew` should generally "just work" for either CSVs, or visually aligned tables
//! which are "nicely" visually displayed (i.e. a table with a header, consistent number of columns, space separated values,
//! columns that are vertically aligned). Inference mode can be enhanced with additional heuristics (e.g. inferring the header index)
//! via the CLI args.
//!
//! Delimited mode is toggled when an explicit delimiter is provided (e.g. `-d ':'`) via the CLI args. In this case, no inference is
//! used and input is simply parsed into columns via the provided delimiter.
//!
//! ## Design
//! For all tables, `vew` streams all the data into memory across multiple threads to speed up load times for large tables or
//! otherwise long-running programs (e.g. `lsof` can quickly load many programs and file entries, but resolving the long tail
//! of files can take some time). `vew` also uses the very useful `egui` crate to render the GUI, and in particular only renders
//! the subset of the table that is viewable in the scroll area with pagination to minimize UI lag and scale to larger tables.
//! In local tests, displaying a CSV with 1M rows and 100 columns can render, scroll, and filter fairly smoothly. One limitation
//! is memory given `vew` will load the entire table in memory. However, note that filtering and sorting tables will cause the
//! `egui` table view to rerender, will take time (several seconds for a table with 1M rows/100 cols or 10M rows/10 cols, for
//! example).
//!
//! Unless a delimiter is explicitly specified, `vew` infers the type of the input stream from the input data, selecting between
//! either a csv or visually aligned table.
//!
//! For delimited files (e.g. CSVs), `vew` simply ensures all rows have the delimiter and sanity checks that the number of
//! columns is constant throughout the dataset and uses the first row as a header and the remainder as data.
//!
//! For visually aligned tables, `vew` uses heuristics to identify table headers and columns. While `vew` cannot infer the layout
//! of all possible tables, it tries to be robust to certain forms of white space in headers and data (i.e. not unnecessarily
//! splitting headers or data), and also tries to infer which input line is the header, since some programs output metadata prior
//! to the main tabular output (e.g. `netstat`, `top`) so the first row is not always the header. `vew` also tries to be robust
//! to certain column misalignments (e.g. `ps aux` output). The implementation specifics can be seen in `inference.rs`, but the
//! heuristics are relatively intuitive.
//!
//! For whitespace in headers, `vew` first preloads the first portion of the table so it can use index overlap with datum in
//! each row to infer whether a header with whitespace is multiple entries or not. For whitespace in datum, the reverse is done
//! but with the header text. `vew` also uses the collective span of data in that column from the header and row to infer the
//! visual span of the column. In that span, `vew` does not split text with spaces into multiple columns and assumes it
//! corresponds to the same column. For inferring the start of a table (i.e. identifying the header), `vew` takes each row in
//! turn and calculates the "intersection over min" metric of each candidate header and datum. If this crosses a threshold, then
//! the row is likely a header and not metadata. `vew` also iteratively merges adjacent space-separated header elements to try
//! to improve this metric. This process is iterated until a row is found with a sufficiently high metric value.
//!
//! ### Heuristic Limitations
//! There are **limitations** to these heuristics and certain tables may not be loaded as a human would infer due to factors such
//! as column misalignment or abnormal white spacing. Additionally, `vew` cannot intelligently group columns together without a
//! shared header.
//!
//! For example, in `ls -al .` output shown above, `vew` does not group the timestamp column into one column but instead splits
//! it into multiple. In these scenarios, you can use other tools such as `grep`, `awk`, or built-in filtering/sorting CLI args
//! in the respective commands if available.
//!
//! In most cases, `vew` should infer the correct table output, but double-check the underlying data or use a different tool if
//! there is a discrepancy. The goal of `vew` is to be a useful tool in addition to these other classic tools.

use anyhow::Result;
use clap::Parser;
use runner::run_vew;

mod parsers;
mod runner;
mod vew;

/// `vew` is a CLI tool for viewing simple tabular data in a graphical user interface for easier interactivity
#[derive(Debug, Parser)]
struct Args {
    /// Delimiter for string separated input. If not provided, `vew` will attempt to infer the format as either
    /// a CSV or a visually aligned table.
    #[arg(short, long)]
    delimiter: Option<String>,
    /// Add a placeholder header. Should be used when the first line is the first data row and not a header
    /// Placeholder will be "Col 1", "Col 2", etc.
    #[arg(short, long)]
    placeholder_header: bool,
    /// Experimental feature which infers the header index. This tries to skip text at the beginning of the table
    /// which may be metadata or summary statistics. Typically works, but may not account for all cases.
    /// Only applies to tables that do not use delimiters.
    /// If not specified, the header will be the first row read in the table.
    #[arg(short, long)]
    infer_header: bool,
}

/// Primary entrypoint for `vew`
#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    run_vew(args).await
}
