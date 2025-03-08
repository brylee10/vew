//! Infers the format of a table
//!
//! Infers properties such as the format type (visually aligned or CSV).
//! For visually aligned tables, primarily infers properties about the header, which then
//! determines how other data will be parsed. In particular, infers the header index
//! and header column widths.

use std::{collections::HashMap, ops::Range};

use log::debug;
use thiserror::Error;

/// Intersection over min (IOM) threshold for a row to be considered a header row
///
/// Value chosen to allow for some whitespace in data rows which decreases IOM.
/// Threshold chosen some trial and error.
pub(in crate::parsers) const HEADER_INTERSECTION_OVER_MIN_THRESHOLD: f64 = 0.50;
/// Allowed to merge at most `MAX_HEADER_MERGES` proportion of adjacent header columns
/// to form concatenated strings to increase the intersection over min in finding candidates
const MAX_HEADER_MERGES: f64 = 0.75;

/// Errors when inferring the type of the table
#[derive(Debug, Error)]
pub enum TableInferenceError {
    #[error("No rows provided. Did the input command to `vew` produce data?")]
    NoRowsProvided,
    #[error(
        "Inconsistent CSV column counts. File is probably a CSV file, but some rows have a different number of columns"
    )]
    InconsistentCSVColumnCounts,
    #[error("Constructing a delimited table with table inference expects a CSV input")]
    NotCSV,
}

/// Infers various properties about the table, such as the format, number of columns, column widths, etc.
/// using heuristics. Used when a delimiter is not explicitly provided.
#[derive(Debug)]
pub enum TableInference {
    Csv,
    FixedWidth(TableInferenceVisuallyAligned),
}

/// Inference result for a visually aligned table
#[derive(Debug)]
pub struct TableInferenceVisuallyAligned {
    pub header_column_widths: Vec<HeaderColumnWidth>,
    pub header_column_idx: usize,
}

impl TableInference {
    /// Constructs a new delimited table. In `TableInference`, this must be a CSV
    pub fn new_csv(rows: &[String]) -> Result<Self, TableInferenceError> {
        let is_csv = Self::infer_is_csv(rows)?;
        if !is_csv {
            return Err(TableInferenceError::NotCSV);
        }
        Ok(Self::Csv)
    }

    /// Constructs a new visually aligned table.
    /// `infer_header_idx` only applies to visually aligned tables
    pub fn new_visually_aligned(
        rows: &[String],
        infer_header_idx: bool,
    ) -> Result<Self, TableInferenceError> {
        if rows.is_empty() {
            return Err(TableInferenceError::NoRowsProvided);
        }
        let header_idx = if infer_header_idx {
            Self::infer_header_idx(rows)?
        } else {
            0
        };
        let header_column_widths = Self::infer_header_column_widths(rows, header_idx)?;
        Ok(Self::FixedWidth(TableInferenceVisuallyAligned {
            header_column_widths,
            header_column_idx: header_idx,
        }))
    }

    // Guesses if the table is CSV based on the first N rows
    // Heuristic: If at least `[CSV_INFER_THRESHOLD]` proportion of the rows have the same number of rows
    // when split by the delimiter, then it is CSV.
    // If not all of the rows have the same number of columns after splitting but most do, then an error is thrown
    // since the CSV is probably malformed.
    pub fn infer_is_csv(rows: &[String]) -> Result<bool, TableInferenceError> {
        if rows.is_empty() {
            return Err(TableInferenceError::NoRowsProvided);
        }
        // Every row must have a comma to be considered CSV
        if !rows.iter().all(|row| row.contains(',')) {
            return Ok(false);
        }

        let n_cols: Vec<usize> = rows.iter().map(|row| row.split(',').count()).collect();

        let mut counts = HashMap::new();
        for n_col in n_cols {
            *counts.entry(n_col).or_insert(0) += 1;
        }
        if let Some((_, &occurrences)) = counts.iter().max_by_key(|entry| entry.1) {
            if occurrences != rows.len() {
                // all rows should have the same number of columns
                Err(TableInferenceError::InconsistentCSVColumnCounts)
            } else {
                Ok(true)
            }
        } else {
            // no rows provided
            Err(TableInferenceError::NoRowsProvided)
        }
    }

    // This sets the header for the table, which can be used to parse all other rows.
    // This infers the number of columns in the header row and the width of each column based on the header row
    fn infer_header_column_widths(
        rows: &[String],
        header_idx: usize,
    ) -> Result<Vec<HeaderColumnWidth>, TableInferenceError> {
        let rows: &[String] = Self::skip_empty_lines(rows);
        let rows = &rows[header_idx..];
        let mut column_widths: Vec<Vec<ColumnWidth>> = rows
            .iter()
            .map(|row| infer_text_column_widths(row))
            .collect();
        let body_rows = column_widths.split_off(1);
        let header_row = column_widths
            .first()
            .ok_or(TableInferenceError::NoRowsProvided)?
            .clone();
        let merged_header = Self::merge_header_spaces(&header_row, &body_rows);
        let inferred_column_widths =
            Self::infer_header_column_widths_from_data(merged_header, &body_rows)?;
        Ok(inferred_column_widths)
    }

    fn infer_header_column_widths_from_data(
        header_row: Vec<ColumnWidth>,
        body_rows: &[Vec<ColumnWidth>],
    ) -> Result<Vec<HeaderColumnWidth>, TableInferenceError> {
        // find all rows with the same number of columns as the header row
        // these should generally be rows that do not have spaces
        let eq_column_len_indices: Vec<usize> = body_rows
            .iter()
            .enumerate()
            .filter(|(_, row)| row.len() == header_row.len())
            .map(|(i, _)| i)
            .collect();

        let mut inferred_column_widths = vec![];
        for (idx, header_col) in header_row.iter().enumerate() {
            // this represents a reasonable estimate of the column width based on the prefetched data
            let mut inferred_column_width = header_col.text.clone();
            let prev_header_col = if idx > 0 {
                Some(&header_row[idx - 1])
            } else {
                None
            };
            let next_header_col = if idx < header_row.len() - 1 {
                Some(&header_row[idx + 1])
            } else {
                None
            };
            // As long as this row column overlaps completely with the header and does not overlap with adjacent header columns,
            // take the union of the cell with the current candidate column width
            for body_row_idx in eq_column_len_indices.iter() {
                let body_col = &body_rows[*body_row_idx][idx];
                let cell_intersects_header_col = (body_col.text.start < header_col.text.end)
                    && (body_col.text.end > header_col.text.start);
                let cell_no_overlap_prev = prev_header_col.is_none()
                    || body_col.text.start >= prev_header_col.unwrap().text.end;
                let cell_no_overlap_next = next_header_col.is_none()
                    || body_col.text.end <= next_header_col.unwrap().text.start;
                if cell_intersects_header_col && cell_no_overlap_prev && cell_no_overlap_next {
                    inferred_column_width = union_ranges(&inferred_column_width, &body_col.text);
                }
            }
            inferred_column_widths.push(HeaderColumnWidth {
                column_width: header_col.clone(),
                inferred_width: inferred_column_width,
            });
        }

        // The inferred widths should also not overlap with each other. Any overlap is assumed to be ambiguous, so it is removed from both inferred widths
        for i in 1..inferred_column_widths.len() {
            let (left, right) = inferred_column_widths.split_at_mut(i);
            let prev = &mut left[i - 1];
            let curr = &mut right[0];
            let overlap = intersection_of_ranges(&curr.inferred_width, &prev.inferred_width);
            if !overlap.is_empty() {
                prev.inferred_width.end = overlap.start;
                curr.inferred_width.start = overlap.end;
            }
        }
        Ok(inferred_column_widths)
    }

    // The goal of header index inference is to reduce the need to use `command | tail -n +N` to manually filter out metadata rows
    // in certain command outputs. This approach uses some heuristics. These should generally work, but users can always manually
    // fallback to `command | tail -n +N` if not.
    //
    // Intuitively, the first row that, when split into columns, "lines up well" with the following data is probably the header.
    // "lines up well" is measured by a "point" system (so called "intersection over min", IOM). If a header gets enough points
    // relative to total number of possible points, then it is considered the header. The specifics of point calculation are elaborated in the
    // `calc_iom_per_column` function.
    //
    // Further, a row may be a header, but may have whitespace in the column names, leading to misalignment with subsequent rows. This function
    // iterately merges adjacent columns, up to some maximum number of merges, based on which had particularly steep drop offs in IOM to
    // increase the IOM.
    fn infer_header_idx(rows: &[String]) -> Result<usize, TableInferenceError> {
        let rows: &[String] = Self::skip_empty_lines(rows);
        let column_widths: Vec<Vec<ColumnWidth>> = rows
            .iter()
            .map(|row| infer_text_column_widths(row))
            .collect();
        // Default to the first non-empty row as the header if no others meet the threshold
        let mut header_idx = 0;
        'row_loop: for (i, row) in rows.iter().enumerate() {
            let candidate_string = row.clone();
            let mut candidate_header = infer_text_column_widths(&candidate_string);
            // Assumes only `MAX_HEADER_MERGES` proportion of columns can be merged. Not every column header could have had whitespace!
            let max_merges = (candidate_header.len() as f64 * MAX_HEADER_MERGES).ceil() as usize;
            for _ in 0..max_merges {
                let row_column_widths: &[Vec<ColumnWidth>] = &column_widths[i + 1..];
                let (per_col_points, per_col_total, overall_iom) =
                    calc_iom_per_column(&candidate_header, row_column_widths);
                debug!("overall_iom: {}", overall_iom);
                debug!("points: {:?}", per_col_points);
                debug!("total: {:?}", per_col_total);
                debug!("candidate_string: {:?}", candidate_string);

                if overall_iom >= HEADER_INTERSECTION_OVER_MIN_THRESHOLD {
                    // found desired header
                    header_idx = i;
                    break 'row_loop;
                } else {
                    // header is not good enough, try to merge columns to improve IOM
                    let per_col_iom = per_col_points
                        .iter()
                        .zip(per_col_total.iter())
                        .map(|(points, total)| points / total)
                        .collect::<Vec<_>>();
                    // find the column that should be merged with the prior column to form a new candidate header
                    let mut merged_header = candidate_header.clone();
                    // skip the first because it has no prior column to merge with
                    for (col_idx, col_iom) in per_col_iom.iter().enumerate() {
                        // we merge from left to right, trying to improve the first significant drops in IOM by merging
                        // merging should improve IOM because typically IOM decreases when a header is prematurely split and
                        // is improperly the header of a later column, leading to low IOM for all subsequent columns
                        if *col_iom < HEADER_INTERSECTION_OVER_MIN_THRESHOLD
                            && col_idx < candidate_header.len() - 1
                        {
                            // merge col_idx with col_idx + 1, replacing col_idx + 1
                            merged_header[col_idx].text = union_ranges(
                                &merged_header[col_idx].text,
                                &merged_header[col_idx + 1].text,
                            );
                            merged_header.remove(col_idx + 1);
                            break;
                        }
                    }
                    candidate_header = merged_header;
                }
            }
        }
        Ok(header_idx)
    }

    // Skip all empty lines at the start of the rows
    fn skip_empty_lines(rows: &[String]) -> &[String] {
        let mut idx = 0;
        while idx < rows.len() && rows[idx].trim().is_empty() {
            idx += 1;
        }
        &rows[idx..]
    }

    // Used to merge adjacent header columns which have spaces by inferring based on the body row content.
    // In particular if it spans two entire adjacent column headers).
    // Returns the merged header row, does not modify in place.
    // A simple example is:
    // ```
    //  Proto Local Address          Foreign Address        (state)
    //  tcp4  192.168.1.10.5432      192.168.1.50.65244     ESTABLISHED
    // ```
    // The first data row implies that "Local Address" is one column, and not "Local" and "Address"
    fn merge_header_spaces(
        header_row: &[ColumnWidth],
        body_rows: &[Vec<ColumnWidth>],
    ) -> Vec<ColumnWidth> {
        let mut new_headers = header_row.to_vec();
        for body_row in body_rows {
            let mut curr_header_idx = 0;
            for cell_width in body_row {
                while curr_header_idx < new_headers.len()
                    && cell_width.text.start > new_headers[curr_header_idx].text.end
                {
                    curr_header_idx += 1;
                }
                // last col, no more columns to merge with
                if curr_header_idx >= new_headers.len() - 1 {
                    break;
                }
                let curr_header_width = &new_headers[curr_header_idx];
                let next_header_width = &new_headers[curr_header_idx + 1];
                let body_text_contains_adj_headers = cell_width.text.start
                    <= curr_header_width.text.start
                    && cell_width.text.end >= next_header_width.text.end;
                if body_text_contains_adj_headers {
                    // merge the current header column with the next one
                    let text_range = union_ranges(&curr_header_width.text, &next_header_width.text);
                    let column_range =
                        union_ranges(&curr_header_width.column, &next_header_width.column);
                    new_headers[curr_header_idx] = ColumnWidth {
                        text: text_range,
                        column: column_range,
                    };
                    new_headers.remove(curr_header_idx + 1);
                }
            }
        }
        new_headers
    }
}

/// Calculates the "intersection over min (IOM)" for a candidate header over a set of rows with
/// respect to their text only. Intuitively, IOM measures how well a candidate header "contains"
/// each cell in each row in the given column. IOM ranges from 0 to 1 and can be interpreted as a
/// "point" value. The higher the IOM, the better the candidate header "contains" the cells. Every data
/// cell in the rows can contribute 1 point to the total IOM. The proportion of points captured by the
/// header is its overall IOM. If this crosses a threshold (`INTERSECTION_OVER_MIN_THRESHOLD`), then the
/// candidate header is selected as the header.
///
/// A candidate header and each row are iterated in a `zip`-like fashion. Each corresponding index is
/// assumed to be a matching column. With each of these corresponding columns, the intersection over min
/// is calculated, where intersection is the overlap in the text range of the candidate header and cell,
/// and the min is the min length of the candidate header and cell. Intuitively, this measures if the cell
/// totally contains the header column or vice versa.
///
/// IOM also incorporates a penalty for cells that intersect with the current column and adjacent columns.
/// Intuitively, this is because well-formed visually aligned tables should not allow data cells to span multiple
/// columns in the header (otherwise it would be difficult to visually parse). This likely means the header
/// candidate is not correct. The penalty increases the more characters overlap with adjacent columns. This is
/// because sometimes columns are slightly misaligned, so penalties for 1-2 characters are relatively low. But
/// many more characters indicate the selected header is probably misaligned and incorrect. Additionally, this
/// penalty is only applied if the row range intersects the current column. This is because sometimes data rows
/// have whitespace, causing a continual "lag" behind the corresponding column headers. In these cases, the
/// issue is not the header but the parseability of the row, so the header is not further penalized.
pub(in crate::parsers) fn calc_iom_per_column(
    candidate_header: &[ColumnWidth],
    rows: &[Vec<ColumnWidth>],
) -> (Vec<f64>, Vec<f64>, f64) {
    let max_cols = candidate_header
        .len()
        .max(rows.iter().map(|row| row.len()).max().unwrap_or(0));
    let mut per_col_points = vec![0.0; max_cols];
    let mut per_col_total = vec![0.0; max_cols];

    debug!("candidate_header: {:?}", candidate_header);
    for (row_idx, row) in rows.iter().enumerate() {
        debug!("Row idx: {}", row_idx);
        // if a header is longer than a row, then the IOM is 0 for the extra columns,
        // and the cell is counted as missed
        if candidate_header.len() > row.len() {
            for total in per_col_total
                .iter_mut()
                .take(candidate_header.len())
                .skip(row.len())
            {
                *total += 1.0;
            }
        }
        for (row_col_idx, row_col) in row.iter().enumerate() {
            debug!("Row col idx: {}", row_col_idx);
            if row_col_idx < candidate_header.len() {
                let col: &_ = &candidate_header[row_col_idx];
                let intersection = intersection_of_ranges(&col.text, &row_col.text);
                let intersect = intersection.len();
                let m = col.text.len().min(row_col.text.len());

                let mut penalty = 0.0;
                if intersect > 0 {
                    if row_col_idx > 0 {
                        let prev_col = &candidate_header[row_col_idx - 1];
                        let prev_intersection =
                            intersection_of_ranges(&row_col.text, &prev_col.text);
                        let prev_intersect = prev_intersection.len() as f64;
                        // penalty increases quadratically with the number of intersections
                        penalty += (prev_intersect) * (prev_intersect - 1.0);
                    }
                    if row_col_idx < candidate_header.len() - 1 {
                        let next_col = &candidate_header[row_col_idx + 1];
                        let next_intersection =
                            intersection_of_ranges(&row_col.text, &next_col.text);
                        let next_intersect = next_intersection.len() as f64;
                        penalty += (next_intersect) * (next_intersect - 1.0);
                    }
                    debug!("intersect: {}, penalty: {}", intersect, penalty);
                }
                let points = (intersect as f64 - penalty) / m as f64;
                debug!("points: {}", points);
                per_col_points[row_col_idx] += points;
            }
            // if the row is longer than the header, then the cell is counted as missed
            per_col_total[row_col_idx] += 1.0;
        }
    }
    let total = per_col_total.iter().sum::<f64>();
    let points = per_col_points.iter().sum::<f64>();
    let overall_iom = if total > 0.0 { points / total } else { 0.0 };
    (per_col_points, per_col_total, overall_iom)
}

/// Infer column boundaries from a single row line using whitespace as the delimiter
/// Accounts for multiple trailing spaces after each row. Populates the [ColumnWidth].
/// Returns a vector of infered column widths
pub(in crate::parsers) fn infer_text_column_widths(row: &str) -> Vec<ColumnWidth> {
    let mut columns = Vec::new();
    // whether current index is in the string of a header column
    let mut in_text = false;
    let mut passed_first_column = false;
    let mut text_start = 0;
    let mut text_end = 0;
    let mut column_start = 0;
    for (i, c) in row.char_indices() {
        if !c.is_whitespace() {
            if !in_text {
                if passed_first_column {
                    columns.push(ColumnWidth {
                        text: text_start..text_end,
                        column: column_start..i,
                    });
                }
                passed_first_column = true;
                in_text = true;
                text_start = i;
                column_start = i;
            }
            text_end = i + 1;
        } else if in_text {
            in_text = false;
            text_end = i;
        }
    }
    columns.push(ColumnWidth {
        text: text_start..text_end,
        column: column_start..row.len(),
    });
    columns
}

/// utility to find the intersection of two ranges
fn intersection_of_ranges<T: Ord + Copy>(a: &Range<T>, b: &Range<T>) -> Range<T> {
    let start = if a.start > b.start { a.start } else { b.start };
    let end = if a.end < b.end { a.end } else { b.end };
    start..end
}

/// utility to find the union of two ranges
fn union_ranges<T: Ord + Copy>(a: &Range<T>, b: &Range<T>) -> Range<T> {
    let start = if a.start < b.start { a.start } else { b.start };
    let end = if a.end > b.end { a.end } else { b.end };
    start..end
}

/// Column width for the header column with additional metadata
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct HeaderColumnWidth {
    pub column_width: ColumnWidth,
    /// The inferred width of each column based on the prefetched data rows.
    /// This is effectively the union of all the column widths in the data rows.
    /// This tries to make parsing more robust to whitespace in data rows for tables
    /// that are well aligned. This will be a superset of the `column_width` text range, but
    /// may not be a superset of the `column_width` `column` range. This is because the `column`
    /// range assumes text headers are left aligned. However, headers could be center or right aligned,
    /// for instance. `inferred_width` uses the data to infer the width of the columns.
    pub inferred_width: Range<usize>,
}

/// Represents the size of a column which takes up a contiguous range of characters (may have whitespace following the text) in a row
/// Identifies only the start and end indices of both the text and the column (including whitespace).
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ColumnWidth {
    /// `text` is the range of characters spanning the start of the non whitespace characters in this column
    /// to the end of the contiguous non whitespace text in this column. Exclusive of end (as `Range` is exclusive, unlike `InclusiveRange`)
    pub text: Range<usize>,
    /// `column` is a superset of the `text` range, and includes any whitespace padding
    /// This is from the start of the nonwhitespace text in this column to the start
    /// of the next contiguous nonwhitespace text. Exclusive of end
    pub column: Range<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;

    mod csv {
        use super::*;

        #[test]
        fn test_infer_is_csv() {
            // Check that a CSV table is inferred correctly in two differentcode paths
            let rows = vec!["1,2,3".to_string(), "4,5,6".to_string()];
            assert!(matches!(TableInference::infer_is_csv(&rows), Ok(true)));

            let table_inference = TableInference::new_csv(&rows).unwrap();
            assert!(matches!(table_inference, TableInference::Csv));
        }
    }

    #[test]
    fn test_infer_is_csv_inconsistent_column_counts() {
        let rows = vec!["1,2,3".to_string(), "4,5".to_string()];
        assert!(TableInference::infer_is_csv(&rows).is_err());
    }

    mod fixed_width {
        use super::*;

        #[test]
        fn test_infer_columns_trailing_spaces() {
            // 3 columns, 5 spaces then 3 spaces then 2 spaces after last column
            let header = "Name     Age   Gender  ";
            let columns = infer_text_column_widths(header);
            assert_eq!(columns.len(), 3);
            assert_eq!(
                columns[0],
                ColumnWidth {
                    text: 0..4,
                    column: 0..9
                }
            );
            assert_eq!(
                columns[1],
                ColumnWidth {
                    text: 9..12,
                    column: 9..15
                }
            );
            assert_eq!(
                columns[2],
                ColumnWidth {
                    text: 15..21,
                    column: 15..23
                }
            );
        }

        #[test]
        fn test_infer_columns_no_trailing_spaces() {
            // 1 column
            let header = "Name";
            let columns = infer_text_column_widths(header);
            assert_eq!(columns.len(), 1);
            assert_eq!(
                columns[0],
                ColumnWidth {
                    text: 0..4,
                    column: 0..4
                }
            );
        }

        #[test]
        fn test_infer_header_column_widths() {
            let rows = "\
Proto Local Address          Foreign Address        (state)
tcp4  215.110.80.17.17422    157.103.238.107.53515  ESTABLISHED
tcp4  066.211.16.118.41604   101.65.129.205.33126   ESTABLISHED
tcp4  197.097.20.05.30567    098.117.56.39.13832    ESTABLISHED
";
            let rows = rows
                .lines()
                .map(|line| line.to_string())
                .collect::<Vec<_>>();
            let header_widths = TableInference::infer_header_column_widths(&rows, 0).unwrap();
            assert_eq!(header_widths.len(), 4);
            assert_eq!(
                header_widths[0].column_width,
                ColumnWidth {
                    text: 0..5,
                    column: 0..6
                }
            );
            assert_eq!(
                header_widths[1].column_width,
                ColumnWidth {
                    text: 6..19,
                    column: 6..29,
                }
            );
            assert_eq!(
                header_widths[2].column_width,
                ColumnWidth {
                    text: 29..44,
                    column: 29..52
                }
            );
            assert_eq!(
                header_widths[3].column_width,
                ColumnWidth {
                    text: 52..59,
                    column: 52..59
                }
            );
        }
    }

    mod infer_header {
        use super::*;

        #[test]
        fn test_calc_iom_per_column_1() {
            let header = vec![
                ColumnWidth {
                    text: 0..4,
                    column: 0..4,
                },
                ColumnWidth {
                    text: 4..8,
                    column: 4..8,
                },
            ];
            let rows = vec![vec![ColumnWidth {
                text: 0..4,
                column: 0..4,
            }]];
            let (per_col_points, per_col_total, overall_iom) = calc_iom_per_column(&header, &rows);
            assert_eq!(per_col_points, vec![1.0, 0.0]);
            assert_eq!(per_col_total, vec![1.0, 1.0]);
            assert_eq!(overall_iom, 1.0 / 2.0);
        }

        #[test]
        fn test_calc_iom_per_column_2() {
            let header = vec![
                ColumnWidth {
                    text: 0..4,
                    column: 0..6,
                },
                ColumnWidth {
                    text: 6..10,
                    column: 6..20,
                },
            ];
            let rows = vec![vec![
                ColumnWidth {
                    text: 0..4,
                    column: 0..8,
                },
                ColumnWidth {
                    text: 8..12,
                    column: 8..15,
                },
            ]];
            let (per_col_points, per_col_total, overall_iom) = calc_iom_per_column(&header, &rows);
            assert_eq!(
                per_col_points,
                vec![4 as f64 / 4 as f64, 2 as f64 / 4 as f64]
            );
            assert_eq!(per_col_total, vec![1.0, 1.0]);
            assert_eq!(overall_iom, 1.5 as f64 / 2.0 as f64);
        }
        #[test]
        fn test_infer_header_idx() {
            // no fancy header ambiguity, the header is the first row
            let rows = "\
Proto Local Address          Foreign Address        (state)
tcp4  215.110.80.17.17422    157.103.238.107.53515  ESTABLISHED
tcp4  066.211.16.118.41604   101.65.129.205.33126   ESTABLISHED
tcp4  197.097.20.05.30567    098.117.56.39.13832    ESTABLISHED
";
            let rows = rows
                .lines()
                .map(|line| line.to_string())
                .collect::<Vec<_>>();
            let header_idx = TableInference::infer_header_idx(&rows).unwrap();
            assert_eq!(header_idx, 0);
        }

        #[test]
        fn test_ls_header_infer() {
            let rows = "\
total 6264904
Permissions   Links  Owner   Group   Size     Date Modified       Name
drwxr-xr-x    13     jdoe    staff   416      Apr  1 14:20        .
drwxr-xr-x    67     jdoe    staff   2144     Apr  1 10:05        ..
drwxr-xr-x    12     jdoe    staff   384      Apr  1 15:45        .git
-rw-r--r--    1      jdoe    staff   8        Apr  1 09:32        .gitignore
-rw-r--r--    1      jdoe    staff   106420   Apr  1 12:58        Cargo.lock
";
            let rows = rows
                .lines()
                .map(|line| line.to_string())
                .collect::<Vec<_>>();
            let header_idx = TableInference::infer_header_idx(&rows).unwrap();
            // the first row is a summary, the second is the correct header
            assert_eq!(header_idx, 1);
        }

        #[test]
        fn test_top_header_infer() {
            let rows = "\
Processes: 715 total, 2 running, 713 sleeping, 4333 threads 
2025/03/05 22:58:10
Load Avg: 3.05, 2.96, 2.60 
CPU usage: 6.26% user, 11.73% sys, 82.0% idle 
SharedLibs: 1115M resident, 175M data, 163M linkedit.
MemRegions: 929132 total, 22G resident, 375M private, 5886M shared.
PhysMem: 46G used (4267M wired, 4246M compressor), 17G unused.
VM: 343T vsize, 4915M framework vsize, 4488194(0) swapins, 17917129(0) swapouts.
Networks: packets: 18308651353/6681G in, 18204406308/6544G out.
Disks: 75768906/1283G read, 74441374/1381G written.

PID    COMMAND          %CPU TIME     #TH    #WQ #PORTS MEM   PURG  CMPRS PGRP  PPID  STATE    BOOSTS          
99618  ptpcamerad       0.0  00:00.06 2      1   38     2497K 0B    2160K 99618 1     sleeping  0[0]           
99617  mscamerad-xpc    0.0  00:00.03 2      1   26     2305K 0B    1920K 99617 1     sleeping  0[0]           
";
            let rows = rows
                .lines()
                .map(|line| line.to_string())
                .collect::<Vec<_>>();
            let header_idx = TableInference::infer_header_idx(&rows).unwrap();
            assert_eq!(header_idx, 11);
        }

        #[test]
        fn test_netstat_header_infer() {
            let rows = "\
Active Internet connections (including servers)
Proto Recv-Q Send-Q  Local Address          Foreign Address        (state)    
tcp4       0      0  10.131.139.22.23204    165.227.120.140.042    ESTABLISHED
tcp6       0      0  2600:4041:4498:2.63259 2a03:2880:f35a:8.3222  CLOSE_WAIT 
tcp4       0      0  127.0.0.1.44960        *.*                    LISTEN     
tcp4       0      0  *.7000                 *.*                    LISTEN     
tcp4       0      0  127.0.0.1.20002        *.*                    LISTEN     
tcp6       0      0  2607:b940:c:208b.56624 2606:4600:4400::.443   ESTABLISHED
tcp6       0      0  2607:b940:c:208b.56602 2a03:2380:f012:1.443   ESTABLISHED
tcp6       0      0  2607:b940:c:208b.56600 2001:4760:4860::.443   ESTABLISHED
udp46      0      0  *.50120                *.*                               
udp6       0      0  *.5353                 *.*                               
udp6       0      0  *.5353                 *.*                               
udp6       0      0  *.5353                 *.*                               
udp6       0      0  *.5353                 *.*                               
udp6       0      0  *.5353                 *.*                               
";
            let rows = rows
                .lines()
                .map(|line| line.to_string())
                .collect::<Vec<_>>();
            let header_idx = TableInference::infer_header_idx(&rows).unwrap();
            assert_eq!(header_idx, 1);
        }

        #[test]
        fn test_infer_header_column_widths_1() {
            // first column has data of length 5, so can infer to char 5
            // second column takes longest data text and can infer up to char 24
            // third column takes the header, up to char 28
            let rows = "\
PID    COMMAND          %CPU
99618  ptpcamerad       0.0
99052  Google Chrome He 0.0
99051  Google Chrome He 0.0
99014  CrashpadHandlerA    0.0
";

            let rows: Vec<String> = rows
                .lines()
                .map(|line| line.to_string())
                .collect::<Vec<_>>();
            let header_idx = 0;
            let header_column_widths =
                TableInference::infer_header_column_widths(&rows, header_idx).unwrap();
            assert_eq!(header_column_widths.len(), 3);

            let expected_header_column_widths = vec![
                HeaderColumnWidth {
                    column_width: ColumnWidth {
                        text: 0..3,
                        column: 0..7,
                    },
                    inferred_width: 0..5,
                },
                HeaderColumnWidth {
                    column_width: ColumnWidth {
                        text: 7..14,
                        column: 7..24,
                    },
                    inferred_width: 7..23,
                },
                HeaderColumnWidth {
                    column_width: ColumnWidth {
                        text: 24..28,
                        column: 24..28,
                    },
                    inferred_width: 24..30,
                },
            ];
            assert_eq!(header_column_widths, expected_header_column_widths);
        }

        #[test]
        fn test_infer_header_column_widths_2() {
            // tests oddly aligned columns
            // recal only values that do not intersect with
            // 1. adjacent headers
            // 2. intersect with the current header
            // count, and any overlap with adjacent inferred column widths are deemed ambiguous and removed from both adjacent inferred widths
            // e.g `_cmiodalassistants` overlaps with `92058`, so the overlap is ambiguous. `9.7` overlaps with `%MEM` so it does not count in the header column width.
            let rows = "\
USER               PID  %CPU %MEM      
testuser         92058  21.9  0.0 
_cmiodalassistants   495   9.7  0.0
testuser         40477   9.4  1.6
_cmiodalassistants 46219   6.2  0.0
";
            let rows = rows
                .lines()
                .map(|line| line.to_string())
                .collect::<Vec<_>>();
            let header_idx = 0;
            let header_column_widths =
                TableInference::infer_header_column_widths(&rows, header_idx).unwrap();
            assert_eq!(header_column_widths.len(), 4);
            let expected_header_column_widths = vec![
                HeaderColumnWidth {
                    column_width: ColumnWidth {
                        text: 0..4,
                        column: 0..19,
                    },
                    inferred_width: 0..17,
                },
                HeaderColumnWidth {
                    column_width: ColumnWidth {
                        text: 19..22,
                        column: 19..24,
                    },
                    inferred_width: 18..24,
                },
                HeaderColumnWidth {
                    column_width: ColumnWidth {
                        text: 24..28,
                        column: 24..29,
                    },
                    inferred_width: 24..28,
                },
                HeaderColumnWidth {
                    column_width: ColumnWidth {
                        text: 29..33,
                        column: 29..39,
                    },
                    inferred_width: 29..35,
                },
            ];
            for (expected, actual) in expected_header_column_widths
                .iter()
                .zip(header_column_widths.iter())
            {
                assert_eq!(expected.column_width, actual.column_width);
                assert_eq!(expected.inferred_width, actual.inferred_width);
            }
        }
    }
}
