//! Parses a strings into table rows using whitespace as the delimiter
//!
//! Uses column widths (from a preparsed header) to more intelligently parse the row to avoid splitting
//! whitespaces into new columns when not necessary.

use log::trace;

use crate::vew::{TableRow, VewError};

use super::{
    Parser,
    inference::{ColumnWidth, HeaderColumnWidth, calc_iom_per_column, infer_text_column_widths},
};

/// Threshold for data rows to be parsed naively. Should be relatively high, otherwise can fall back to more involved parsing.
const DATA_INTERSECTION_OVER_MIN_THRESHOLD: f64 = 0.90;

/// Parses a whitespace separated table row into a `[TableRow]`
/// Uses `column_widths` to more intelligently parse the row to avoid splitting
/// whitespaces into new columns when not necessary (i.e. whitespace below the header text)
pub struct VisuallyAlignedTableParser {
    /// Width of each column in characters, as determined by the header
    header_col_widths: Vec<HeaderColumnWidth>,
}

impl VisuallyAlignedTableParser {
    pub fn new(header_col_widths: Vec<HeaderColumnWidth>) -> Self {
        Self { header_col_widths }
    }

    /// First try naively spitting the row by whitespace,
    /// if that is good enough (measured by IOM), then return that split. Otherwise return None
    /// and try a more involved method
    fn naive_parse(&self, input: &str) -> Result<Option<TableRow>, VewError> {
        let columns = infer_text_column_widths(input);
        let header_col_widths: Vec<ColumnWidth> = self
            .header_col_widths
            .iter()
            .map(|h| h.column_width.clone())
            .collect();
        let col_count_matches = columns.len() == self.header_col_widths.len();
        let (_, _, iom) = calc_iom_per_column(&header_col_widths, &[columns]);
        if col_count_matches && iom > DATA_INTERSECTION_OVER_MIN_THRESHOLD {
            let input = input.split_whitespace().map(|s| s.to_string()).collect();
            let table_row = TableRow::new(input);
            Ok(Some(table_row))
        } else {
            Ok(None)
        }
    }
}

impl Parser for VisuallyAlignedTableParser {
    /// Parses a row of text into a `[TableRow]` using whitespace as the delimiter
    fn parse(&self, input: &str) -> Result<TableRow, VewError> {
        if input.trim().is_empty() {
            return Err(VewError::EmptyRowFound);
        }
        // first try a naive parse on whitespace split
        if let Some(table_row) = self.naive_parse(input)? {
            return Ok(table_row);
        }

        let mut table_row = TableRow::new(Vec::new());
        // indicates if current index is in table data (i.e. not in whitespace)
        let mut in_data = false;
        let mut curr_col_idx = 0;
        let mut curr_col_width = self.header_col_widths[curr_col_idx].inferred_width.clone();
        // start index of current datum
        let mut start = 0;
        for (i, c) in input.char_indices() {
            let last_idx_in_range = curr_col_width.end <= i + 1;
            if !c.is_whitespace() {
                // encountered data
                if !in_data {
                    in_data = true;
                    start = i;
                }
            } else if last_idx_in_range && !in_data {
                // current index is past the end of this range (end is exclusive)
                // this datum was all whitespace
                // add empty cell
                table_row.add_cell(String::new());
                // move to next column
                start = i;
                curr_col_idx += 1;
                if curr_col_idx >= self.header_col_widths.len() {
                    // no more columns to parse
                    break;
                }
                curr_col_width = self.header_col_widths[curr_col_idx].inferred_width.clone();
            } else if c.is_whitespace() && i + 1 < curr_col_width.end && i >= curr_col_width.start {
                // This intends to avoid splitting whitespace into new columns when not necessarily (i.e. whitespace below the header text)
                //
                // the plus 1 means if this character is aligned with the column index of
                // the last character in the text header, then it should not considered part of the data
                // of the previous word. The space is intended to connect two adjacent words. If this is the last
                // char under the header, then there is no word to connect to. This accounts for scenarios like this:
                // ```
                // HEADER1 HEADER2
                // Value  Value 2
                // ```
                // Without +1, then "XValue" would be parsed with `Value1`
                // heuristic: even if encounter whitespace, if it is still within the header text,
                // it is part of the current column
                continue;
            } else if c.is_whitespace() && in_data {
                // encountered whitespace after data
                let is_last_col = curr_col_idx == self.header_col_widths.len() - 1;
                if is_last_col {
                    // treat last column as special, take the rest of the string
                    table_row.add_cell(input[start..].trim().to_string());
                    in_data = false;
                    break;
                } else {
                    // otherwise, move to the next column
                    table_row.add_cell(input[start..i].trim().to_string());
                    start = i;
                    in_data = false;
                    curr_col_idx += 1;
                    curr_col_width = self.header_col_widths[curr_col_idx].inferred_width.clone();
                }
            }
        }
        if in_data {
            // last datum did not have whitespace after it, take the whole string
            table_row.add_cell(input[start..].trim().to_string());
        }
        trace!("Parsed row: {:?}", table_row);
        Ok(table_row)
    }

    #[cfg(test)]
    fn delimiter(&self) -> Option<&str> {
        None
    }
}

#[cfg(test)]
mod visually_aligned_tests {
    use crate::parsers::inference::TableInference;

    use super::*;

    fn init_parser(main_text: &[String]) -> VisuallyAlignedTableParser {
        let inference = TableInference::new_visually_aligned(main_text, true).unwrap();
        match inference {
            TableInference::FixedWidth(inference) => {
                VisuallyAlignedTableParser::new(inference.header_column_widths)
            }
            _ => panic!("Expected fixed width inference"),
        }
    }

    #[test]
    fn test_top_parse() {
        // Validate `vew` can infer the `COMMAND` column width despite `Google Chrome He` having spaces
        let main_text = "\
PID    COMMAND          %CPU TIME     
99618  ptpcamerad       0.0  00:00.06  
99014  CrashpadHandlerA 0.0  00:00.30 
99051  Google Chrome He 0.0  01:10.48
";
        let main_text: Vec<String> = main_text.lines().map(|line| line.to_string()).collect();
        let parser = init_parser(&main_text);

        let expected_table_row = TableRow::new(vec![
            "99618".to_string(),
            "ptpcamerad".to_string(),
            "0.0".to_string(),
            "00:00.06".to_string(),
        ]);

        let table_row = parser.parse(&main_text[1]).unwrap();
        assert_eq!(table_row.cells().len(), 4);
        assert_eq!(table_row, expected_table_row);

        let expected_table_row = TableRow::new(vec![
            "99014".to_string(),
            "CrashpadHandlerA".to_string(),
            "0.0".to_string(),
            "00:00.30".to_string(),
        ]);

        let table_row = parser.parse(&main_text[2]).unwrap();
        assert_eq!(table_row.cells().len(), 4);
        assert_eq!(table_row, expected_table_row);

        let expected_table_row = TableRow::new(vec![
            "99051".to_string(),
            "Google Chrome He".to_string(),
            "0.0".to_string(),
            "01:10.48".to_string(),
        ]);
        let table_row = parser.parse(&main_text[3]).unwrap();
        assert_eq!(table_row.cells().len(), 4);
        assert_eq!(table_row, expected_table_row);
    }
    #[test]
    fn test_ps_parse() {
        let main_text = "\
USER         PID  %CPU %MEM      SIZE    RES   TTY  STAT STARTED      TIME COMMAND
alpha      31248  14.3  0.1 490322048  12672 s003  S+   22Feb24 123:45.67 app/server
root       98765   9.6  0.0 380215000  21984 s078  S+   Tue9AM  78:45.23   system
alpha      10456  12.2  1.2 201802500 789612   ??  S    Tue8AM   56:23.45 /opt/AppRunner
service     2378   4.8  0.5 320918400 299764   ??  Rs   15Feb24 1450:02.89 /bin/sys_process
";
        let main_text: Vec<String> = main_text.lines().map(|line| line.to_string()).collect();
        let parser = init_parser(&main_text);

        let expected_table_row = TableRow::new(vec![
            "alpha".to_string(),
            "31248".to_string(),
            "14.3".to_string(),
            "0.1".to_string(),
            "490322048".to_string(),
            "12672".to_string(),
            "s003".to_string(),
            "S+".to_string(),
            "22Feb24".to_string(),
            "123:45.67".to_string(),
            "app/server".to_string(),
        ]);

        let table_row = parser.parse(&main_text[1]).unwrap();
        assert_eq!(table_row.cells().len(), 11);
        assert_eq!(table_row, expected_table_row);

        let expected_table_row = TableRow::new(vec![
            "root".to_string(),
            "98765".to_string(),
            "9.6".to_string(),
            "0.0".to_string(),
            "380215000".to_string(),
            "21984".to_string(),
            "s078".to_string(),
            "S+".to_string(),
            "Tue9AM".to_string(),
            "78:45.23".to_string(),
            "system".to_string(),
        ]);

        let table_row = parser.parse(&main_text[2]).unwrap();
        assert_eq!(table_row.cells().len(), 11);
        assert_eq!(table_row, expected_table_row);

        let expected_table_row = TableRow::new(vec![
            "alpha".to_string(),
            "10456".to_string(),
            "12.2".to_string(),
            "1.2".to_string(),
            "201802500".to_string(),
            "789612".to_string(),
            "??".to_string(),
            "S".to_string(),
            "Tue8AM".to_string(),
            "56:23.45".to_string(),
            "/opt/AppRunner".to_string(),
        ]);

        let table_row = parser.parse(&main_text[3]).unwrap();
        assert_eq!(table_row.cells().len(), 11);
        assert_eq!(table_row, expected_table_row);

        let expected_table_row = TableRow::new(vec![
            "service".to_string(),
            "2378".to_string(),
            "4.8".to_string(),
            "0.5".to_string(),
            "320918400".to_string(),
            "299764".to_string(),
            "??".to_string(),
            "Rs".to_string(),
            "15Feb24".to_string(),
            "1450:02.89".to_string(),
            "/bin/sys_process".to_string(),
        ]);

        let table_row = parser.parse(&main_text[4]).unwrap();
        assert_eq!(table_row.cells().len(), 11);
        assert_eq!(table_row, expected_table_row);
    }

    #[test]
    fn test_ls_parse() {
        let main_text = "\
total 128
drwxr-xr-x  19 testuser staff   608 Mar  3 21:22 .
drwxr-xr-x   5 testuser staff   160 Mar  1 09:15 ..
-rw-r--r--   1 testuser staff  6148 Mar  3 21:22 .hidden_file
drwxr-xr-x  13 testuser staff   416 Mar  3 21:21 .config
-rw-r--r--   1 testuser staff   183 Feb 25 11:30 data.txt
-rw-r--r--   1 testuser staff  1062 Feb 25 11:30 app_config.toml
drwxr-xr-x   4 testuser staff   128 Feb 25 11:30 bin
-rw-r--r--   1 testuser staff  2965 Feb 25 11:30 README.md
";
        let main_text: Vec<String> = main_text.lines().map(|line| line.to_string()).collect();
        let parser = init_parser(&main_text);

        let expected_table_row = TableRow::new(vec![
            "drwxr-xr-x".to_string(),
            "19".to_string(),
            "testuser".to_string(),
            "staff".to_string(),
            "608".to_string(),
            "Mar".to_string(),
            "3".to_string(),
            "21:22".to_string(),
            ".".to_string(),
        ]);

        let table_row = parser.parse(&main_text[1]).unwrap();
        assert_eq!(table_row.cells().len(), 9);
        assert_eq!(table_row, expected_table_row);

        let expected_table_row = TableRow::new(vec![
            "-rw-r--r--".to_string(),
            "1".to_string(),
            "testuser".to_string(),
            "staff".to_string(),
            "2965".to_string(),
            "Feb".to_string(),
            "25".to_string(),
            "11:30".to_string(),
            "README.md".to_string(),
        ]);

        let table_row = parser.parse(&main_text[8]).unwrap();
        assert_eq!(table_row.cells().len(), 9);
        assert_eq!(table_row, expected_table_row);
    }

    #[test]
    fn test_netstat_an_parse() {
        let main_text = "\
Active Internet connections (including servers)
Proto Recv-Q Send-Q  Local Address          Foreign Address        (state)    
tcp4       0      0  127.0.0.1.5432         127.0.0.1.65244        ESTABLISHED
tcp4       0      0  127.0.0.1.65244        127.0.0.1.5432         ESTABLISHED
tcp4       0      0  127.0.0.1.5432         *.*                    LISTEN     
tcp4       0      0  127.0.0.1.631          *.*                    LISTEN     
tcp6       0      0  ::1.631                *.*                    LISTEN     
udp4       0      0  *.*                    *.*                               
udp6       0      0  *.546                  *.*                               
";
        // Skip the first line as it's a description, not part of the table
        let main_text: Vec<String> = main_text
            .lines()
            .skip(1)
            .map(|line| line.to_string())
            .collect();
        let parser = init_parser(&main_text);

        let expected_table_row = TableRow::new(vec![
            "tcp4".to_string(),
            "0".to_string(),
            "0".to_string(),
            "127.0.0.1.5432".to_string(),
            "127.0.0.1.65244".to_string(),
            "ESTABLISHED".to_string(),
        ]);

        let table_row = parser.parse(&main_text[1]).unwrap();
        assert_eq!(table_row.cells().len(), 6);
        assert_eq!(table_row, expected_table_row);

        let expected_table_row = TableRow::new(vec![
            "tcp4".to_string(),
            "0".to_string(),
            "0".to_string(),
            "127.0.0.1.5432".to_string(),
            "*.*".to_string(),
            "LISTEN".to_string(),
        ]);

        let table_row = parser.parse(&main_text[3]).unwrap();
        assert_eq!(table_row.cells().len(), 6);
        assert_eq!(table_row, expected_table_row);

        let expected_table_row = TableRow::new(vec![
            "udp4".to_string(),
            "0".to_string(),
            "0".to_string(),
            "*.*".to_string(),
            "*.*".to_string(),
            "".to_string(),
        ]);

        let table_row = parser.parse(&main_text[6]).unwrap();
        // assert_eq!(table_row.cells().len(), 6);
        assert_eq!(table_row, expected_table_row);
    }

    #[test]
    fn test_df_parse() {
        let main_text = "\
Filesystem     512-blocks      Used Available Capacity  iused    ifree    %iused  Mounted on
/dev/disk1s1s1  1942512496  71630864 322265080    19%  538548 1611325400    0%    /
devfs                 408       408         0   100%     706        0     100%    /dev
/dev/disk1s3    1942512496  19442240 322265080     6%       6 1611325400    0%     /System/Volumes/Preboot
/dev/disk1s5    1942512496    628664 322265080     1%      43 1611325400    0%     /System/Volumes/VM
/dev/disk1s6    1942512496  10493088 322265080     4%    1229 1611325400    0%     /System/Volumes/Update
/dev/disk1s2    1942512496 1517752240 322265080    83% 5347112 1611325400   0%     /System/Volumes/Data
";
        let main_text: Vec<String> = main_text.lines().map(|line| line.to_string()).collect();
        let parser = init_parser(&main_text);

        let expected_table_row = TableRow::new(vec![
            "/dev/disk1s1s1".to_string(),
            "1942512496".to_string(),
            "71630864".to_string(),
            "322265080".to_string(),
            "19%".to_string(),
            "538548".to_string(),
            "1611325400".to_string(),
            "0%".to_string(),
            "/".to_string(),
        ]);

        let table_row = parser.parse(&main_text[1]).unwrap();
        assert_eq!(table_row.cells().len(), 9);
        assert_eq!(table_row, expected_table_row);

        let expected_table_row = TableRow::new(vec![
            "devfs".to_string(),
            "408".to_string(),
            "408".to_string(),
            "0".to_string(),
            "100%".to_string(),
            "706".to_string(),
            "0".to_string(),
            "100%".to_string(),
            "/dev".to_string(),
        ]);

        let table_row = parser.parse(&main_text[2]).unwrap();
        assert_eq!(table_row.cells().len(), 9);
        assert_eq!(table_row, expected_table_row);

        let expected_table_row = TableRow::new(vec![
            "/dev/disk1s2".to_string(),
            "1942512496".to_string(),
            "1517752240".to_string(),
            "322265080".to_string(),
            "83%".to_string(),
            "5347112".to_string(),
            "1611325400".to_string(),
            "0%".to_string(),
            "/System/Volumes/Data".to_string(),
        ]);

        let table_row = parser.parse(&main_text[6]).unwrap();
        assert_eq!(table_row.cells().len(), 9);
        assert_eq!(table_row, expected_table_row);
    }

    #[test]
    fn test_lsof_parse() {
        let main_text = "\
COMMAND     PID   USER   FD   TYPE             DEVICE  SIZE/OFF     NODE NAME
systemd       1   root  cwd    DIR                8,1      4096        2 /
systemd       1   root  rtd    DIR                8,1      4096        2 /
systemd       1   root  txt    REG                8,1   1620224   535528 /usr/lib/systemd/systemd
systemd       1   root  mem    REG                8,1     18976   535354 /usr/lib/x86_64-linux-gnu/libuuid.so.1.3.0
bash       1234   user    0u   CHR              136,0       0t0        3 /dev/pts/0
bash       1234   user    1u   CHR              136,0       0t0        3 /dev/pts/0
bash       1234   user    2u   CHR              136,0       0t0        3 /dev/pts/0
chrome     2345   user   10u  IPv4            1234567       0t0      TCP localhost:45678->remote-host:https (ESTABLISHED)
";
        let main_text: Vec<String> = main_text.lines().map(|line| line.to_string()).collect();
        let parser = init_parser(&main_text);

        let expected_table_row = TableRow::new(vec![
            "systemd".to_string(),
            "1".to_string(),
            "root".to_string(),
            "cwd".to_string(),
            "DIR".to_string(),
            "8,1".to_string(),
            "4096".to_string(),
            "2".to_string(),
            "/".to_string(),
        ]);

        let table_row = parser.parse(&main_text[1]).unwrap();
        assert_eq!(table_row.cells().len(), 9);
        assert_eq!(table_row, expected_table_row);

        let expected_table_row = TableRow::new(vec![
            "systemd".to_string(),
            "1".to_string(),
            "root".to_string(),
            "txt".to_string(),
            "REG".to_string(),
            "8,1".to_string(),
            "1620224".to_string(),
            "535528".to_string(),
            "/usr/lib/systemd/systemd".to_string(),
        ]);

        let table_row = parser.parse(&main_text[3]).unwrap();
        assert_eq!(table_row.cells().len(), 9);
        assert_eq!(table_row, expected_table_row);

        let expected_table_row = TableRow::new(vec![
            "bash".to_string(),
            "1234".to_string(),
            "user".to_string(),
            "0u".to_string(),
            "CHR".to_string(),
            "136,0".to_string(),
            "0t0".to_string(),
            "3".to_string(),
            "/dev/pts/0".to_string(),
        ]);

        let table_row = parser.parse(&main_text[5]).unwrap();
        assert_eq!(table_row.cells().len(), 9);
        assert_eq!(table_row, expected_table_row);

        let expected_table_row = TableRow::new(vec![
            "chrome".to_string(),
            "2345".to_string(),
            "user".to_string(),
            "10u".to_string(),
            "IPv4".to_string(),
            "1234567".to_string(),
            "0t0".to_string(),
            "TCP".to_string(),
            "localhost:45678->remote-host:https (ESTABLISHED)".to_string(),
        ]);

        let table_row = parser.parse(&main_text[8]).unwrap();
        assert_eq!(table_row.cells().len(), 9);
        assert_eq!(table_row, expected_table_row);
    }
}
