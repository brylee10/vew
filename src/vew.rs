//! Creates the Vew GUI

use crate::parsers::{
    Parser,
    delimited::DelimitedParser,
    inference::{TableInference, TableInferenceError},
    visually_aligned::VisuallyAlignedTableParser,
};
use anyhow::Result;
use bytesize::ByteSize;
use eframe::{App, Frame};
use egui::{
    Align, Button, Color32, FontId, Id, Layout, PopupCloseBehavior, RichText, ScrollArea, Ui,
    emath::OrderedFloat, popup_below_widget,
};
use egui_extras::{Column, TableBuilder};
use log::{debug, error};
use regex::Regex;
use std::{
    cmp::Ordering,
    collections::HashMap,
    io::{self},
    ops::{Deref, Range},
    str::FromStr,
    sync::{
        Arc, RwLock,
        atomic::{self, AtomicBool, AtomicUsize},
    },
    thread,
    time::Instant,
};
use thiserror::Error;
use tokio::sync::{
    broadcast::error::RecvError,
    mpsc::{UnboundedReceiver, error::SendError},
};

/// Convert chars to logical pixels (determines column width)
pub const DEFAULT_FILTER_ICON_WIDTH: f32 = 20.0;
/// Height of the footer below the table
pub const FOOTER_HEIGHT: f32 = 30.0;
/// Height of each row in the table
pub const ROW_HEIGHT: f32 = 20.0;
/// Number of rows per page
pub const ROWS_PER_PAGE: usize = 1_000;
/// Width of the pagination input field
pub const PAGINATION_WIDTH: f32 = 150.0;
/// When sorting, ignore these characters so the column can be sorted numerically
const STRIP_CHARACTERS: &[char] = &['%'];

/// Thread safe reference to the table data
type SharedTableData = Arc<RwLock<TableData>>;
/// Thread safe reference to the parser
type SharedParser = Arc<RwLock<Box<dyn Parser>>>;

/// Represents a column index or row index
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Index(usize);

impl Deref for Index {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Errors that can occur when running `vew`
#[derive(Debug, Error)]
pub enum VewError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Send(#[from] SendError<String>),
    #[error(transparent)]
    Recv(#[from] RecvError),
    #[error(transparent)]
    Regex(#[from] regex::Error),
    #[error(transparent)]
    TableInference(#[from] TableInferenceError),
    #[error("No delimiter found in input")]
    NoDelimiterFound,
    #[error("Empty row found in input")]
    EmptyRowFound,
}

/// Represents the data in the table
#[derive(Debug)]
pub struct TableData {
    header: TableRow,
    rows: Vec<TableRow>,
}

impl TableData {
    pub fn new() -> Self {
        Self {
            header: TableRow::new(Vec::new()),
            rows: Vec::new(),
        }
    }

    pub fn set_header(&mut self, header: TableRow) {
        self.header = header;
    }

    pub fn add_row(&mut self, row: TableRow) {
        self.rows.push(row);
    }
}

/// Represents a row in the table
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRow {
    cells: Vec<String>,
}

impl TableRow {
    pub fn new(cells: Vec<String>) -> Self {
        Self { cells }
    }

    pub fn add_cell(&mut self, cell: String) {
        self.cells.push(cell);
    }

    pub fn cells(&self) -> &[String] {
        &self.cells
    }
}

/// Direction of the sort
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortDirection {
    Ascending,
    Descending,
    None,
}

impl SortDirection {
    fn toggle(&self) -> Self {
        match self {
            SortDirection::None => SortDirection::Ascending,
            SortDirection::Ascending => SortDirection::Descending,
            SortDirection::Descending => SortDirection::None,
        }
    }

    fn arrow_symbol(&self) -> &str {
        match self {
            SortDirection::Ascending => " ⬆",
            SortDirection::Descending => " ⬇",
            SortDirection::None => "",
        }
    }
}

/// Vew GUI app
pub struct VewApp {
    /// Data in the table
    pub data: SharedTableData,
    /// Parser for the data
    pub parser: SharedParser,
    /// Supports multiple simultaneous filters, one per column
    filters: HashMap<Index, String>,
    /// Filter regexes, one per column
    filter_regexes: HashMap<Index, Regex>,
    /// contains the indices (in sorted order by a column, if applicable) of the rows which match the filters
    filtered_row_indices: Arc<RwLock<Vec<Index>>>,
    /// Only supports one column sort at a time
    sort_col: Option<Index>,
    /// Direction of the sort
    sort_direction: SortDirection,
    /// Error loading the data
    loading_error: Option<String>,
    /// Used to check if data loading was successful and child thread failures aren't silently ignored
    /// polled every update
    data_status_rx: UnboundedReceiver<Result<()>>,
    /// Current page in pagination
    current_page: usize,
    /// User input for pagination
    page_input: String,
    /// Indicates if the vew has changed since the last render
    change_manager: Arc<VewChangeManager>,
}

impl VewApp {
    pub fn new(
        delimiter: Option<String>,
        rows: Vec<String>,
        infer_header_idx: bool,
        data_status_rx: UnboundedReceiver<Result<()>>,
        use_placeholder_header: bool,
    ) -> Result<Self, VewError> {
        let (parser, data) = Self::init_table_data(delimiter, rows, infer_header_idx)?;
        if use_placeholder_header {
            Self::swap_header(Arc::clone(&data))?;
        }
        Ok(Self {
            data,
            parser,
            filters: HashMap::new(),
            filter_regexes: HashMap::new(),
            filtered_row_indices: Arc::new(RwLock::new(Vec::new())),
            sort_col: None,
            sort_direction: SortDirection::None,
            loading_error: None,
            data_status_rx,
            current_page: 0,
            page_input: "1".to_string(),
            change_manager: Arc::new(VewChangeManager::new()),
        })
    }

    fn init_table_data(
        delimiter: Option<String>,
        rows: Vec<String>,
        infer_header_idx: bool,
    ) -> Result<(SharedParser, SharedTableData), VewError> {
        // `vew` has two modes:
        // 1. Parse the data using a provided delimiter
        // 2. Infer the column dimensions and parse the previewed data into the data table
        let (parser, header_idx) = match delimiter {
            Some(delimiter) => {
                let parser: SharedParser =
                    Arc::new(RwLock::new(Box::new(DelimitedParser::new(delimiter))));
                // delimited files always have first row as header
                (parser, 0)
            }
            None => {
                // Table inference can either be a CSV or visually aligned table
                let is_csv: bool = TableInference::infer_is_csv(&rows)?;
                let table_inference = if is_csv {
                    TableInference::new_csv(&rows)?
                } else {
                    TableInference::new_visually_aligned(&rows, infer_header_idx)?
                };
                let (parser, header_idx): (SharedParser, usize) = match table_inference {
                    TableInference::Csv => (
                        Arc::new(RwLock::new(Box::new(DelimitedParser::new(",".to_string())))),
                        // delimited files always have first row as header
                        0,
                    ),
                    TableInference::FixedWidth(inference) => (
                        Arc::new(RwLock::new(Box::new(VisuallyAlignedTableParser::new(
                            inference.header_column_widths,
                        )))),
                        inference.header_column_idx,
                    ),
                };
                (parser, header_idx)
            }
        };

        let mut has_set_header = false;
        let data = Arc::new(RwLock::new(TableData::new()));
        // start parsing from header onwards, skipping metadata (if configured)
        for row in rows.iter().skip(header_idx) {
            let row = match parser.read().unwrap().parse(row) {
                Ok(row) => row,
                Err(VewError::NoDelimiterFound) => {
                    // skip rows with no valid delimiter for the delimiter parser
                    // like with unix `column`, this is not an error, quietly skip
                    debug!(
                        "Skipping row with no valid delimiter for the delimiter parser: {:?}",
                        row
                    );
                    continue;
                }
                Err(VewError::EmptyRowFound) => {
                    // skip empty rows
                    debug!("Skipping empty row: {:?}", row);
                    continue;
                }
                Err(e) => return Err(e),
            };
            if !has_set_header {
                data.write().unwrap().set_header(row);
                has_set_header = true;
            } else {
                data.write().unwrap().add_row(row);
            }
        }
        Ok((parser, data))
    }

    /// If using a placeholder header, the first parsed row will be the first data row but will be parsed as a "header".
    /// This is useful for determining if the file is a CSV or fixed width, and also the number of columns.
    /// Place this `[TableRow]` into the data table and replace the header with a placeholder header.
    fn swap_header(data: SharedTableData) -> Result<(), VewError> {
        let n_cols = data.read().unwrap().header.cells.len();
        let placeholder_header = (0..n_cols).map(|i| format!("Col {}", i + 1)).collect();
        let placeholder_header = TableRow::new(placeholder_header);

        let curr_header = data.read().unwrap().header.clone();
        // unwrap: if lock is poisoned, exit
        let mut data = data.write().unwrap();
        data.add_row(curr_header);
        data.set_header(placeholder_header);
        Ok(())
    }

    fn update_filtered_indices(&mut self) -> Result<(), VewError> {
        let updating_filter = self.change_manager.updating_filter.compare_exchange(
            false,
            true,
            atomic::Ordering::SeqCst,
            atomic::Ordering::SeqCst,
        );
        // The filter is already being updated, so we don't need to do it again
        if !matches!(updating_filter, Ok(false)) {
            return Ok(());
        }

        let filter_regexes = self.filter_regexes.clone();
        let sort_col = self.sort_col;
        let data = Arc::clone(&self.data);
        let filtered_row_indices = Arc::clone(&self.filtered_row_indices);
        let change_manager = Arc::clone(&self.change_manager);
        let sort_direction = self.sort_direction;
        thread::spawn(move || {
            let mut indices = Vec::new();
            {
                // reset the filter changed flag
                change_manager
                    .filter_changed
                    .store(false, atomic::Ordering::SeqCst);
                let data = data.read().unwrap();
                change_manager
                    .last_data_row_count
                    .store(data.rows.len(), atomic::Ordering::SeqCst);
                // Filter rows based on regex patterns for each column
                for (row_idx, row) in data.rows.iter().enumerate() {
                    let mut row_matches = true;

                    for (&col_idx, filter_regex) in filter_regexes.iter() {
                        if *col_idx < row.cells.len()
                            && !filter_regex.is_match(&row.cells[*col_idx])
                        {
                            row_matches = false;
                            break;
                        }
                    }

                    if row_matches {
                        indices.push(Index(row_idx));
                    }
                }
            }

            // Sort filtered indices if a sort column is set
            // reset the sort changed flag
            change_manager
                .sort_changed
                .store(false, atomic::Ordering::SeqCst);

            let starting_sort_col = sort_col;
            if let Some(&sort_col) = starting_sort_col.as_ref() {
                let start = Instant::now();
                let mut index_keys: Vec<(Index, SortKey)> = {
                    let data = data.read().unwrap();
                    // do all string to f64 parsing and grabbing data first instead of on each comparison
                    // this can increase memory usage but decrease runtime
                    indices
                        .iter()
                        .map(|&i| {
                            if sort_col.0 >= data.rows[i.0].cells.len() {
                                // default to an empty string for rows which do not have a value for the sort column
                                return (i, SortKey::Text("".to_string()));
                            }
                            let cell = &data.rows[i.0].cells[sort_col.0];
                            let cell = strip_end_chars(cell, STRIP_CHARACTERS);
                            let key = if let Ok(num) = cell.parse::<f64>() {
                                SortKey::Numeric(OrderedFloat(num))
                            } else if let Ok(bytesize) = ByteSize::from_str(cell) {
                                SortKey::Bytesize(bytesize)
                            } else {
                                SortKey::Text(cell.to_string())
                            };
                            (i, key)
                        })
                        .collect()
                };
                let header_len = {
                    let data = data.read().unwrap();
                    data.header.cells.len()
                };

                if *sort_col < header_len {
                    index_keys.sort_by(|a, b| match sort_direction {
                        SortDirection::Ascending => a.1.cmp(&b.1),
                        SortDirection::Descending => b.1.cmp(&a.1),
                        SortDirection::None => Ordering::Equal,
                    });
                    indices = index_keys.into_iter().map(|(i, _)| i).collect();
                }
                let duration = start.elapsed();
                debug!("Sorting filtered indices took {:?}", duration);
            }
            *filtered_row_indices.write().unwrap() = indices;
            change_manager
                .updating_filter
                .store(false, atomic::Ordering::SeqCst);
        });
        Ok(())
    }

    /// Set a filter for a column based on a regex pattern
    fn set_column_filter(&mut self, col_idx: usize, pattern: String) -> Result<(), VewError> {
        if pattern.is_empty() {
            self.filters.remove(&Index(col_idx));
            self.filter_regexes.remove(&Index(col_idx));
        } else {
            let regex = Regex::new(&pattern)?;
            self.filters.insert(Index(col_idx), pattern);
            self.filter_regexes.insert(Index(col_idx), regex);
        }
        self.change_manager
            .filter_changed
            .store(true, atomic::Ordering::SeqCst);

        self.update_filtered_indices()?;
        Ok(())
    }

    /// Render the header of the table with responsive buttons for sorting and filtering
    fn render_header(&mut self, mut egui_header: egui_extras::TableRow) {
        let data = Arc::clone(&self.data);
        let data = data.read().unwrap();
        for (idx, header) in data.header.cells.iter().enumerate() {
            let mut text = header.to_string();
            // add sort indicator if applicable
            if Some(Index(idx)) == self.sort_col {
                text.push_str(self.sort_direction.arrow_symbol());
            }

            // Headers highlight if filtered
            let text = if self.filters.contains_key(&Index(idx)) {
                RichText::new(text).color(Color32::LIGHT_BLUE)
            } else {
                RichText::new(text)
            };
            let column_header = |ui: &mut Ui| {
                let response = ui.button(text);

                if response.clicked() {
                    if Some(Index(idx)) == self.sort_col {
                        self.sort_direction = self.sort_direction.toggle();
                        if self.sort_direction == SortDirection::None {
                            self.sort_col = None;
                        }
                    } else {
                        self.sort_col = Some(Index(idx));
                        self.sort_direction = SortDirection::Ascending;
                    }

                    self.change_manager
                        .sort_changed
                        .store(true, atomic::Ordering::SeqCst);
                    if let Err(e) = self.update_filtered_indices() {
                        self.loading_error = Some(format!("Error updating sort: {}", e));
                    }
                }

                // render filter popup
                let popup_id = Id::new(format!("filter_popup_{}", idx));
                let filter_icon =
                    ui.add_sized([DEFAULT_FILTER_ICON_WIDTH, 20.0], Button::new("🔍"));
                if filter_icon.clicked() {
                    ui.memory_mut(|mem| mem.open_popup(popup_id));
                }

                let add_contents = |ui: &mut Ui| {
                    ui.set_min_width(200.0);
                    ui.horizontal(|ui| {
                        ui.label("Filter (regex):");
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            if ui.button("X").on_hover_text("Close Filter Box").clicked() {
                                ui.memory_mut(|mem| mem.close_popup());
                            }
                            if ui.button("Clear").on_hover_text("Clear Filter").clicked() {
                                if let Err(e) = self.set_column_filter(idx, String::new()) {
                                    self.loading_error =
                                        Some(format!("Error clearing filter: {}", e));
                                }
                                ui.memory_mut(|mem| mem.close_popup());
                            }
                        });
                    });

                    let filter = self.filters.get(&Index(idx)).cloned().unwrap_or_default();
                    let mut filter_text = filter.clone();

                    if ui.text_edit_singleline(&mut filter_text).changed() {
                        if let Err(e) = self.set_column_filter(idx, filter_text) {
                            self.loading_error = Some(format!("Invalid filter: {}", e));
                        }
                    }
                };
                popup_below_widget(
                    ui,
                    popup_id,
                    &response,
                    PopupCloseBehavior::IgnoreClicks,
                    add_contents,
                );
            };
            egui_header.col(column_header);
        }
    }

    fn render_table(&mut self, ui: &mut Ui) {
        {
            let data: std::sync::RwLockReadGuard<'_, TableData> = self.data.read().unwrap();
            if data.rows.is_empty() {
                ui.heading("No data rows available");
                return;
            }
        }
        self.render_table_data(ui);
        self.render_footer(ui);
    }

    fn render_table_data(&mut self, ui: &mut Ui) {
        let total_rows = {
            let filtered_row_indices = self.filtered_row_indices.read().unwrap();
            filtered_row_indices.len()
        };
        let n_cols = self.data.read().unwrap().header.cells.len();

        let start = self.current_page * ROWS_PER_PAGE;
        let end = ((self.current_page + 1) * ROWS_PER_PAGE).min(total_rows);
        let page_rows = end.saturating_sub(start);

        ScrollArea::both()
            // leave room for footer
            .max_height(ui.available_height() - FOOTER_HEIGHT)
            // add one to total rows to account for the header, otherwise even for small tables the scrollbar will be needed
            .show_rows(ui, ROW_HEIGHT, page_rows + 1, |ui, row_range| {
                TableBuilder::new(ui)
                    .striped(true)
                    .cell_layout(Layout::left_to_right(Align::Min))
                    .columns(Column::auto().resizable(true), n_cols)
                    // the header is fixed at the top, but does shift slightly when the user is scrolling until the
                    // next row is rendered. `Vew` has this problem too 🤷
                    .header(ROW_HEIGHT, |header| {
                        self.render_header(header);
                    })
                    .body(|body| {
                        // reduce the row range to account for the header adjustment, otherwise there will be an
                        // index out of bounds error in `filtered_row_indices`
                        let page_range = start + row_range.start..start + row_range.end - 1;
                        self.render_rows(body, page_range);
                    });
            });
    }

    /// Renders rows which virtualizes scrolling by only rendering rows which are visible, includes pagination
    fn render_rows(&mut self, mut body: egui_extras::TableBody, row_range: Range<usize>) {
        let data = self.data.read().unwrap();
        // adjust range because the desired row_range from the scroll area may be stale after a `update_filtered_indices` call
        // in `render_header`
        let filtered_row_indices = self.filtered_row_indices.read().unwrap();
        let total_rows = filtered_row_indices.len();
        let range_start = row_range.start.min(total_rows);
        let range_end = row_range.end.min(total_rows);
        for &row_idx in &filtered_row_indices[range_start..range_end] {
            if *row_idx < data.rows.len() {
                let row = &data.rows[*row_idx];
                body.row(ROW_HEIGHT, |mut row_ui| {
                    for cell in row.cells().iter() {
                        row_ui.col(|ui| {
                            ui.label(cell);
                        });
                    }
                });
            }
        }
    }

    fn render_footer(&mut self, ui: &mut Ui) {
        ui.separator();
        let total_rows = self.filtered_row_indices.read().unwrap().len();
        let total_pages = if total_rows == 0 {
            1
        } else {
            total_rows.div_ceil(ROWS_PER_PAGE)
        };

        ui.horizontal(|ui| {
            ui.allocate_ui_with_layout(
                egui::Vec2::new(PAGINATION_WIDTH, ROW_HEIGHT),
                egui::Layout::left_to_right(egui::Align::Center),
                |ui| {
                    if ui.button("Prev").clicked() && self.current_page > 0 {
                        self.current_page -= 1;
                        self.page_input = format!("{}", self.current_page + 1);
                    }
                    let num_digits = self.current_page.to_string().len().min(4);
                    let text_box_width = ui.fonts(|f| {
                        f.glyph_width(&FontId::proportional(12.0), '0') * num_digits as f32
                    });
                    let text_edit = ui.add_sized(
                        [text_box_width, ROW_HEIGHT],
                        egui::TextEdit::singleline(&mut self.page_input),
                    );
                    if text_edit.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                        if let Ok(page) = self.page_input.trim().parse::<usize>() {
                            self.current_page =
                                (page.saturating_sub(1)).min(total_pages.saturating_sub(1));
                            self.page_input = format!("{}", self.current_page + 1);
                        } else {
                            self.page_input = format!("{}", self.current_page + 1);
                        }
                    }
                    ui.label(format!(" / {}", total_pages));
                    if ui.button("Next").clicked() && self.current_page + 1 < total_pages {
                        self.current_page += 1;
                        self.page_input = format!("{}", self.current_page + 1);
                    }
                },
            );

            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                ui.label(format!(
                    "Filter showing {} of {} rows",
                    total_rows,
                    self.data.read().unwrap().rows.len()
                ));
            });
        });
    }
}

impl App for VewApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut Frame) {
        if let Ok(Err(e)) = self.data_status_rx.try_recv() {
            error!("Error from child thread loading data: {}. Exiting app.", e);
            return;
        }
        egui::CentralPanel::default().show(ctx, |ui| {
            if let Some(err) = &self.loading_error {
                ui.colored_label(Color32::RED, format!("Error: {}", err));
                if ui.button("Clear Error").clicked() {
                    self.loading_error = None;
                }
                ui.separator();
            }

            {
                // update filtered indices only if the data, sort, or filter has changed
                let n_data = self.data.read().unwrap().rows.len();
                let data_changed = self
                    .change_manager
                    .last_data_row_count
                    .load(atomic::Ordering::SeqCst)
                    != n_data;
                let sort_changed = self
                    .change_manager
                    .sort_changed
                    .load(atomic::Ordering::SeqCst);
                let filter_changed = self
                    .change_manager
                    .filter_changed
                    .load(atomic::Ordering::SeqCst);
                if data_changed || sort_changed || filter_changed {
                    if let Err(e) = self.update_filtered_indices() {
                        self.loading_error = Some(format!("Error initializing indices: {}", e));
                    }
                }
            }

            self.render_table(ui);
        });
    }
}

/// Indicates if certain features of `vew` has changed since the last render
struct VewChangeManager {
    updating_filter: AtomicBool,
    sort_changed: AtomicBool,
    filter_changed: AtomicBool,
    last_data_row_count: AtomicUsize,
}

impl VewChangeManager {
    fn new() -> Self {
        Self {
            updating_filter: AtomicBool::new(false),
            sort_changed: AtomicBool::new(false),
            filter_changed: AtomicBool::new(false),
            last_data_row_count: AtomicUsize::new(0),
        }
    }
}

/// utility to strip characters from the end of a string
fn strip_end_chars<'a>(s: &'a str, chars_to_strip: &[char]) -> &'a str {
    let mut end = s.len();

    while end > 0 {
        let last_char = s[..end].chars().last().unwrap();
        if chars_to_strip.contains(&last_char) {
            // find the byte position of this character since the char may be multi-byte
            let char_size = last_char.len_utf8();
            end -= char_size;
        } else {
            break;
        }
    }

    &s[..end]
}

/// Key for sorting, either numeric or text
enum SortKey {
    Numeric(OrderedFloat<f64>),
    Text(String),
    Bytesize(ByteSize),
}

impl PartialEq for SortKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for SortKey {}
impl PartialOrd for SortKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for SortKey {
    fn cmp(&self, other: &Self) -> Ordering {
        use SortKey::*;
        match (self, other) {
            (Numeric(a), Numeric(b)) => a.cmp(b),
            (Text(a), Text(b)) => a.cmp(b),
            (Bytesize(a), Bytesize(b)) => a.cmp(b),
            // arbitrarily decide that numeric values should come first, byte size second, and text last
            (Numeric(_), Text(_)) => Ordering::Greater,
            (Text(_), Numeric(_)) => Ordering::Less,
            (Bytesize(_), Numeric(_)) => Ordering::Less,
            (Numeric(_), Bytesize(_)) => Ordering::Greater,
            (Bytesize(_), Text(_)) => Ordering::Greater,
            (Text(_), Bytesize(_)) => Ordering::Less,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_table_data_colon_delimited() {
        // test ":" as a delimiter, input is analogous to etc/passwd
        let main_text = "root:x:0:0:root:/root:/bin/bash
daemon:x:1:1:daemon:/usr/sbin:/usr/sbin/nologin
bin:x:2:2:bin:/bin:/usr/sbin/nologin
sys:x:3:3:sys:/dev:/usr/sbin/nologin
sync:x:4:65532:sync:/bin:/bin/sync
nobody:x:65534:65534:nobody:/nonexistent:/usr/sbin/nologin";
        // Parser should skip any comments
        let input = format!(
            "# comment
# comment
# comment

{}",
            main_text
        );
        let delimiter = Some(":".to_string());
        let rows = input.lines().map(|s| s.to_string()).collect();
        let (parser, data) = VewApp::init_table_data(delimiter, rows, false).unwrap();
        assert_eq!(parser.read().unwrap().delimiter(), Some(":"));
        assert_eq!(data.read().unwrap().header.cells.len(), 7);
        // technically the header takes up the first row so 6 - 1 = 5
        assert_eq!(data.read().unwrap().rows.len(), 5);

        let mut main_text_lines = main_text.lines();
        assert_eq!(
            main_text_lines
                .next()
                .unwrap()
                .split(':')
                .map(|s| s.to_string())
                .collect::<Vec<String>>(),
            data.read().unwrap().header.cells
        );
        for row in data.read().unwrap().rows.iter() {
            assert_eq!(
                row.cells,
                main_text_lines
                    .next()
                    .unwrap()
                    .split(':')
                    .map(|s| s.to_string())
                    .collect::<Vec<String>>()
            );
        }
    }

    #[test]
    fn test_init_table_data_comma_delimited() {
        let main_text = "ID,Name,Age,City,Score
1,John Doe,29,New York,85
2,Jane Smith,34,Los Angeles,90
3,Bob Johnson,42,Chicago,78
4,Alice Williams,27,Houston,88
5,Michael Brown,31,Phoenix,92
";
        let delimiter = Some(",".to_string());
        let rows = main_text.lines().map(|s| s.to_string()).collect();
        let (parser, data) = VewApp::init_table_data(delimiter, rows, false).unwrap();
        assert_eq!(parser.read().unwrap().delimiter(), Some(","));
        assert_eq!(data.read().unwrap().header.cells.len(), 5);
        assert_eq!(data.read().unwrap().rows.len(), 5);

        let mut main_text_lines = main_text.lines();
        assert_eq!(
            main_text_lines
                .next()
                .unwrap()
                .split(',')
                .map(|s| s.to_string())
                .collect::<Vec<String>>(),
            data.read().unwrap().header.cells
        );
        for row in data.read().unwrap().rows.iter() {
            assert_eq!(
                row.cells,
                main_text_lines
                    .next()
                    .unwrap()
                    .split(',')
                    .map(|s| s.to_string())
                    .collect::<Vec<String>>()
            );
        }
    }

    mod fixed_width {
        use super::*;

        #[test]
        fn test_init_table_data_ps_1() {
            let main_text = "
USER       PID  %CPU %MEM    VSZ   RSS TTY      STAT START   TIME COMMAND
root         1  0.0  0.1  22532  2340 ?        Ss   Feb20   0:05 /sbin/init
root       234  0.1  0.3  45678  6789 ?        Sl   Feb20   0:10 /usr/lib/systemd/systemd-journald
alice     1024  2.3  1.2  65432 12345 pts/0    R+   10:12   0:03 /usr/bin/python3 script.py
";
            let rows = main_text.lines().map(|s| s.to_string()).collect();
            let delimiter = None;
            let (parser, data) = VewApp::init_table_data(delimiter, rows, false).unwrap();
            let n_rows = 3;
            assert_eq!(parser.read().unwrap().delimiter(), None);
            assert_eq!(data.read().unwrap().header.cells.len(), 11);
            assert_eq!(data.read().unwrap().rows.len(), n_rows);

            assert_eq!(
                vec![
                    "USER".to_owned(),
                    "PID".to_owned(),
                    "%CPU".to_owned(),
                    "%MEM".to_owned(),
                    "VSZ".to_owned(),
                    "RSS".to_owned(),
                    "TTY".to_owned(),
                    "STAT".to_owned(),
                    "START".to_owned(),
                    "TIME".to_owned(),
                    "COMMAND".to_owned(),
                ],
                data.read().unwrap().header.cells
            );
            let expected_rows = vec![
                vec![
                    "root".to_owned(),
                    "1".to_owned(),
                    "0.0".to_owned(),
                    "0.1".to_owned(),
                    "22532".to_owned(),
                    "2340".to_owned(),
                    "?".to_owned(),
                    "Ss".to_owned(),
                    "Feb20".to_owned(),
                    "0:05".to_owned(),
                    "/sbin/init".to_owned(),
                ],
                vec![
                    "root".to_owned(),
                    "234".to_owned(),
                    "0.1".to_owned(),
                    "0.3".to_owned(),
                    "45678".to_owned(),
                    "6789".to_owned(),
                    "?".to_owned(),
                    "Sl".to_owned(),
                    "Feb20".to_owned(),
                    "0:10".to_owned(),
                    "/usr/lib/systemd/systemd-journald".to_owned(),
                ],
                vec![
                    "alice".to_owned(),
                    "1024".to_owned(),
                    "2.3".to_owned(),
                    "1.2".to_owned(),
                    "65432".to_owned(),
                    "12345".to_owned(),
                    "pts/0".to_owned(),
                    "R+".to_owned(),
                    "10:12".to_owned(),
                    "0:03".to_owned(),
                    "/usr/bin/python3 script.py".to_owned(),
                ],
            ];

            let data = data.read().unwrap();
            assert_eq!(data.rows.len(), expected_rows.len());
            for (row, expected) in data.rows.iter().zip(expected_rows.iter()) {
                assert_eq!(row.cells, *expected);
            }
        }

        #[test]
        fn test_init_table_data_ps_2() {
            // Test very oddly overlapped columns
            let main_text = "
USER               PID  %CPU %MEM      VSZ    RSS   TT  STAT STARTED      TIME COMMAND
anotheruser       5678   8.9  0.3 987654321   5678   ??  R    02Feb22    234:56.78 /usr/local/bin/anotherprocess
sampleuser        9101  15.2  0.5 192837465   9101   ??    S    03Mar23      345:67.89 /opt/sample/bin/sampleprocess
";
            let rows = main_text.lines().map(|s| s.to_string()).collect();
            let delimiter = None;
            let (parser, data) = VewApp::init_table_data(delimiter, rows, false).unwrap();
            assert_eq!(parser.read().unwrap().delimiter(), None);
            assert_eq!(data.read().unwrap().header.cells.len(), 11);
            assert_eq!(data.read().unwrap().rows.len(), 2);

            let expected_header = vec![
                "USER".to_owned(),
                "PID".to_owned(),
                "%CPU".to_owned(),
                "%MEM".to_owned(),
                "VSZ".to_owned(),
                "RSS".to_owned(),
                "TT".to_owned(),
                "STAT".to_owned(),
                "STARTED".to_owned(),
                "TIME".to_owned(),
                "COMMAND".to_owned(),
            ];
            let expected_rows = vec![
                vec![
                    "anotheruser".to_owned(),
                    "5678".to_owned(),
                    "8.9".to_owned(),
                    "0.3".to_owned(),
                    "987654321".to_owned(),
                    "5678".to_owned(),
                    "??".to_owned(),
                    "R".to_owned(),
                    "02Feb22".to_owned(),
                    "234:56.78".to_owned(),
                    "/usr/local/bin/anotherprocess".to_owned(),
                ],
                vec![
                    "sampleuser".to_owned(),
                    "9101".to_owned(),
                    "15.2".to_owned(),
                    "0.5".to_owned(),
                    "192837465".to_owned(),
                    "9101".to_owned(),
                    "??".to_owned(),
                    "S".to_owned(),
                    "03Mar23".to_owned(),
                    "345:67.89".to_owned(),
                    "/opt/sample/bin/sampleprocess".to_owned(),
                ],
            ];

            assert_eq!(data.read().unwrap().header.cells, expected_header);

            let data = data.read().unwrap();
            assert_eq!(data.rows.len(), expected_rows.len());
            for (row, expected) in data.rows.iter().zip(expected_rows.iter()) {
                assert_eq!(row.cells, *expected);
            }
        }

        #[test]
        fn test_init_table_data_df_h() {
            // Sample output for "df -h" (simplified for testing)
            let main_text = "\
Filesystem      Size  Used Avail Use% Mounted_on
udev            7.8G     0  7.8G   0% /dev
tmpfs           1.6G  1.7M  1.6G   1% /run
/dev/sda1       117G   32G   80G  29% /
tmpfs           7.8G   82M  7.7G   2% /dev/shm";
            let rows: Vec<String> = main_text.lines().map(|s| s.to_string()).collect();
            let (_parser, data) = VewApp::init_table_data(None, rows, false).unwrap();

            let expected_header = vec![
                "Filesystem".to_owned(),
                "Size".to_owned(),
                "Used".to_owned(),
                "Avail".to_owned(),
                "Use%".to_owned(),
                "Mounted_on".to_owned(),
            ];
            let expected_rows = vec![
                vec![
                    "udev".to_owned(),
                    "7.8G".to_owned(),
                    "0".to_owned(),
                    "7.8G".to_owned(),
                    "0%".to_owned(),
                    "/dev".to_owned(),
                ],
                vec![
                    "tmpfs".to_owned(),
                    "1.6G".to_owned(),
                    "1.7M".to_owned(),
                    "1.6G".to_owned(),
                    "1%".to_owned(),
                    "/run".to_owned(),
                ],
                vec![
                    "/dev/sda1".to_owned(),
                    "117G".to_owned(),
                    "32G".to_owned(),
                    "80G".to_owned(),
                    "29%".to_owned(),
                    "/".to_owned(),
                ],
                vec![
                    "tmpfs".to_owned(),
                    "7.8G".to_owned(),
                    "82M".to_owned(),
                    "7.7G".to_owned(),
                    "2%".to_owned(),
                    "/dev/shm".to_owned(),
                ],
            ];

            let data = data.read().unwrap();
            assert_eq!(data.header.cells, expected_header);
            assert_eq!(data.rows.len(), expected_rows.len());
            for (row, expected) in data.rows.iter().zip(expected_rows.iter()) {
                assert_eq!(row.cells, *expected);
            }
        }

        #[test]
        fn test_init_table_data_netstat() {
            // Sample output for "netstat" (simplified)
            let main_text = "\
Proto Recv-Q Send-Q Local Address           Foreign Address         State
tcp        0      0 192.168.1.5:22          192.168.1.100:53122     ESTABLISHED
tcp6       0      0 :::80                   :::*                    LISTEN";

            let rows: Vec<String> = main_text.lines().map(|s| s.to_string()).collect();
            let (_parser, data) = VewApp::init_table_data(None, rows, false).unwrap();

            let expected_header = vec![
                "Proto".to_owned(),
                "Recv-Q".to_owned(),
                "Send-Q".to_owned(),
                "Local Address".to_owned(),
                "Foreign Address".to_owned(),
                "State".to_owned(),
            ];
            let expected_rows = vec![
                vec![
                    "tcp".to_owned(),
                    "0".to_owned(),
                    "0".to_owned(),
                    "192.168.1.5:22".to_owned(),
                    "192.168.1.100:53122".to_owned(),
                    "ESTABLISHED".to_owned(),
                ],
                vec![
                    "tcp6".to_owned(),
                    "0".to_owned(),
                    "0".to_owned(),
                    ":::80".to_owned(),
                    ":::*".to_owned(),
                    "LISTEN".to_owned(),
                ],
            ];

            let data = data.read().unwrap();
            assert_eq!(data.header.cells, expected_header);
            assert_eq!(data.rows.len(), expected_rows.len());
            for (row, expected) in data.rows.iter().zip(expected_rows.iter()) {
                assert_eq!(row.cells, *expected);
            }
        }

        #[test]
        fn test_init_table_data_ls() {
            // Simulated output from "ls -l" (simplified for testing).
            // Here we assume that our parser (when delimiter is None) uses fixed‑width logic
            // to determine column boundaries, so we provide a manually encoded expected output.
            let main_text = "\
Permissions   Owner   Name
drwxr-xr-x   user    dir1
-rw-r--r--   user    file1.txt";
            let rows: Vec<String> = main_text.lines().map(|s| s.to_string()).collect();
            let delimiter = None;
            let (_parser, data) = VewApp::init_table_data(delimiter, rows, false).unwrap();

            let expected_header = vec![
                "Permissions".to_owned(),
                "Owner".to_owned(),
                "Name".to_owned(),
            ];
            let expected_rows = vec![
                vec![
                    "drwxr-xr-x".to_owned(),
                    "user".to_owned(),
                    "dir1".to_owned(),
                ],
                vec![
                    "-rw-r--r--".to_owned(),
                    "user".to_owned(),
                    "file1.txt".to_owned(),
                ],
            ];

            let data = data.read().unwrap();
            assert_eq!(data.header.cells, expected_header);
            assert_eq!(data.rows.len(), expected_rows.len());
            for (row, expected) in data.rows.iter().zip(expected_rows.iter()) {
                assert_eq!(row.cells, *expected);
            }
        }

        #[test]
        fn test_init_table_data_lsof() {
            // Simulated output from "lsof" (simplified).
            // Note: lsof typically has many columns; here we include nine columns as an example.
            let main_text = "\
COMMAND     PID  USER  FD  TYPE  DEVICE  SIZE/OFF  NODE  NAME
vim         1234 user  4u  REG   8,1     1024      5678  /tmp/file.txt
bash        5678 user  1w  CHR   136,1   0t0       345   /dev/pts/0";
            let rows: Vec<String> = main_text.lines().map(|s| s.to_string()).collect();
            let delimiter = None;
            let (_parser, data) = VewApp::init_table_data(delimiter, rows, false).unwrap();

            let expected_header = vec![
                "COMMAND".to_owned(),
                "PID".to_owned(),
                "USER".to_owned(),
                "FD".to_owned(),
                "TYPE".to_owned(),
                "DEVICE".to_owned(),
                "SIZE/OFF".to_owned(),
                "NODE".to_owned(),
                "NAME".to_owned(),
            ];
            let expected_rows = vec![
                vec![
                    "vim".to_owned(),
                    "1234".to_owned(),
                    "user".to_owned(),
                    "4u".to_owned(),
                    "REG".to_owned(),
                    "8,1".to_owned(),
                    "1024".to_owned(),
                    "5678".to_owned(),
                    "/tmp/file.txt".to_owned(),
                ],
                vec![
                    "bash".to_owned(),
                    "5678".to_owned(),
                    "user".to_owned(),
                    "1w".to_owned(),
                    "CHR".to_owned(),
                    "136,1".to_owned(),
                    "0t0".to_owned(),
                    "345".to_owned(),
                    "/dev/pts/0".to_owned(),
                ],
            ];

            let data = data.read().unwrap();
            assert_eq!(data.header.cells, expected_header);
            assert_eq!(data.rows.len(), expected_rows.len());
            for (row, expected) in data.rows.iter().zip(expected_rows.iter()) {
                assert_eq!(row.cells, *expected);
            }
        }

        #[test]
        fn test_init_table_data_odd_parses() {
            // Tests many odd parses
            // - Spaces in the datum
            // - An entry that has limited spacing and is difficult to discern which column it belongs to, so we estimate
            // - Empty row (should be skipped)
            let main_text = "\
COLUMN1  COLUMN2        COLUMN3
space v  space v      space v                      
improv cat dog connected val

";
            let rows: Vec<String> = main_text.lines().map(|s| s.to_string()).collect();
            let delimiter = None;
            let (parser, data) = VewApp::init_table_data(delimiter, rows, false).unwrap();
            assert_eq!(parser.read().unwrap().delimiter(), None);
            assert_eq!(data.read().unwrap().header.cells.len(), 3);
            assert_eq!(data.read().unwrap().rows.len(), 2);

            let expected_header = vec![
                "COLUMN1".to_owned(),
                "COLUMN2".to_owned(),
                "COLUMN3".to_owned(),
            ];
            let expected_rows = vec![
                vec![
                    "space v".to_owned(),
                    "space v".to_owned(),
                    "space v".to_owned(),
                ],
                vec![
                    "improv".to_owned(),
                    "cat dog connected".to_owned(),
                    "val".to_owned(),
                ],
            ];

            let data = data.read().unwrap();
            assert_eq!(data.header.cells, expected_header);
            assert_eq!(data.rows.len(), expected_rows.len());
            for (row, expected) in data.rows.iter().zip(expected_rows.iter()) {
                assert_eq!(row.cells, *expected);
            }
        }
    }
}
