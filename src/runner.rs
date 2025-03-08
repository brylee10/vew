//! Runs the view app

use std::{
    io::{self, BufRead, BufReader},
    sync::{Arc, RwLock},
    thread,
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use eframe::{NativeOptions, run_native};
use egui::ViewportBuilder;
use log::{debug, trace};
use tokio::{
    select,
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    time::{Instant, timeout},
};

use crate::parsers::Parser;
use crate::{
    Args,
    vew::{TableData, VewApp, VewError},
};

/// Timeout for preview rows
const PREVIEW_TIMEOUT: Duration = Duration::from_millis(1000);
/// Use the first up to `PREVIEW_ROWS` rows to infer column widths, particularly the header widths
const PREVIEW_ROWS: usize = 100;

/// Reads input from stdin and sends it to the main thread
fn spawn_read_stdin(
    stdin_tx: UnboundedSender<String>,
    data_status_tx: UnboundedSender<Result<()>>,
) {
    thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async move {
            let run_loop = || -> Result<()> {
                let stdin = io::stdin();
                let reader = BufReader::new(stdin);
                let lines = reader.lines();
                for line in lines {
                    let line = line.context("Failed to read line from stdin")?;
                    // skip empty lines
                    if line.is_empty() {
                        continue;
                    }
                    trace!("Sending line: {}", line);
                    stdin_tx.send(line).map_err(VewError::Send)?;
                }
                debug!("Read stdin thread sending EOF");
                Ok(())
            };
            data_status_tx.send(run_loop()).unwrap_or_else(|e| {
                // if error is sending to closed channel and the GUI was closed, this is expected
                debug!("Failed to send result to sender: {}", e);
            });
        });
    });
}

/// Streams the stdin input as `[TableRow]` and adds them to the table data
fn spawn_stream_table_data(
    mut stdin_rx: UnboundedReceiver<String>,
    parser: Arc<RwLock<Box<dyn Parser>>>,
    data: Arc<RwLock<TableData>>,
    data_status_tx: UnboundedSender<Result<()>>,
) {
    thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async move {
            let mut run_loop = async || -> Result<()> {
                while let Some(input) = stdin_rx.recv().await {
                    let table_row = {
                        let parser = parser.read().map_err(|_| {
                            anyhow!("Failed to acquire parser write lock in stream table")
                        })?;
                        match parser.parse(&input) {
                            Ok(table_row) => table_row,
                            Err(VewError::NoDelimiterFound) => {
                                // skip rows with no valid delimiter for the delimiter parser
                                debug!(
                                    "Skipping row {} with no valid delimiter for the delimiter parser",
                                    input
                                );
                                continue;
                            }
                            // other errors are unexpected
                            Err(e) => return Err(e.into()),
                        }
                    };

                    loop {
                        // do not use blocking write lock, because it will block the main thread from reading
                        // in particular if the gui loop has a read lock, then we try to take a write lock, but then
                        // the user presses a button which causes a rerender which will try to take another read lock
                        // in the same thread as the first read lock, then the first read lock will not be freed and 
                        // out of fairness the last read will not acquired the lock until the write gets the lock, so there 
                        // is deadlock
                        match data.try_write() {
                            Ok(mut data) => {
                                data.add_row(table_row);
                                break;
                            }
                            Err(_) => {
                                // try again
                            }
                        }
                    }
                }
                Ok(())
            };
            data_status_tx.send(run_loop().await).unwrap_or_else(|e| {
                // If error is sending to closed channel and the GUI was closed, this is expected
                debug!("Failed to send result to sender: {}", e);
            });
        });
    });
}

/// Pefetch the first up to `PREVIEW_ROWS` rows
///
/// Used to later infer the format or simply to prepopulate the table with data if not using inference
async fn get_preview_rows(
    stdin_rx: &mut UnboundedReceiver<String>,
    data_status_rx: &mut UnboundedReceiver<Result<()>>,
    n_rows: usize,
    preview_timeout: Duration,
) -> Result<Vec<String>> {
    let mut rows = Vec::new();
    let deadline = Instant::now() + preview_timeout;

    while rows.len() < n_rows {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining == Duration::ZERO {
            break;
        }

        select! {
            ret = timeout(remaining, stdin_rx.recv()) => {
                match ret {
                    Ok(Some(row)) => {
                        debug!("row: {}", row);
                        rows.push(row);
                    }
                    Ok(None) => {
                        debug!("channel closed");
                        break; // channel closed
                    }
                    Err(_) => {
                        debug!("timeout expired");
                        if !rows.is_empty() {
                            break; // timeout expired
                        } else {
                            // even if timeout expires, if no data has been read yet we should continue waiting
                            // maybe upstream process takes long time to start up or is (undesirably) block buffering
                            debug!("timeout expired, but no rows read, continue waiting");
                            continue;
                        }
                    }
                }
            }
            ret = data_status_rx.recv() => {
                match ret {
                    Some(Ok(())) => {
                        debug!("task finished");
                        // other channel finished reading rows from stdin and will send them to main thread,
                        // the main thread will now continue read those rows from the channel
                    }
                    Some(Err(e)) => {
                        debug!("error: {:?}", e);
                        return Err(e); // error from child task
                    },
                    None => {
                        debug!("channel closed");
                        break;
                    }
                }
            }
        }
    }
    Ok(rows)
}

/// Runs the vew app
pub async fn run_vew(args: Args) -> Result<()> {
    let use_placeholder_header = args.placeholder_header;

    let (stdin_tx, mut stdin_rx) = unbounded_channel();
    let (data_status_tx, mut data_status_rx) = unbounded_channel();
    spawn_read_stdin(stdin_tx.clone(), data_status_tx.clone());

    let rows: Vec<String> = get_preview_rows(
        &mut stdin_rx,
        &mut data_status_rx,
        PREVIEW_ROWS,
        PREVIEW_TIMEOUT,
    )
    .await?;

    let app = VewApp::new(
        args.delimiter,
        rows,
        args.infer_header,
        data_status_rx,
        use_placeholder_header,
    )?;

    spawn_stream_table_data(
        stdin_rx,
        Arc::clone(&app.parser),
        Arc::clone(&app.data),
        data_status_tx,
    );
    let viewport = ViewportBuilder::default().with_app_id("vew");
    let native_options = NativeOptions {
        viewport,
        ..Default::default()
    };
    run_native("vew", native_options, Box::new(|_cc| Ok(Box::new(app))))
        .map_err(|_| anyhow!("Failed to run native app"))?;

    Ok(())
}
