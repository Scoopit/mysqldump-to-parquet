use std::{
    borrow::Cow,
    fs::{create_dir_all, File},
    io::{self, BufRead, BufReader},
    path::PathBuf,
};

use clap::Parser;
use color_eyre::eyre::{Context, Result};
use flate2::read::GzDecoder;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use crate::parquet_writer::ParquetWriter;

mod line_parser;
mod parquet_writer;

#[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

/// Parse MYSQL dump and write tables to parquet files
#[derive(Parser)]
struct Opts {
    /// Output directory
    #[clap(short, long, default_value("."))]
    output: String,
    /// Input statement from this file instead of stdin (.sql or .sql.gz)
    input: Option<String>,
}

fn main() -> Result<()> {
    color_eyre::install()?;
    let args = Opts::parse();
    let mut reader: Box<dyn BufRead> = {
        match &args.input {
            Some(file) => {
                if file.ends_with(".gz") {
                    Box::new(BufReader::with_capacity(
                        8192 * 1000,
                        GzDecoder::new(
                            File::open(file).with_context(|| format!("Cannot open {file}"))?,
                        ),
                    ))
                } else {
                    Box::new(BufReader::with_capacity(
                        8192 * 1000,
                        File::open(file).with_context(|| format!("Cannot open {file}"))?,
                    ))
                }
            }

            None => Box::new(io::stdin().lock()),
        }
    };
    let output_dir = PathBuf::from(&args.output);
    create_dir_all(&output_dir)
        .with_context(|| format!("Cannot create output directory {}", args.output))?;

    // progress bar handling

    let progress = MultiProgress::new();
    let read_progress_bar = ProgressBar::new_spinner().with_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] {human_pos:>12} lines  {spinner}  Reading file... {msg}",
        )
        .unwrap(),
    );
    let parse_progress_bar = ProgressBar::new_spinner().with_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] {human_pos:>12} rows   {spinner}  Parsing {msg}",
        )
        .unwrap(),
    );
    let write_progress_bar = ProgressBar::new_spinner().with_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] {human_pos:>12} rows   {spinner}  Writing {msg}",
        )
        .unwrap(),
    );
    progress.add(read_progress_bar.clone());
    progress.add(parse_progress_bar.clone());
    progress.add(write_progress_bar.clone());

    let (writer_sender, write_thread_join_handle) =
        ParquetWriter::start(output_dir, write_progress_bar);
    let (line_parser_sender, line_parser_receiver) = crossbeam::channel::bounded::<String>(1000);

    let line_parser_handle = std::thread::spawn(move || {
        while let Ok(line) = line_parser_receiver.recv() {
            let line = line_parser::parse_line(&line).unwrap();
            match &line {
                line_parser::Line::InsertInto(_, rows) => parse_progress_bar.inc(rows.len() as u64),
                line_parser::Line::CreateTable(table_name, _) => {
                    parse_progress_bar.set_message(format!("`{table_name}`"))
                }
                _ => parse_progress_bar.tick(),
            }
            writer_sender
                .send(line)
                .expect("Cannot send parsed rows to parquet writer");
        }
        parse_progress_bar.set_message("Done parsing sql");
        parse_progress_bar.finish();
    });

    let mut current_statement = String::with_capacity(8192);
    let mut line = String::with_capacity(8192);
    loop {
        line.clear();
        if reader
            .read_line(&mut line)
            .context("Unable to read input")?
            == 0
        {
            break;
        };
        read_progress_bar.inc(1);
        let line = line.trim();
        if line.starts_with("--")
            || line.starts_with("/*") && line.ends_with("*/;")
            || line.is_empty()
        {
            // ignore comments
            continue;
        } else if current_statement.starts_with("CREATE TABLE") {
            current_statement.extend(cleanup_key(line).chars())
        } else {
            current_statement.push_str(line)
        }

        if current_statement.ends_with(';') {
            line_parser_sender
                .send(current_statement.trim().to_string())
                .context("Cannot send SQL statement to parser!")?;
            current_statement.clear();
        }
    }
    // nothing to send anymore, drop the sender so the parser thread will end.
    drop(line_parser_sender);
    read_progress_bar.set_message("done!");
    read_progress_bar.finish();
    line_parser_handle.join().expect("Parser thread crashed!");
    write_thread_join_handle
        .join()
        .expect("Parquet writer thread crashed!");

    Ok(())
}

fn cleanup_key(line: &str) -> Cow<str> {
    if line.contains("KEY ") {
        let mut ret = String::new();
        let mut depth = 0;
        for c in line.chars() {
            if c == '(' {
                depth += 1;
            }
            if c == ')' {
                depth -= 1;
                if depth == 1 {
                    continue;
                }
            }
            if depth >= 2 {
                continue;
            }
            ret.push(c);
        }
        ret.into()
    } else {
        line.into()
    }
}

#[cfg(test)]
mod test {
    use crate::cleanup_key;

    #[test]
    fn cleanup() {
        assert_eq!(
            cleanup_key("KEY `facebookConnectId_index` (`facebookConnectId`)"),
            "KEY `facebookConnectId_index` (`facebookConnectId`)"
        );
        assert_eq!(
            cleanup_key("KEY `facebookConnectId_index` (`facebookConnectId`(144))"),
            "KEY `facebookConnectId_index` (`facebookConnectId`)"
        );
        assert_eq!(
            cleanup_key("KEY `facebookConnectId_index` (`facebookConnectId`(144),`plop`)"),
            "KEY `facebookConnectId_index` (`facebookConnectId`,`plop`)"
        );
        assert_eq!(
            cleanup_key("KEY `facebookConnectId_index` (`facebookConnectId`(144),`plop`(12))"),
            "KEY `facebookConnectId_index` (`facebookConnectId`,`plop`)"
        );
        assert_eq!(
            cleanup_key("KEY `facebookConnectId_index` (`facebookConnectId`,`plop`(12))"),
            "KEY `facebookConnectId_index` (`facebookConnectId`,`plop`)"
        );
        assert_eq!(
            cleanup_key("FOREIGN KEY (`facebookConnectId`)"),
            "FOREIGN KEY (`facebookConnectId`)"
        );
        assert_eq!(
            cleanup_key("FOREIGN KEY (`facebookConnectId`(144))"),
            "FOREIGN KEY (`facebookConnectId`)"
        );
    }
}
