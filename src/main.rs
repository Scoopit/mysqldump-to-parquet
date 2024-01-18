#![allow(unused)]

use std::{
    borrow::Cow,
    fs::{create_dir_all, File},
    io::{self, BufRead, BufReader},
    path::PathBuf,
};

use clap::Parser;
use color_eyre::eyre::{Context, Result};
use flate2::read::GzDecoder;

use crate::parquet_writer::ParquetWriter;

mod line_parser;
mod parquet_writer;

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
    let mut line_number = 0;
    let mut current_statement = String::with_capacity(8192);
    let mut line = String::with_capacity(8192);

    let (writer_sender, write_thread_join_handle) = ParquetWriter::start();
    let (line_parser_sender, line_parser_receiver) = crossbeam::channel::bounded::<String>(1000);

    let line_parser_handle = std::thread::spawn(move || {
        while let Ok(line) = line_parser_receiver.recv() {
            writer_sender
                .send(line_parser::parse_line(&line).unwrap())
                .unwrap();
        }
    });

    loop {
        line_number += 1;
        line.clear();
        if reader
            .read_line(&mut line)
            .context("Unable to read input")?
            == 0
        {
            break;
        };
        let line = line.trim();
        if line.starts_with("--")
            || line.starts_with("/*") && line.ends_with("*/;")
            || line.len() == 0
        {
            // ignore comments
            continue;
        } else {
            if current_statement.starts_with("CREATE TABLE") {
                current_statement.extend(cleanup_key(line).chars())
            } else {
                current_statement.extend(line.chars())
            }
        }

        if current_statement.ends_with(";") {
            line_parser_sender.send(current_statement.trim().to_string())?;
            current_statement.clear();
        }
    }
    println!("{line_number} lines read.");
    // nothing to send anymore, drop the sender so the parser thread will end.
    drop(line_parser_sender);
    line_parser_handle.join().unwrap();
    write_thread_join_handle.join().unwrap();

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
