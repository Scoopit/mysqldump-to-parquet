use std::{
    fs::File,
    thread::{self, JoinHandle},
    time::Instant, path::PathBuf,
};

use arrow::{
    array::{
        make_builder, ArrayBuilder, ArrayRef, BooleanBuilder, Float64Array, Float64Builder,
        Int64Builder, StringBuilder, TimestampSecondBuilder,
    },
    datatypes::{SchemaBuilder, SchemaRef},
    ipc::{TimestampBuilder, Utf8Builder},
    record_batch::RecordBatch,
};
use chrono::{DateTime, Local, NaiveDateTime, TimeZone, Utc};
use crossbeam::channel::Receiver;
use indicatif::ProgressBar;
use parquet::{
    arrow::ArrowWriter,
    basic::Compression,
    column,
    file::properties::{WriterProperties, WriterPropertiesBuilder},
};

use crate::line_parser::{ColumnValue, Line, Schema};

pub struct ParquetWriter {
    output_dir: PathBuf,
    current_writer: Option<CurrentParquetWriter>,
    progress_bar: ProgressBar,
}

pub struct CurrentParquetWriter {
    row_count: usize,
    table_name: String,
    schema: Schema,
    started: Instant,
    arrow_schema: SchemaRef,
    arrow_writer: ArrowWriter<File>,
}

impl Drop for ParquetWriter {
    fn drop(&mut self) {
        let current_writer = self.current_writer.take();
        if let Some(current_writer) = current_writer {
            current_writer.finish();
        }
        self.progress_bar.set_message("Done writing parquet file(s).");
        self.progress_bar.finish();
    }
}

impl ParquetWriter {
    pub fn start(output_dir: PathBuf,progress_bar: ProgressBar) -> (crossbeam::channel::Sender<Line>, JoinHandle<()>) {
        let (sender, receiver) = crossbeam::channel::bounded(100);

        let writer_thread_join_handle = thread::spawn(move || {
            let mut w = ParquetWriter{
                output_dir,
                progress_bar,
                current_writer: None,
            };
            while let Ok(line) = receiver.recv() {
                w.new_line(line);
            }
        });
        (sender, writer_thread_join_handle)
    }

    fn new_line(&mut self, line: Line) {
        match line {
            Line::CreateTable(table_name, schema) => {
                self.progress_bar.set_message(format!("`{table_name}`"));
                // build Arrow schema
                let arrow_schema = SchemaRef::from(schema.to_arrow_schema());
                // build ArrowWriter
                let props = WriterProperties::builder()
                    .set_compression(Compression::SNAPPY)
                    .build();
                let file_name = format!("{table_name}.parquet");
                let file_path = self.output_dir.join(file_name);
                let file = File::create(file_path).unwrap();
                let arrow_writer =
                    ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();
                let previous_writer = self.current_writer.replace(CurrentParquetWriter {
                    row_count: 0,
                    table_name: table_name,
                    started: Instant::now(),
                    arrow_schema,
                    arrow_writer,
                    schema,
                });
                if let Some(preview_writer) = previous_writer {
                    preview_writer.finish();
                }
            }
            Line::InsertInto(table_name, rows) => {
                if Some(&table_name) != self.current_writer.as_ref().map(|w| &w.table_name) {
                    eprintln!("Received a line from an unknown table: CREATE TABLE statement must precede any INSERT INTO.");
                } else {
                    // INSERT DATA, by construction there is a current writer ;)
                    let mut current_writer = self.current_writer.as_mut().unwrap();
                    let row_count = rows.len();
                    
                    current_writer.write_rows(rows);
                    self.progress_bar.inc(row_count as u64);
                    current_writer.row_count +=row_count;

                }
            }
            Line::NOP => {}
        }
    }
}

impl CurrentParquetWriter {
    fn array_builders(&self, capacity: usize) -> Vec<Box<dyn ArrayBuilder>> {
        self.arrow_schema
            .fields()
            .into_iter()
            .map(|field| make_builder(field.data_type(), capacity))
            .collect()
    }

    fn write_rows(&mut self, rows: Vec<Vec<ColumnValue>>) {
        let mut array_builders = self.array_builders(rows.len());
        for row in rows {
            // zip hell
            for ((column_name, column_type), (array_builder, column_value)) in self
                .schema
                .0
                .iter()
                .zip(array_builders.iter_mut().zip(row.into_iter()))
            {
                match column_type {
                    crate::line_parser::ColumnType::String => {
                        let mut builder = array_builder
                            .as_any_mut()
                            .downcast_mut::<StringBuilder>()
                            .unwrap();
                        match column_value{
                            ColumnValue::String(value) => builder.append_value(value),
                            ColumnValue::Null => builder.append_null(),
                            _ => panic!("Value for column {column_name} should be a string but is {column_value:?}"),
                        };
                    }
                    crate::line_parser::ColumnType::Integer => {
                        let mut builder = array_builder
                            .as_any_mut()
                            .downcast_mut::<Int64Builder>()
                            .unwrap();
                        match column_value{
                            ColumnValue::Integer(value) => builder.append_value(value),
                            ColumnValue::Null => builder.append_null(),
                            _ => panic!("Value for column {column_name} should be a string but is {column_value:?}"),
                        };
                    }
                    crate::line_parser::ColumnType::Float => {
                        let mut builder = array_builder
                            .as_any_mut()
                            .downcast_mut::<Float64Builder>()
                            .unwrap();
                        match column_value{
                            ColumnValue::Float(value) => builder.append_value(value),
                            ColumnValue::Null => builder.append_null(),
                            _ => panic!("Value for column {column_name} should be a string but is {column_value:?}"),
                        };
                    }
                    crate::line_parser::ColumnType::Timestamp => {
                        let mut builder = array_builder
                            .as_any_mut()
                            .downcast_mut::<TimestampSecondBuilder>()
                            .unwrap();
                        match column_value{
                            ColumnValue::String(value) =>{
                                let datetime = NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S")
                            .expect("Unable to parse date");

                        let local_tz_datetime = match datetime.and_local_timezone(Utc){
                            chrono::LocalResult::None => panic!("{datetime} cannot be converted in local timezone"),
                            chrono::LocalResult::Single(dt) => dt,
                            // ignore ambigous (not sure how this is handled by mysql)
                            chrono::LocalResult::Ambiguous(dt, _) => dt,
                        };
                        
                        builder.append_value(local_tz_datetime.timestamp());
                            },
                            ColumnValue::Null => builder.append_null(),
                            _ => panic!("Value for column {column_name} should be a string but is {column_value:?}"),
                        };
                    }
                    crate::line_parser::ColumnType::Boolean => {
                        let mut builder = array_builder
                            .as_any_mut()
                            .downcast_mut::<BooleanBuilder>()
                            .unwrap();
                        match column_value{
                            ColumnValue::Boolean(value) => builder.append_value(value),
                            ColumnValue::Null => builder.append_null(),
                            _ => panic!("Value for column {column_name} should be a string but is {column_value:?}"),
                        };
                    }
                }
            }
        }
        let array_refs: Vec<ArrayRef> = array_builders
            .iter_mut()
            .map(|builder| builder.finish())
            .collect();
        let record_batch = RecordBatch::try_new(self.arrow_schema.clone(), array_refs).unwrap();
        self.arrow_writer.write(&record_batch).unwrap();
    }

    fn finish(self) {
        self.arrow_writer.close();
    }
}
