use std::{
    fs::File,
    path::PathBuf,
    thread::{self, JoinHandle},
};

use arrow::{
    array::{
        make_builder, ArrayBuilder, ArrayRef, BooleanBuilder, Float64Builder, Int64Builder,
        StringBuilder, TimestampSecondBuilder,
    },
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Utc};
use indicatif::ProgressBar;
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};

use crate::line_parser::{ColumnDef, ColumnValue, Line, Schema};

pub struct ParquetWriter {
    output_dir: PathBuf,
    current_writer: Option<CurrentParquetWriter>,
    progress_bar: ProgressBar,
}

pub struct CurrentParquetWriter {
    row_count: usize,
    table_name: String,
    schema: Schema,
    arrow_schema: SchemaRef,
    arrow_writer: ArrowWriter<File>,
}

impl Drop for ParquetWriter {
    fn drop(&mut self) {
        let current_writer = self.current_writer.take();
        if let Some(current_writer) = current_writer {
            current_writer.finish();
        }
        self.progress_bar
            .set_message("Done writing parquet file(s).");
        self.progress_bar.finish();
    }
}

impl ParquetWriter {
    pub fn start(
        output_dir: PathBuf,
        progress_bar: ProgressBar,
    ) -> (crossbeam::channel::Sender<Line>, JoinHandle<()>) {
        let (sender, receiver) = crossbeam::channel::bounded(100);

        let writer_thread_join_handle = thread::spawn(move || {
            let mut w = ParquetWriter {
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
                    table_name,
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
                    let current_writer = self.current_writer.as_mut().unwrap();
                    let row_count = rows.len();

                    current_writer.write_rows(rows);
                    self.progress_bar.inc(row_count as u64);
                    current_writer.row_count += row_count;
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
            for (i, column_value) in row.into_iter().enumerate() {
                let ColumnDef {
                    column_name,
                    nullable: _,
                    column_type,
                } = &self.schema.0[i];
                let array_builder = &mut array_builders[i];

                match column_type {
                    crate::line_parser::ColumnType::String => {
                        let builder = array_builder
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
                        let builder = array_builder
                            .as_any_mut()
                            .downcast_mut::<Int64Builder>()
                            .unwrap();
                        match column_value{
                            ColumnValue::Integer(value) => builder.append_value(value),
                            ColumnValue::Null => builder.append_null(),
                            _ => panic!("Value for column {column_name} should be an integer but is {column_value:?}"),
                        };
                    }
                    crate::line_parser::ColumnType::Float => {
                        let builder = array_builder
                            .as_any_mut()
                            .downcast_mut::<Float64Builder>()
                            .unwrap();
                        match column_value{
                            ColumnValue::Float(value) => builder.append_value(value),
                            ColumnValue::Integer(value) => builder.append_value(value as f64),
                            ColumnValue::Null => builder.append_null(),
                            _ => panic!("Value for column {column_name} should be a float but is {column_value:?}"),
                        };
                    }
                    crate::line_parser::ColumnType::Timestamp => {
                        let builder = array_builder
                            .as_any_mut()
                            .downcast_mut::<TimestampSecondBuilder>()
                            .unwrap();
                        match column_value{
                            ColumnValue::String(value) =>{
                                // Brute force parse date YYYY-mm-DD hh:mm:ss
                                //                        0123456789
                                let year = value[0..4].parse().unwrap();
                                let month = value[5..7].parse().unwrap();
                                let day = value[8..10].parse().unwrap();

                                let hour = value[11..13].parse().unwrap();
                                let min = value[14..16].parse().unwrap();
                                let sec = value[17..19].parse().unwrap();

                                let datetime = NaiveDateTime::new(NaiveDate::from_ymd_opt(year, month, day).expect("Unable to create date"), NaiveTime::from_hms_opt(hour, min, sec).expect("Unable to create time"));

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
                        let builder = array_builder
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
        self.arrow_writer.close().unwrap();
    }
}
