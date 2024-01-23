# mysqldump-to-parquet

Convert a mysql dump - or portion of it - into parquet files, one file per table.

## Installation

````bash
cargo install --git https://github.com/Scoopit/mysqldump-to-parquet.git
````

## Features / Limitations

Schema is created from `CREATE TABLE` statement. It handles nullable/not nullable values depending on `NOT NULL` or `PRIMARY KEY` column options.

For a given table, `CREATE TABLE` statement must appear before `INSERT INTO` statements.

Data from multiple tables cannot be interleaved.

## License

Licensed under either of

- Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license
   ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
