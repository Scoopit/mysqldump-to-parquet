# mysqldump-to-parquet

Convert mysqldump or portion of it to parquet files, one file per table

## Installation

This tool is not yet published to crates.io.

````bash
cargo install --git https://github.com/Scoopit/mysqldump-to-parquet.git
````

## Features / Limitations

- read schema from `CREATE TABLE` statement
- handle null/not null column

## Usage

Input stream (file or stdin) must contain `CREATE TABLE` statement before
any insertion of data into the table.

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
