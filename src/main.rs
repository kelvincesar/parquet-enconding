use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::errors::Result;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{WriterProperties, WriterVersion};
use arrow::record_batch::RecordBatch;
use std::fs::File;

use clap::{Arg, Command};

fn main() {
    let matches = Command::new("Parquet Encoder")
        .version("1.0")
        .author("Kelvin Andrade")
        .about("Rewrites a Parquet file with dictionary encoding")
        .arg(
            Arg::new("input")
                .short('i')
                .long("input")
                .value_name("INPUT")
                .help("Input Parquet file")
                .required(true),
        )
        .arg(
            Arg::new("output")
                .short('o')
                .long("output")
                .value_name("OUTPUT")
                .help("Output Parquet file")
                .required(true),
        )
        .get_matches();

    let input_file = matches.get_one::<String>("input").unwrap();
    let output_file = matches.get_one::<String>("output").unwrap();

    rewrite_parquet_with_dictionary(input_file, output_file).expect("Failed to rewrite parquet file");

}



fn rewrite_parquet_with_dictionary(input_path: &str, output_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    // read file
    let file = File::open(input_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let parquet_reader = builder.with_batch_size(8192).build()?;

    let batches: Vec<RecordBatch> = parquet_reader.collect::<Result<_, _>>()?;
    println!("Read {} batches from the input Parquet file", batches.len());
    let schema = if let Some(batch) = batches.first() {
        batch.schema()
    } else {
        return Err("No data found in the input Parquet file.".into());
    };


    // rewrite with new encoding parameters
    let file = File::create(output_path)?;
    let writer_props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .set_dictionary_enabled(true)
        .build();

    let mut writer = ArrowWriter::try_new(file, schema, Some(writer_props))?;

    for batch in batches {
        writer.write(&batch)?;
    }

    writer.close()?;

    println!("Rewritten Parquet file with RLE encoding enabled to: {}", output_path);

    Ok(())
}