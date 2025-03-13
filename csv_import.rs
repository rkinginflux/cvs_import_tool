use clap::Parser;
use polars::prelude::*;
use reqwest::blocking::Client;
use std::error::Error;

/// CLI Arguments Parser
#[derive(Parser)]
#[command(author, version, about, long_about = None, arg_required_else_help = true)]
struct Args {
    /// Path to the CSV file
    #[arg(long)]
    file: String,

    /// Column to use as an InfluxDB tag
    #[arg(long)]
    tag: String,

    /// InfluxDB server URL (e.g., http://192.168.0.23:8086)
    #[arg(long)]
    url: String,

    /// InfluxDB database name
    #[arg(long)]
    database: String,

    /// InfluxDB measurement name
    #[arg(long)]
    measurement: String,
}

/// Escape spaces, commas, and equal signs for identifiers
fn escape_identifier(input: &str) -> String {
    input
        .replace(" ", "\\ ")
        .replace(",", "\\,")
        .replace("=", "\\=")
}

/// Processes the CSV using Polars and writes each row to InfluxDB
fn process_csv_polars(args: &Args) -> Result<(), Box<dyn Error>> {
    // Read the CSV file quickly with Polars.
    let mut df = CsvReader::from_path(&args.file)?
        .infer_schema(None)
        .has_header(true)
        .finish()?;

    // Force every column to be Utf8 (string) even if inferred otherwise.
    let new_cols = df
        .get_columns()
        .iter()
        .map(|s| {
            if s.dtype() != &DataType::Utf8 {
                s.cast(&DataType::Utf8)
            } else {
                Ok(s.clone())
            }
        })
        .collect::<Result<Vec<_>, PolarsError>>()?;
    df = DataFrame::new(new_cols)?;

    // Get column names as a Vec<&str>
    let headers = df.get_column_names();

    // Find the index for the tag column
    let tag_index = headers
        .iter()
        .position(|h| *h == args.tag)
        .ok_or("Specified tag column not found in CSV header")?;

    let client = Client::new();
    let influx_write_url = format!("{}/write?db={}", args.url, args.database);
    let measurement_escaped = escape_identifier(&args.measurement);

    // Iterate over rows by index.
    for row_idx in 0..df.height() {
        // Get the tag value
        let tag_value = df.column(&args.tag)?
            .utf8()?
            .get(row_idx)
            .unwrap_or("");
        let tag_value_escaped = escape_identifier(tag_value);

        let mut fields = Vec::new();
        for (col_idx, header) in headers.iter().enumerate() {
            if col_idx == tag_index {
                continue;
            }
            let series = df.column(header)?;
            let field_value = series.utf8()?.get(row_idx).unwrap_or("");
            let field_escaped = field_value.replace('"', "\\\"");
            let header_escaped = escape_identifier(header);
            fields.push(format!("{}=\"{}\"", header_escaped, field_escaped));
        }

        if fields.is_empty() {
            continue;
        }

        // Build the InfluxDB line protocol string:
        // <measurement>,<tag_key>=<tag_value> <field_key>="field_value",...
        let influx_line = format!(
            "{},{}={} {}",
            measurement_escaped,
            args.tag,
            tag_value_escaped,
            fields.join(",")
        );
        println!("Writing: {}", influx_line);

        let response = client.post(&influx_write_url)
            .body(influx_line)
            .send()?;

        if !response.status().is_success() {
            eprintln!("Failed to write to InfluxDB: {:?}", response.text()?);
        }
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    process_csv_polars(&args)?;
    Ok(())
}
