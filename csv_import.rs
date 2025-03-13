use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use clap::Parser;
use reqwest::blocking::Client;

#[derive(Parser)]
#[command(author, version, about, long_about = None, arg_required_else_help = true)]
struct Args {
    
    #[arg(long)]
    file: String,

    #[arg(long)]
    tag: String,

    #[arg(long)]
    url: String,

    #[arg(long)]
    database: String,

    #[arg(long)]
    measurement: String,
}

fn escape_identifier(input: &str) -> String {
    input.replace(" ", "\\ ")
         .replace(",", "\\,")
         .replace("=", "\\=")
}

fn escape_field_key(input: &str) -> String {
    escape_identifier(input)
}

fn process_csv(
    file_path: &str,
    tag_column: &str,
    influx_url: &str,
    database: &str,
    measurement: &str,
) -> Result<(), Box<dyn Error>> {
    let file = File::open(file_path)?;
    let mut reader = csv::Reader::from_reader(BufReader::new(file));
    
    let headers = reader.headers()?.clone();
    let tag_index = headers.iter().position(|h| h == tag_column)
        .ok_or("Specified tag column not found in CSV header")?;

    let client = Client::new();
    let influx_write_url = format!("{}/write?db={}", influx_url, database);

    let measurement_escaped = escape_identifier(measurement);

    for result in reader.records() {
        let record = result?;
        
        let tag_value = &record[tag_index];
        // Escape tag value (spaces, etc.) as needed.
        let tag_value_escaped = escape_identifier(tag_value);

        let mut fields = vec![];

        for (i, field) in record.iter().enumerate() {
            if i != tag_index {
                let field_escaped = field.replace('"', "\\\"");
                // Escape the field key from the header.
                let header = headers.get(i).ok_or("Header index out of range")?;
                let header_escaped = escape_field_key(header);
                fields.push(format!("{}=\"{}\"", header_escaped, field_escaped));
            }
        }

        if fields.is_empty() {
            continue;
        }

        let influx_line = format!("{},{}={} {}", measurement_escaped, tag_column, tag_value_escaped, fields.join(","));
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

fn main() {
    let args = Args::parse();

    if let Err(err) = process_csv(&args.file, &args.tag, &args.url, &args.database, &args.measurement) {
        eprintln!("Error: {}", err);
    }
}
