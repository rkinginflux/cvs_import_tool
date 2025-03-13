# cvs_import_tool
A csv import tool to import any csv file into a InfluxData Cluster.

./csv_import --file cars.csv --tag Model --url http://192.168.0.23:8086 --database ev_cars --measurement cars

```bash
 ./csv_import
CLI Arguments Parser

Usage: cars --file <FILE> --tag <TAG> --url <URL> --database <DATABASE> --measurement <MEASUREMENT>

Options:
      --file <FILE>                Path to the CSV file
      --tag <TAG>                  Column to use as an InfluxDB tag
      --url <URL>                  InfluxDB server URL (e.g., http://192.168.0.23:8086)
      --database <DATABASE>        InfluxDB database name
      --measurement <MEASUREMENT>  InfluxDB measurement name
  -h, --help                       Print help
  -V, --version                    Print version

Disclaimer
-----------
- Pros
Can import almost ANY csv

- Cons
Not as fast as using Polars; i.e. slow
Can't declare multipe Tags yet
Not tested against InfluxDB v3 yet
