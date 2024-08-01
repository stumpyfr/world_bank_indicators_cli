# World Bank Indicators CLI

This is a command line interface (CLI) that allows users to interact with the World Bank Indicators API. The CLI allows users to search for indicators per source and to retrieve data for specific indicators.

## Installation

To install the CLI, run the following command:

```bash
go build -o wbi main.go
```

## Usage

To use the CLI, run the following command:

```bash
wbi sources
wbi indicators --source <source>
wbi dl -t 2023:2018 -i AG.LND.FRST.K2 -d output.db --csv output.csv --parquet output.parquet --json output.json
```
