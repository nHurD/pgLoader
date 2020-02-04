# pgLoader

This is a simple utility that I wrote to help me load very large CSV
files (sic 700+ GB) into lower PostgreSQL environments for a multitude
of reasons from testing new partition schemes to upgrading Postgres
compatible Aurora instances to newer versions.

## Requirements

* Go v1.13 and above
* Access to a PostgreSQL instance

## Building

``` shell
$ go build ./cmd/pgloader
```

## Usage

Command help:

```
Usage ./pgloader (flags) [table] [csv_file]:

Required Arguments:
  table    - the destination table
  csv_file - the input file to read

Optional Flags:
  -batch int
    	Batch size for committing (default 10000)
  -connection string
    	The connection string to use in the format of 'postgresql://user:password@host:port/database'.
    	This defaults to the value in $CONN_STRING
  -upsert
    	Upsert mode
  -workers int
    	Number of Workers (default 4)
```
