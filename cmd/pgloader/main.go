package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/apex/log"
	"github.com/apex/log/handlers/text"
	"github.com/nHurD/pgloader/pkg/loader"
)

var (
	Log = log.Log
)

func init() {
	log.SetHandler(text.New(os.Stderr))
}

func main() {

	var (
		dataCh  chan []string
		workers []*loader.Loader
		header  []string
	)
	wg := sync.WaitGroup{}
	numWorkers := flag.Int("workers", 4, "Number of Workers")
	batchSize := flag.Int("batch", 10000, "Batch size for committing")
	upsert := flag.Bool("upsert", false, "Upsert mode")
	connStr := flag.String(
		"connection",
		os.Getenv("CONN_STRING"),
		"The connection string to use in the format of "+
			"'postgresql://user:password@host:port/database'."+
			"\nThis defaults to the value in $CONN_STRING",
	)

	// Customize the usage message
	flag.Usage = func() {
		fmt.Fprintf(
			os.Stderr,
			"Usage %s (flags) [table] [csv_file]:\n",
			os.Args[0],
		)
		fmt.Fprint(os.Stderr, "\nRequired Arguments:\n")
		fmt.Fprintln(os.Stderr, "  table    - the destination table")
		fmt.Fprintln(os.Stderr, "  csv_file - the input file to read")
		fmt.Fprintln(os.Stderr, "\nOptional Flags:")
		flag.PrintDefaults()
	}

	flag.Parse()
	args := flag.Args()

	if len(args) < 2 {
		flag.Usage()
		os.Exit(1)
	}
	dataCh = make(chan []string)

	for i := 0; i < *numWorkers; i++ {
		wg.Add(1)
		db, err := sql.Open("postgres", *connStr)
		if err != nil {
			Log.WithError(err).
				Fatal("Unable to connect to the database server")
		}

		ldr := loader.NewLoader(
			db, args[0], i, *batchSize, *upsert, &wg, Log,
		)
		go ldr.DoWork(dataCh)
		workers = append(workers, ldr)
	}

	csvFile, err := os.Open(args[1])
	if err != nil {
		Log.WithError(err).
			Fatal("Unable to open file")
	}
	defer csvFile.Close()

	csvReader := csv.NewReader(csvFile)
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			Log.WithError(err).
				Fatal("Error Reading Row")
		}
		if header == nil {
			header = record
			for i := 0; i < *numWorkers; i++ {
				workers[i].SetHeader(header)
			}
			continue
		}
		dataCh <- record
	}

	for _, worker := range workers {
		worker.Done()
	}
	wg.Wait()
}
