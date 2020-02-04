package loader

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/apex/log"
	"github.com/lib/pq"
)

func NewLoader(
	dbContext *sql.DB, tableName string, id, batchSize int,
	upsert bool, wg *sync.WaitGroup, log log.Interface) *Loader {

	loader := &Loader{
		id:        id,
		batchSize: batchSize,
		table:     tableName,
		ctx:       dbContext,
		wg:        wg,
		doneCh:    make(chan bool),
		upsert:    upsert,
		log:       log,
	}
	return loader
}

// Loader encompases all items required to load data into a
// given postgres database
type Loader struct {
	id          int
	ctx         *sql.DB
	wg          *sync.WaitGroup
	doneCh      chan bool
	table       string
	headers     []string
	colTypes    []string
	primaryKeys []string
	batchSize   int
	upsert      bool
	log         log.Interface
}

// Done will notify the worker to clean up and shutdown
func (l *Loader) Done() {
	l.doneCh <- true
}

// SetHeader will set the headers
func (l *Loader) SetHeader(headers []string) {
	l.headers = headers
}

func (l *Loader) getColumnTypes() {
	l.colTypes = make([]string, len(l.headers))
	queryStr := "SELECT data_type from information_schema.columns where " +
		"table_name = $1 and column_name = $2"
	for idx, col := range l.headers {
		var colType string
		err := l.ctx.QueryRow(queryStr, l.table, col).Scan(&colType)
		if err != nil {
			l.log.WithError(err).
				Fatalf("Unable to get column type: %s", col)
		}
		l.colTypes[idx] = colType
	}

}

// getPrimaryKeys will fetch primary key data for the struct's table.
// This will be leveraged for the `upsert` mode.
func (l *Loader) getPrimaryKeys() {
	var key string

	l.log.Infof("Worker %d gathering primary keys for table", l.id)
	queryStr := `
		SELECT c.column_name
		FROM information_schema.key_column_usage AS c
		LEFT JOIN information_schema.table_constraints AS t
		ON t.constraint_name = c.constraint_name
		WHERE t.table_name = $1
		AND t.constraint_type = 'PRIMARY KEY';`
	rows, err := l.ctx.Query(queryStr, l.table)
	if err != nil {
		l.log.WithError(err).
			Fatal("Unable to query primary keys")
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&key)
		if err != nil {
			l.log.WithError(err).
				Fatal("Unable to read data")
		}
		l.primaryKeys = append(l.primaryKeys, key)
	}
}

// makeUpsertQuery will generate the upsert query for loading
// data into the database
func (l *Loader) makeUpsertQuery() string {
	var updatedCols []string

	l.log.Infof("Worker %d generating upsert query", l.id)
	queryStr := `
		INSERT INTO %s (%s)
		SELECT %[2]s FROM %s
		ON CONFLICT (%s) DO
		UPDATE SET %s;`

	for _, col := range l.headers {
		updatedCols = append(
			updatedCols,
			fmt.Sprintf("%s = excluded.%[1]s", col),
		)
	}

	return fmt.Sprintf(
		queryStr,
		l.table,
		strings.Join(l.headers, ","),
		fmt.Sprintf("tmp_%s", l.table),
		strings.Join(l.primaryKeys, ","),
		strings.Join(updatedCols, ","),
	)
}

// makeTempTable will create the temporary table for handling upserting
// data.
func (l *Loader) makeTempTable() error {
	l.log.Infof("Worker %d creating temporary table", l.id)

	statement := fmt.Sprintf(
		"CREATE TEMPORARY TABLE tmp_%s AS "+
			"SELECT * FROM %[1]s LIMIT 1 WITH NO DATA",
		l.table,
	)
	_, err := l.ctx.Exec(statement)
	if err != nil {
		return err
	}
	return nil
}

// convertData will convert the incoming string slice into an interface slice
// notes:
// - integer types smallint, integer, and bigint are returned as int64
// - floating-point types real and double precision are returned as float64
// - character types char, varchar, and text are returned as string
// - temporal types date, time, timetz, timestamp, and timestamptz are
//   returned as time.Time
// - the boolean type is returned as bool
// - the bytea type is returned as []byte
func (l *Loader) convertData(data []string) []interface{} {
	var err error
	result := make([]interface{}, len(data))
	for i, v := range data {
		switch l.colTypes[i] {
		case "smallint", "integer", "bigint":
			if v == "" {
				result[i] = nil
				continue
			}
			result[i], err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				l.log.WithError(err).
					Warnf("Unable to convert %v to int", v)
				result[i] = v
			}
		case "real", "double precision":
			if v == "" {
				result[i] = nil
				continue
			}
			result[i], err = strconv.ParseFloat(v, 64)
			if err != nil {
				l.log.WithError(err).
					Warnf("Unable to convert %v to float", v)
				result[i] = v
			}
		case "boolean":
			result[i], err = strconv.ParseBool(v)
			if err != nil {
				l.log.WithError(err).
					Warnf("Unable to convert %v to boolean", v)
				result[i] = v
			}
		default:
			result[i] = v
		}
	}
	return result
}

// DoWork receives data from dataCh and processes the data accordingly
func (l *Loader) DoWork(dataCh chan []string) {
	var (
		txn         *sql.Tx
		stmt        *sql.Stmt
		err         error
		upsertQuery string
		tableName   string
	)
	defer l.wg.Done()
	currentCount := 0
	l.log.Infof("Worker %d Starting Up", l.id)

	if l.upsert {
		tableName = fmt.Sprintf("tmp_%s", l.table)
	} else {
		tableName = l.table
	}

	for {
		select {
		case data := <-dataCh:
			if l.colTypes == nil {
				l.log.Infof("Worker %d fetching column types")
				l.getColumnTypes()
				if l.upsert {
					l.getPrimaryKeys()
					err = l.makeTempTable()
					if err != nil {
						l.log.WithError(err).
							Fatal("Unable to make temporary table")
					}
					upsertQuery = l.makeUpsertQuery()
				}
			}
			if currentCount == l.batchSize {
				l.log.Infof("Worker %d commiting batch", l.id)
				_, err = stmt.Exec()
				if err != nil {
					l.log.WithError(err).Fatal("Unable to copy data")
				}
				if l.upsert {
					l.log.Infof("Worker %d performing upsert", l.id)
					_, err = txn.Exec(upsertQuery)
					if err != nil {
						l.log.WithError(err).Fatal("Unable to perform upsert")
					}
				}
				err = txn.Commit()
				if err != nil {
					l.log.WithError(err).Fatal("Unable to commit")
				}
				if l.upsert {
					l.log.Infof("Worker %d clearing temp table", l.id)
					_, err := l.ctx.Exec(
						fmt.Sprintf("TRUNCATE TABLE %s", tableName),
					)
					if err != nil {
						l.log.WithError(err).
							Fatal("Unable to truncate temp table")
					}
				}
				currentCount = 0
			}

			if currentCount == 0 {
				l.log.Infof("Worker %d preparing new transaction", l.id)
				txn, err = l.ctx.Begin()
				if err != nil {
					l.log.WithError(err).Fatal("Unable to begin transaction")
				}
				stmt, err = txn.Prepare(pq.CopyIn(tableName, l.headers...))
				if err != nil {
					l.log.WithError(err).Fatal("Unable to prepare copy")
				}
			}
			convertedData := l.convertData(data)
			_, err = stmt.Exec(convertedData...)
			if err != nil {
				l.log.WithError(err).
					Fatalf(
						"Unable to import \nrow: %v\n converted row: %v\n",
						data,
						convertedData,
					)
			}
			currentCount++

		case <-l.doneCh:
			l.log.Info("Received Shutdown Flag. Cleaning up")
			l.log.Infof("Worker %d commiting batch", l.id)
			_, err = stmt.Exec()
			if err != nil {
				l.log.WithError(err).Fatal("Unable to copy data")
			}
			if l.upsert {
				l.log.Infof("Worker %d performing upsert", l.id)
				_, err = txn.Exec(upsertQuery)
				if err != nil {
					l.log.WithError(err).Fatal("Unable to perform upsert")
				}
			}
			err = txn.Commit()
			if err != nil {
				l.log.WithError(err).
					Fatal("Unable to commit transaction")
			}
			if l.upsert {
				l.log.Infof("Worker %d removing temp table", l.id)
				_, err = l.ctx.Exec(
					fmt.Sprintf("DROP TABLE %s", tableName),
				)
				if err != nil {
					l.log.WithError(err).
						Warn("Unable to drop temp table")
				}
			}
			return
		}
	}
}
