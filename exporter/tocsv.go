package exporter

import (
	"MSSQLParser/db/tables"
	mslogger "MSSQLParser/logger"
	"MSSQLParser/utils"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

type CSVExporter struct {
	Filename string
	Path     string
}

func (c CSVExporter) writeHeader(headers []string) {
	fpath := filepath.Join(c.Path, fmt.Sprintf("%s.csv", c.Filename))
	file, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		mslogger.Mslogger.Error(fmt.Sprintf("failed to open file %s", err))
	}
	defer file.Close()
	w := csv.NewWriter(file)
	w.Write(headers)
	w.Flush()
	if err := w.Error(); err != nil {
		log.Fatal(err)
	}
}

func (c CSVExporter) WriteRecords(wg *sync.WaitGroup, records <-chan utils.Record,
	headers []string, filename string) {
	c.writeHeader(headers)
	defer wg.Done()
	fpath := filepath.Join(c.Path, fmt.Sprintf("%s.csv", filename))
	file, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)

	if err != nil {
		mslogger.Mslogger.Error(fmt.Sprintf("failed to open file %s", err))
	}
	defer file.Close()
	w := csv.NewWriter(file)

	msg := fmt.Sprintf("Exporting %d rows from %s", len(records), filename)
	fmt.Printf(msg + " ")
	mslogger.Mslogger.Info(msg)

	for record := range records {
		w.Write(record.Vals)
	}
	// Write any buffered data to the underlying writer (standard output).
	w.Flush()

	if err := w.Error(); err != nil {
		log.Fatal(err)
	}
	//len(records) - header
	msg = fmt.Sprintf("to %s", fpath)
	mslogger.Mslogger.Info(msg)
	fmt.Printf(msg + "\n")

}

func WriteCSV(wg *sync.WaitGroup, records <-chan utils.Record, filename string,
	folder string, headers []string) {
	defer wg.Done()
	fpath := filepath.Join(folder, fmt.Sprintf("%s.csv", filename))
	file, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)

	if err != nil {
		mslogger.Mslogger.Error(fmt.Sprintf("failed to open file %s", err))
	}
	defer file.Close()
	w := csv.NewWriter(file)

	msg := fmt.Sprintf("Exporting %d rows from %s", len(records), filename)
	fmt.Printf(msg + " ")
	mslogger.Mslogger.Info(msg)

	w.Write(headers)

	for record := range records {
		w.Write(record.Vals)
	}
	// Write any buffered data to the underlying writer (standard output).
	w.Flush()

	if err := w.Error(); err != nil {
		log.Fatal(err)
	}
	//len(records) - header
	msg = fmt.Sprintf("to %s", fpath)
	mslogger.Mslogger.Info(msg)
	fmt.Printf(msg + "\n")

}

func (c CSVExporter) WriteIndexRecords(wg *sync.WaitGroup, tableName string,
	indexRecords <-chan utils.Record, index tables.Index) {
	defer wg.Done()
	fpath := filepath.Join(c.Path, fmt.Sprintf("%s_index.csv", index.Name))
	file, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		mslogger.Mslogger.Error(fmt.Sprintf("failed to open file %s", err))
	}
	defer file.Close()
	w := csv.NewWriter(file)

	msg := fmt.Sprintf("Exporting index %s data from %s", index.Name, tableName)
	fmt.Printf(msg + " \n")
	mslogger.Mslogger.Info(msg)

	var headers []string
	for _, col := range index.Columns {
		headers = append(headers, col.Name)
	}

	w.Write(headers)

	for record := range indexRecords {
		w.Write(record.Vals)
	}
	// Write any buffered data to the underlying writer (standard output).
	w.Flush()

	if err := w.Error(); err != nil {
		log.Fatal(err)
	}
	//len(records) - header
	msg = fmt.Sprintf("to %s", fpath)
	mslogger.Mslogger.Info(msg)
	fmt.Printf(msg + "\n")

}

func (c CSVExporter) WriteSchema(schema []tables.Column, filename string) {
	fpath := filepath.Join(c.Path, fmt.Sprintf("%s_schema.csv", filename))
	file, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		mslogger.Mslogger.Error(fmt.Sprintf("failed to open file %s", err))
	}
	defer file.Close()
	w := csv.NewWriter(file)

	msg := fmt.Sprintf("Exporting schema of %s", filename)
	fmt.Printf(msg + " \n")
	mslogger.Mslogger.Info(msg)

	var headers []string
	headers = append(headers, "Column Name", "Data Type", "Length", "Nullable", "Ansi Padded", "Identity", "Computed", "Persisted", "Collation")
	w.Write(headers)

	for _, col := range schema {
		var record []string
		record = append(record, col.Name, col.Type, fmt.Sprintf("%d", col.Size),
			fmt.Sprintf("%t", col.IslNullable), fmt.Sprintf("%d", col.IsAnsiPadded),
			fmt.Sprintf("%t", col.IsIdentity), fmt.Sprintf("%t", col.IsComputed),
			fmt.Sprintf("%t", col.IsPersisted), fmt.Sprintf("%d", col.CollationId))
		w.Write(record)
	}
	w.Flush()

	if err := w.Error(); err != nil {
		log.Fatal(err)
	}

}
