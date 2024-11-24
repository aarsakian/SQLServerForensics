package exporter

import (
	mslogger "MSSQLParser/logger"
	"MSSQLParser/utils"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

func writeCSV(wg *sync.WaitGroup, records <-chan utils.Record, filename string, folder string) {
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

	for record := range records {
		w.Write(record)
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
