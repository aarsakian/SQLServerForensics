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

	fmt.Printf("Exporting table %s contents to %s\n", filename, fpath)

	for record := range records {
		w.Write(record)
	}
	// Write any buffered data to the underlying writer (standard output).
	w.Flush()

	if err := w.Error(); err != nil {
		log.Fatal(err)
	}
	//len(records) - header
	msg := fmt.Sprintf("Exported %d rows to %s", len(records)-1, fpath)
	mslogger.Mslogger.Info(msg)

}
