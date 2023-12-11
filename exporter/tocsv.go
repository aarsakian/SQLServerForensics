package exporter

import (
	mslogger "MSSQLParser/logger"
	"MSSQLParser/utils"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

func writeCSV(records utils.Records, filename string, folder string) {

	file, err := os.Create(fmt.Sprintf("%s.csv", filepath.Join(folder, filename)))
	defer file.Close()
	if err != nil {
		mslogger.Mslogger.Error(fmt.Sprintf("failed to open file %s", err))
	}

	w := csv.NewWriter(file)

	w.WriteAll(records)

	// Write any buffered data to the underlying writer (standard output).
	w.Flush()

	if err := w.Error(); err != nil {
		log.Fatal(err)
	}
}
