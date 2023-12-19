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
	fpath := filepath.Join(folder, filename)
	file, err := os.Create(fmt.Sprintf("%s.csv", fpath))

	if err != nil {
		mslogger.Mslogger.Error(fmt.Sprintf("failed to open file %s", err))
	}
	defer file.Close()
	w := csv.NewWriter(file)

	w.WriteAll(records)

	// Write any buffered data to the underlying writer (standard output).
	w.Flush()

	if err := w.Error(); err != nil {
		log.Fatal(err)
	}
	//len(records) - header
	msg := fmt.Sprintf("Exported %d rows to %s", len(records)-1, fpath)
	mslogger.Mslogger.Info(msg)
	fmt.Printf(msg + "\n")

}
