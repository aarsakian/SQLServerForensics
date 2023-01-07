package exporter

import (
	"MSSQLParser/utils"
	"encoding/csv"
	"fmt"
	"log"
	"os"
)

func writeCSV(records utils.Records, filename string) {

	file, err := os.Create(fmt.Sprintf("%s.csv", filename))
	defer file.Close()
	if err != nil {
		log.Fatalln("failed to open file", err)
	}
	w := csv.NewWriter(file)

	w.WriteAll(records)

	// Write any buffered data to the underlying writer (standard output).
	w.Flush()

	if err := w.Error(); err != nil {
		log.Fatal(err)
	}
}
