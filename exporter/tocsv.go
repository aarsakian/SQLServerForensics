package exporter

import (
	"MSSQLParser/utils"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path"
)

func writeCSV(records utils.Records, filename string, folder string) {
	fmt.Printf("Exporting Table %s. \n", filename)
	file, err := os.Create(fmt.Sprintf("%s.csv", path.Join(folder, filename)))
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
