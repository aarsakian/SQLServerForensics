package exporter

import (
	db "MSSQLParser/db"
	"MSSQLParser/utils"
	"log"
	"os"
)

type Writer interface {
	write(records utils.Records)
}

type Exporter struct {
	Format string
}

func (exp Exporter) Export(database db.Database, tablename string) {
	var records utils.Records

	err := os.Mkdir(database.Name, 0750)
	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}

	for _, table := range database.Tables {

		records = table.GetRecords()

		if exp.Format == "csv" {
			writeCSV(records, table.Name, database.Name)
		}

	}

}
