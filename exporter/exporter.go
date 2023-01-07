package exporter

import (
	db "MSSQLParser/db"
	"MSSQLParser/utils"
)

type Writer interface {
	write(records utils.Records)
}

type Exporter struct {
	Format string
}

func (exp Exporter) Export(database db.Database, tablename string) {
	var records utils.Records

	for _, table := range database.Tables {
		if table.Name != tablename {
			continue
		}
		records = table.GetRecords()
		if exp.Format == "csv" {
			writeCSV(records, table.Name)
		}

	}

}
