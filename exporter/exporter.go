package exporter

import (
	db "MSSQLParser/db"
	mslogger "MSSQLParser/logger"
	"MSSQLParser/utils"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

type Writer interface {
	write(records utils.Records)
}

type Exporter struct {
	Format string
	Image  bool
}

func (exp Exporter) Export(database db.Database, tablename string, tabletype string) {
	var records utils.Records
	var images utils.Images

	err := os.Mkdir(database.GetName(), 0750)

	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}

	for _, table := range database.Tables {

		if tablename != "all" && table.Name != tablename {

			continue
		}

		if tabletype == "user" && table.Type != "User Table" {
			continue
		}

		if table.Type != "" {
			err := os.Mkdir(filepath.Join(database.GetName(), table.Type), 0750)
			if err != nil && !os.IsExist(err) {
				log.Fatal(err)
			}
		}

		records = table.GetRecords()

		if exp.Image {
			images = table.GetImages()

			writeImages(images, table.Name, filepath.Join(database.GetName(), table.Type))

		}

		if exp.Format == "csv" {
			msg := fmt.Sprintf("Exporting Table %s with %d records", table.Name, len(records)-1)
			mslogger.Mslogger.Info(msg)
			writeCSV(records, table.Name, filepath.Join(database.GetName(), table.Type))
		}

	}

}
