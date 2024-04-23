package exporter

import (
	db "MSSQLParser/db"
	"MSSQLParser/utils"
	"log"
	"os"
	"path/filepath"
	"sync"
)

type Writer interface {
	write(records utils.Records)
}

type Exporter struct {
	Format string
	Image  bool
	Path   string
}

func (exp Exporter) Export(database db.Database, tablename string, tabletype string, selectedTableRow int) {

	var images utils.Images
	var err error
	err = os.RemoveAll(filepath.Join(exp.Path, database.Name))
	if err != nil {
		log.Fatal(err)
	}

	err = os.MkdirAll(filepath.Join(exp.Path, database.Name), 0750)

	if err != nil {
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
			err := os.Mkdir(filepath.Join(exp.Path, database.Name, table.Type), 0750)
			if err != nil && !os.IsExist(err) {
				log.Fatal(err)
			}
		}

		wg := new(sync.WaitGroup)
		wg.Add(2)
		records := make(chan utils.Record, 1000)

		go table.GetRecords(wg, selectedTableRow, records)

		if exp.Image {
			images = table.GetImages()

			writeImages(images, table.Name, filepath.Join(database.GetName(), table.Type))

		}

		if exp.Format == "csv" {

			go writeCSV(wg, records, table.Name, filepath.Join(exp.Path, database.Name, table.Type))
			wg.Wait()
		}

	}

}
