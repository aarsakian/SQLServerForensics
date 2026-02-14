package exporter

import (
	"MSSQLParser/db"
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

func (exp Exporter) CreateExportPath(databaseFolder string,
	databaseName string, tableType string) string {

	expPath := filepath.Join(exp.Path, databaseFolder, databaseName, tableType)

	err := os.Mkdir(expPath, 0750)
	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}
	return expPath

}

func (exp Exporter) Export(expWg *sync.WaitGroup, selectedTableRow []int, colnames []string,
	databaseName string, databaseFolder string, tables <-chan db.Table) {
	defer expWg.Done()

	databaseName = filepath.Base(databaseName)
	err := os.RemoveAll(filepath.Join(exp.Path, databaseFolder, databaseName))
	if err != nil {
		log.Fatal(err)
	}
	err = os.MkdirAll(filepath.Join(exp.Path, databaseFolder, databaseName), 0750)
	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}

	for table := range tables {

		expPath := exp.CreateExportPath(databaseFolder, databaseName, table.Type)
		wg := new(sync.WaitGroup)
		wg.Add(1)
		records := make(chan utils.Record, 1000)

		go table.GetRecords(wg, selectedTableRow, colnames, records)

		if exp.Image {
			images := make(chan utils.Image, 10)
			wg.Add(1)
			go table.GetImages(wg, images)
			wg.Add(1)
			go writeImages(wg, images, table.Name, expPath)

		}

		if exp.Format == "csv" {
			wg.Add(1)
			go WriteCSV(wg, records, table.Name, expPath)
			wg.Wait()
		}
	}

}
