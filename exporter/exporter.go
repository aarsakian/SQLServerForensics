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

func (exp Exporter) CreateExportPath(databaseName string, tableType string) string {
	expPath := filepath.Join(exp.Path, databaseName, tableType)

	err := os.Mkdir(expPath, 0750)
	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}
	return expPath

}

func (exp Exporter) Export(expWg *sync.WaitGroup, selectedTableRow []int, colnames []string, databaseName string, tables <-chan db.Table) {
	defer expWg.Done()

	if exp.Path == "" {
		return
	}
	var images utils.Images

	err := os.RemoveAll(filepath.Join(exp.Path, databaseName))
	if err != nil {
		log.Fatal(err)
	}
	err = os.MkdirAll(filepath.Join(exp.Path, databaseName), 0750)
	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}

	for table := range tables {

		expPath := exp.CreateExportPath(databaseName, table.Type)
		wg := new(sync.WaitGroup)
		wg.Add(2)
		records := make(chan utils.Record, 1000)

		go table.GetRecords(wg, selectedTableRow, colnames, records)

		if exp.Image {
			images = table.GetImages()

			writeImages(images, table.Name, expPath)

		}

		if exp.Format == "csv" {

			go writeCSV(wg, records, table.Name, expPath)
			wg.Wait()
		}
	}

}
