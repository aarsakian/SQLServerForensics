package exporter

import (
	"MSSQLParser/db"
	"MSSQLParser/db/tables"
	"MSSQLParser/utils"
	"html/template"
	"log"
	"os"
	"path/filepath"
	"sync"
)

type Writer interface {
	WriteRecords(*sync.WaitGroup, <-chan utils.Record, []string, string)

	WriteSchema([]tables.Column, string)
	WriteIndexRecords(*sync.WaitGroup, string, <-chan utils.Record, tables.Index)
}

type Exporter struct {
	Format string
	Blobs  bool
	Path   string
	Index  bool
	Schema bool
}

func (exp Exporter) CreateExportPath(databaseFolder string,
	databaseName string, tableType string, tableName string) string {

	expPath := filepath.Join(exp.Path, databaseFolder, databaseName, tableType, tableName)

	err := os.MkdirAll(expPath, 0750)
	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}
	return expPath

}

func (exp Exporter) Export(expWg *sync.WaitGroup, selectedTableRow []int, colnames []string,
	databaseName string, databaseFolder string, tables <-chan db.Table) {
	defer expWg.Done()

	var writer Writer
	databaseName = filepath.Base(databaseName)
	err := os.RemoveAll(filepath.Join(exp.Path, databaseFolder, databaseName))
	if err != nil {
		log.Fatal(err)
	}
	err = os.MkdirAll(filepath.Join(exp.Path, databaseFolder, databaseName), 0750)
	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}
	indexPath := filepath.Join(exp.Path, databaseFolder, databaseName, "toc.html")
	indexFile, err := os.Create(indexPath)
	if err != nil {
		log.Fatal(err)
	}
	defer indexFile.Close()

	tocTmpl, err := template.New("toc.tmpl").Funcs(funcMap).ParseFiles("templates/toc.tmpl")
	if err != nil {
		log.Fatal(err)
	}

	writeTOCHeader(tocTmpl, indexFile, databaseName)
	for table := range tables {

		wg := new(sync.WaitGroup)

		expPath := exp.CreateExportPath(databaseFolder, databaseName, table.Type, table.Name)

		if exp.Blobs {
			wginner := new(sync.WaitGroup)
			blobs := make(chan utils.Blob, 10)

			wginner.Add(1)
			go table.GetBlobs(wginner, blobs)
			wginner.Add(1)
			go writeBlobs(wginner, blobs, expPath)
			wginner.Wait()

		}
		if exp.Format != "html" && exp.Format != "csv" {
			continue
		}

		records := make(chan utils.Record, 1000)

		headers := table.GetHeader(colnames)

		if exp.Format == "html" {
			hTMLExporter := HTMLExporter{Path: expPath}
			hTMLExporter.InitalizeTemplates()
			writeTOC(tocTmpl, indexFile, table, exp.Schema, exp.Index)

			writer = hTMLExporter
		} else if exp.Format == "csv" {
			csvExporter := CSVExporter{Path: expPath}
			writer = csvExporter
		}

		indexRecordsMap := make(map[string]chan utils.Record)

		wg.Add(1)
		go table.GetRecords(wg, selectedTableRow, colnames, records)

		if exp.Index {
			for _, index := range table.Indexes {

				wg.Add(1)
				indexRecordsMap[index.Name] = make(chan utils.Record, 100)
				go index.GetRecords(wg, selectedTableRow, colnames, indexRecordsMap[index.Name])
			}
		}
		if exp.Schema {
			writer.WriteSchema(table.Schema, table.Name)
		}

		wg.Add(1)
		go writer.WriteRecords(wg, records, headers, table.Name)

		if exp.Index {
			for _, index := range table.Indexes {
				wg.Add(1)
				go writer.WriteIndexRecords(wg, table.Name, indexRecordsMap[index.Name], index)
			}
		}
		wg.Wait()

	}
}
