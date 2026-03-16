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
	Image  bool
	Path   string
	Index  bool
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

	//var tmpl *template.Template
	//var indexFile *os.File
	var writers []Writer
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

		if exp.Image {
			images := make(chan utils.Image, 10)
			wg.Add(1)
			go table.GetImages(wg, images)
			wg.Add(1)
			go writeImages(wg, images, table.Name, expPath)

		}

		records := make(chan utils.Record, 1000)

		headers := table.GetHeader(colnames)

		if exp.Format == "html" {
			hTMLExporter := HTMLExporter{Path: expPath, Filename: databaseName}
			hTMLExporter.InitalizeTemplates()
			writeTOC(tocTmpl, indexFile, table)

			writers = append(writers, hTMLExporter)
		} else if exp.Format == "csv" {
			csvExporter := CSVExporter{Path: expPath, Filename: databaseName}
			writers = append(writers, csvExporter)
		}

		indexRecordsMap := make(map[string]chan utils.Record)

		wg.Add(1)
		go table.GetRecords(wg, selectedTableRow, colnames, records)

		for _, index := range table.Indexes {

			wg.Add(1)
			indexRecordsMap[index.Name] = make(chan utils.Record, 100)
			go index.GetRecords(wg, selectedTableRow, colnames, indexRecordsMap[index.Name])
		}

		for _, writer := range writers {
			writer.WriteSchema(table.Schema, table.Name)
			wg.Add(1)
			go writer.WriteRecords(wg, records, headers, table.Name)

			for _, index := range table.Indexes {
				wg.Add(1)
				go writer.WriteIndexRecords(wg, table.Name, indexRecordsMap[index.Name], index)
			}

		}
		wg.Wait()
	}
}
