package exporter

import (
	"MSSQLParser/db"
	"MSSQLParser/utils"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"text/template"
)

type Writer interface {
	write(records utils.Records)
}

type Exporter struct {
	Format string
	Image  bool
	Path   string
	Index  bool
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

	var tmpl *template.Template
	var indexFile *os.File

	databaseName = filepath.Base(databaseName)
	err := os.RemoveAll(filepath.Join(exp.Path, databaseFolder, databaseName))
	if err != nil {
		log.Fatal(err)
	}
	err = os.MkdirAll(filepath.Join(exp.Path, databaseFolder, databaseName), 0750)
	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}

	if exp.Format == "html" {
		indexPath := filepath.Join(exp.Path, databaseFolder, databaseName, "index.html")
		indexFile, err = os.Create(indexPath)
		if err != nil {
			log.Fatal(err)
		}
		defer indexFile.Close()

		funcMap := template.FuncMap{
			"pathBase":   path.Base,
			"replaceAll": strings.ReplaceAll,
		}

		tmpl = template.New("templates/index.tmpl").Funcs(funcMap)

		tmpl, err := tmpl.ParseFiles("templates/index.tmpl")
		if err != nil {
			log.Fatal(err)
		}

		if err := tmpl.ExecuteTemplate(indexFile, "header", struct {
			DatabaseName string
		}{
			DatabaseName: databaseName,
		}); err != nil {
			log.Fatal(err)
		}

	}

	for table := range tables {

		expPath := exp.CreateExportPath(databaseFolder, databaseName, table.Type)

		records := make(chan utils.Record, 1000)

		headers := table.GetHeader(colnames)

		indexRecordsMap := make(map[string]chan utils.Record)

		wg := new(sync.WaitGroup)

		wg.Add(1)
		go table.GetRecords(wg, selectedTableRow, colnames, records)

		for _, index := range table.Indexes {

			wg.Add(1)
			indexRecordsMap[index.Name] = make(chan utils.Record, 100)
			go index.GetRecords(wg, selectedTableRow, colnames, indexRecordsMap[index.Name])
		}

		if exp.Image {
			images := make(chan utils.Image, 10)
			wg.Add(1)
			go table.GetImages(wg, images)
			wg.Add(1)
			go writeImages(wg, images, table.Name, expPath)

		}

		switch exp.Format {
		case "csv":
			wg.Add(1)
			go WriteCSV(wg, records, table.Name, expPath, headers)
			wg.Wait()
		case "html":

			writeTOC(tmpl, indexFile, table)
			writeSchema(table.Schema, expPath, table.Name)
			wg.Add(1)
			go WriteHTML(wg, records, table.Name, expPath, headers)

			for _, index := range table.Indexes {

				wg.Add(1)
				go writeIndexRecords(wg, table.Name, expPath, indexRecordsMap[index.Name], index)
			}

			wg.Wait()
		}
	}

	if exp.Format == "html" {
		if err := tmpl.ExecuteTemplate(indexFile, "footer", nil); err != nil {
			log.Fatal(err)
		}
	}

}
