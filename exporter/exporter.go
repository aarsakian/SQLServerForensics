package exporter

import (
	"MSSQLParser/db"
	"MSSQLParser/db/tables"
	"MSSQLParser/utils"
	"embed"
	"html/template"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

//go:embed templates/*tmpl
var tmplFS embed.FS

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

func exportedRowCount(table *db.Table, selectedTableRow []int) int {
	if len(selectedTableRow) == 0 {
		return len(table.Rows)
	}

	rowCount := 0
	for _, rowNum := range selectedTableRow {
		if rowNum >= 0 && rowNum < len(table.Rows) {
			rowCount++
		}
	}

	return rowCount
}

func sanitizeDatabaseFolder(databaseFolder string) string {
	cleaned := filepath.Clean(databaseFolder)
	if cleaned == "." {
		return ""
	}

	cleaned = strings.TrimPrefix(cleaned, filepath.VolumeName(cleaned))
	cleaned = strings.TrimLeft(cleaned, `/\\`)

	parts := strings.FieldsFunc(cleaned, func(r rune) bool {
		return r == '\\' || r == '/'
	})

	safeParts := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			continue
		}
		safeParts = append(safeParts, part)
	}

	if len(safeParts) == 0 {
		return ""
	}

	return filepath.Join(safeParts...)
}

func (exp Exporter) CreateExportPath(databaseName string, dbName string, tableType string, tableName string) string {

	expPath := filepath.Join(exp.Path, databaseName, dbName, tableType, tableName)

	err := os.MkdirAll(expPath, 0750)
	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}
	return expPath

}

func (exp Exporter) Export(expWg *sync.WaitGroup, selectedTableRow []int, colnames []string,
	databaseName string, sourceFilename string, databaseFolder string, dbName string, tables <-chan *db.Table) {
	defer expWg.Done()

	databaseFolder = sanitizeDatabaseFolder(databaseFolder)

	var tocTmpl *template.Template
	var writer Writer
	var indexFile *os.File

	err := os.RemoveAll(filepath.Join(exp.Path, databaseName, dbName))
	if err != nil {
		log.Fatal(err)
	}
	err = os.MkdirAll(filepath.Join(exp.Path, databaseName, dbName), 0750)
	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}

	if exp.Format == "html" {
		indexPath := filepath.Join(exp.Path, databaseName, dbName, "index.html")
		indexFile, err = os.Create(indexPath)
		if err != nil {
			log.Fatal(err)
		}
		defer indexFile.Close()
		tocTmpl, err = template.New("toc.tmpl").Funcs(funcMap).ParseFS(tmplFS, "templates/toc.tmpl")
		if err != nil {
			log.Fatal(err)
		}
		writeTOCHeader(tocTmpl, indexFile, sourceFilename, dbName)
	}

	for table := range tables {

		wg := new(sync.WaitGroup)

		expPath := exp.CreateExportPath(databaseName, dbName, table.Type, table.Name)

		if exp.Blobs {
			wginner := new(sync.WaitGroup)
			blobs := make(chan utils.Blob, 10)

			wginner.Add(1)
			go table.GetBlobs(wginner, blobs)
			wginner.Add(1)
			go writeBlobs(wginner, blobs, expPath)
			wginner.Wait()

		}
		if exp.Format != "html" && exp.Format != "csv" && exp.Format != "xlsx" {
			continue
		}

		records := make(chan utils.Record, 1000)

		headers := table.GetHeader(colnames)

		switch exp.Format {
		case "html":
			hTMLExporter := HTMLExporter{Path: expPath, SourceFilename: sourceFilename, DatabaseName: databaseName}
			hTMLExporter.InitalizeTemplates()

			writer = hTMLExporter
		case "csv":
			csvExporter := CSVExporter{Path: expPath}
			writer = csvExporter
		case "xlsx":
			xlsxExporter := &XLSXExporter{Path: expPath}
			xlsxExporter.InitializeXlsxTemplates(table.Name)
			writer = xlsxExporter
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

		if xlsxExporter, ok := writer.(*XLSXExporter); ok {
			if err := xlsxExporter.Close(); err != nil {
				log.Fatal(err)
			}
		}

		if exp.Format == "html" {
			paginatedPath := filepath.Join(expPath, table.Name+"_0.html")
			_, err := os.Stat(paginatedPath)
			includePaginated := err == nil
			if err != nil && !os.IsNotExist(err) {
				log.Fatal(err)
			}

			writeTOC(tocTmpl, indexFile, table, exportedRowCount(table, selectedTableRow), includePaginated, exp.Schema, exp.Index)
		}

	}
}
