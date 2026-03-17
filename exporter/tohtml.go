package exporter

import (
	"MSSQLParser/db"
	"MSSQLParser/db/tables"
	mslogger "MSSQLParser/logger"
	"MSSQLParser/utils"
	"fmt"
	"html/template"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

type HTMLExporter struct {
	Path      string
	Templates map[string]*template.Template
}

type tableTemplateData struct {
	Headers     []string
	PrevPage    string
	NextPage    string
	CurPage     string
	Name        string
	IsPaginated bool
}

var funcMap = template.FuncMap{
	"pathBase":   path.Base,
	"replaceAll": strings.ReplaceAll,
}

func (h *HTMLExporter) InitalizeTemplates() {
	h.Templates = make(map[string]*template.Template)
	var err error
	// Check if templates exist
	if _, err := os.Stat("templates/table.tmpl"); os.IsNotExist(err) {
		log.Fatal("table template not found")
	}
	if _, err := os.Stat("templates/schema.tmpl"); os.IsNotExist(err) {
		log.Fatal("schema template not found")
	}
	if _, err := os.Stat("templates/toc.tmpl"); os.IsNotExist(err) {
		log.Fatal("toc template not found")
	}

	if _, err := os.Stat("templates/index_records.tmpl"); os.IsNotExist(err) {
		log.Fatal("index records template not found")
	}

	h.Templates["table"], err = template.New("table.tmpl").Funcs(funcMap).ParseFiles("templates/table.tmpl")
	if err != nil {
		log.Fatal(err)
	}

	h.Templates["schema"], err = template.New("schema.tmpl").Funcs(funcMap).ParseFiles("templates/schema.tmpl")
	if err != nil {
		log.Fatal(err)
	}

	h.Templates["indexes"], err = template.New("index_records.tmpl").Funcs(funcMap).ParseFiles("templates/index_records.tmpl")
	if err != nil {
		log.Fatal(err)
	}

}

func paginatedPageName(filename string, page int) string {
	return fmt.Sprintf("%s_%d.html", filename, page)
}

func (h HTMLExporter) WriteRecords(wg *sync.WaitGroup, records <-chan utils.Record,
	headers []string, filename string) {
	defer wg.Done()

	var paginatedFile *os.File
	var paginatedfpath string

	msg := fmt.Sprintf("Exporting data from %s to %s", filename, h.Path)
	fmt.Printf(msg + " \n")
	mslogger.Mslogger.Info(msg)

	data := tableTemplateData{Headers: headers,
		Name: filename, IsPaginated: false}

	fpath := filepath.Join(h.Path, fmt.Sprintf("%s.html", filename))
	file, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	if err = h.Templates["table"].ExecuteTemplate(file, "header", data); err != nil {
		log.Fatal(err)
	}

	nofRows := 0
	RowsPerPage := 1000

	data.IsPaginated = false

	for record := range records {
		// Render paginated HTML header

		if nofRows%RowsPerPage == 0 {
			data.IsPaginated = true
			paginatedfpath = filepath.Join(h.Path, fmt.Sprintf("%s_%d.html", filename,
				nofRows/RowsPerPage))
			paginatedFile, err = os.OpenFile(paginatedfpath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)

			data.NextPage = paginatedPageName(filename, nofRows/RowsPerPage+1)
			data.CurPage = fmt.Sprintf("%d", nofRows/RowsPerPage+1)

			if nofRows/RowsPerPage-1 < 0 {
				data.PrevPage = ""
			} else {
				data.PrevPage = paginatedPageName(filename, nofRows/RowsPerPage-1)
			}

			if err = h.Templates["table"].ExecuteTemplate(paginatedFile, "header", data); err != nil {
				log.Fatal(err)
			}
			defer paginatedFile.Close()
		}

		// Render paginated HTML row
		data.IsPaginated = true
		if err := h.Templates["table"].ExecuteTemplate(paginatedFile, "row", record); err != nil {
			log.Fatal(err)
		}

		// Render full HTML row
		data.IsPaginated = false
		if err := h.Templates["table"].ExecuteTemplate(file, "row", record); err != nil {
			log.Fatal(err)
		}
		nofRows++

		// Render paginated HTML footer
		if nofRows%RowsPerPage == 0 {
			data.IsPaginated = true
			if err := h.Templates["table"].ExecuteTemplate(paginatedFile, "footer", data); err != nil {
				log.Fatal(err)
			}
		}
	}

	// Render full HTML footer
	data.IsPaginated = false
	data.PrevPage = ""
	data.NextPage = ""
	data.CurPage = ""
	if err := h.Templates["table"].ExecuteTemplate(file, "footer", data); err != nil {
		log.Fatal(err)
	}

}

func (h HTMLExporter) WriteSchema(schema []tables.Column, filename string) {
	fpath := filepath.Join(h.Path, fmt.Sprintf("%s_schema.html", filename))
	file, err := os.Create(fpath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	tmpl, err := template.ParseFiles("templates/schema.tmpl")
	if err != nil {
		log.Fatal(err)
	}

	data := struct {
		TableName string

		Columns []tables.Column
	}{
		TableName: filename,

		Columns: schema,
	}

	if err := tmpl.ExecuteTemplate(file, "schema", data); err != nil {
		log.Fatal(err)
	}
}

func writeTOCHeader(tocTmpl *template.Template, indexFile *os.File, databaseName string) {
	data := struct {
		DatabaseName string
	}{
		DatabaseName: databaseName,
	}
	if err := tocTmpl.ExecuteTemplate(indexFile, "header", data); err != nil {
		log.Fatal(err)
	}

}

func writeTOC(tocTmpl *template.Template, indexFile *os.File, table db.Table,
	includeSchema bool, includeIndexes bool) {

	exportPaths := make([]string, 0, len(table.Indexes)+2)

	exportPaths = append(exportPaths, filepath.ToSlash(filepath.Join(table.Type, table.Name, table.Name+".html")))
	exportPaths = append(exportPaths, filepath.ToSlash(filepath.Join(table.Type, table.Name, fmt.Sprintf("%s_0.html", table.Name))))
	if includeSchema {
		exportPaths = append(exportPaths, filepath.ToSlash(filepath.Join(table.Type, table.Name, table.Name+"_schema.html")))
	}

	if includeIndexes {
		for _, index := range table.Indexes {
			exportPaths = append(exportPaths, filepath.ToSlash(filepath.Join(table.Type, table.Name, fmt.Sprintf("%s_index.html", index.Name))))
		}
	}

	rowData := struct {
		Name           string
		Type           string
		PageIDsPerType map[string][]uint32
		ExportPaths    []string
	}{
		Name:           table.Name,
		Type:           table.Type,
		PageIDsPerType: table.PageIDsPerType,
		ExportPaths:    exportPaths,
	}
	if err := tocTmpl.ExecuteTemplate(indexFile, "row", rowData); err != nil {
		log.Fatal(err)
	}
}

func (h HTMLExporter) WriteIndexRecords(wg *sync.WaitGroup, tableName string,
	indexRecords <-chan utils.Record, index tables.Index) {

	defer wg.Done()
	fpath := filepath.Join(h.Path, fmt.Sprintf("%s_index.html", index.Name))
	file, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)

	if err != nil {
		mslogger.Mslogger.Error(fmt.Sprintf("failed to open file %s", err))
		fmt.Printf("%s\n", err)
	}
	defer file.Close()
	tmpl, err := template.ParseFiles("templates/index_records.tmpl")
	if err != nil {
		log.Fatal(err)
	}
	msg := fmt.Sprintf("Exporting index %s data from %s to %s", index.Name, tableName, fpath)
	fmt.Printf(msg + " \n")
	mslogger.Mslogger.Info(msg)

	var headers []string
	for _, col := range index.Columns {
		headers = append(headers, col.Name)
	}

	data := struct {
		TableName string
		IndexName string
		Headers   []string
	}{
		TableName: tableName,
		IndexName: index.Name,
		Headers:   headers,
	}

	if err := tmpl.ExecuteTemplate(file, "header", data); err != nil {
		log.Fatal(err)
	}

	for record := range indexRecords {
		if err := tmpl.ExecuteTemplate(file, "row", record.Vals); err != nil {
			log.Fatal(err)
			fmt.Printf("%s\n", err)
		}
	}

	if err := tmpl.ExecuteTemplate(file, "footer", nil); err != nil {
		log.Fatal(err)
	}
}
