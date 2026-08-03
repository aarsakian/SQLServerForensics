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
	"slices"
	"strings"
	"sync"
)

type HTMLExporter struct {
	Path           string
	SourceFilename string
	DatabaseName   string
	Templates      map[string]*template.Template
}

type rowTemplateData struct {
	Row             utils.Record
	IncludeLogInfo  bool
	IncludePageInfo bool
}

type tableTemplateData struct {
	Headers     []string
	PrevPage    string
	NextPage    string
	CurPage     string
	Name        string
	SourceFile  string
	Database    string
	IsPaginated bool
}

var funcMap = template.FuncMap{
	"pathBase":   path.Base,
	"replaceAll": strings.ReplaceAll,
}

func (h *HTMLExporter) InitalizeTemplates() {
	h.Templates = make(map[string]*template.Template)
	var err error

	h.Templates["table"], err = template.New("table.tmpl").Funcs(funcMap).ParseFS(tmplFS, "templates/table.tmpl")
	if err != nil {
		log.Fatal(err)
	}

	h.Templates["schema"], err = template.New("schema.tmpl").Funcs(funcMap).ParseFS(tmplFS, "templates/schema.tmpl")
	if err != nil {
		log.Fatal(err)
	}

	h.Templates["indexes"], err = template.New("index_records.tmpl").Funcs(funcMap).ParseFS(tmplFS, "templates/index_records.tmpl")
	if err != nil {
		log.Fatal(err)
	}

}

func paginatedPageName(filename string, page int) string {
	return fmt.Sprintf("%s_%d.html", filename, page)
}

func (h HTMLExporter) WriteRecords(wg *sync.WaitGroup, records <-chan utils.Record,
	headers []string, filename string, includePageInfo bool, includeLogInfo bool) {
	defer wg.Done()

	var paginatedFile *os.File

	msg := fmt.Sprintf("Exporting data from %s to %s", filename, h.Path)
	fmt.Printf("%s \n", msg)
	mslogger.Mslogger.Info(msg)

	//last two are page - log related header
	if !includePageInfo && !includeLogInfo {
		headers = slices.Delete(headers, len(headers)-2, len(headers))
	} else if !includeLogInfo && includePageInfo {
		headers = slices.Delete(headers, len(headers)-2, len(headers)-1)
	} else if !includePageInfo && includeLogInfo {
		headers = slices.Delete(headers, len(headers)-1, len(headers))
	}

	data := tableTemplateData{Headers: headers,
		Name: filename, SourceFile: h.SourceFilename, Database: h.DatabaseName, IsPaginated: false}

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
	pageIdx := -1
	pageRowCount := 0
	paginationEnabled := false
	firstPageBuffer := make([]rowTemplateData, 0, RowsPerPage)

	openPaginatedPage := func(idx int) {
		var err error
		fpath := filepath.Join(h.Path, fmt.Sprintf("%s_%d.html", filename, idx))
		paginatedFile, err = os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			log.Fatal(err)
		}

		pageData := data
		pageData.IsPaginated = true
		pageData.CurPage = fmt.Sprintf("%d", idx+1)
		if idx == 0 {
			pageData.PrevPage = ""
		} else {
			pageData.PrevPage = paginatedPageName(filename, idx-1)
		}
		pageData.NextPage = ""

		if err = h.Templates["table"].ExecuteTemplate(paginatedFile, "header", pageData); err != nil {
			log.Fatal(err)
		}
	}

	closePaginatedPage := func(hasNext bool) {
		if paginatedFile == nil {
			return
		}

		pageData := data
		pageData.IsPaginated = true
		pageData.CurPage = fmt.Sprintf("%d", pageIdx+1)
		if pageIdx == 0 {
			pageData.PrevPage = ""
		} else {
			pageData.PrevPage = paginatedPageName(filename, pageIdx-1)
		}
		if hasNext {
			pageData.NextPage = paginatedPageName(filename, pageIdx+1)
		} else {
			pageData.NextPage = ""
		}

		if err := h.Templates["table"].ExecuteTemplate(paginatedFile, "footer", pageData); err != nil {
			log.Fatal(err)
		}
		if err := paginatedFile.Close(); err != nil {
			log.Fatal(err)
		}
		paginatedFile = nil
	}

	for record := range records {
		// Render full HTML row
		data.IsPaginated = false

		rowTmpl := rowTemplateData{Row: record, IncludeLogInfo: includeLogInfo, IncludePageInfo: includePageInfo}

		if err := h.Templates["table"].ExecuteTemplate(file, "row", rowTmpl); err != nil {
			log.Fatal(err)
		}
		nofRows++

		if !paginationEnabled {
			firstPageBuffer = append(firstPageBuffer, rowTmpl)
			if nofRows == RowsPerPage {
				paginationEnabled = true
				pageIdx = 0
				openPaginatedPage(pageIdx)
				for _, bufferedRecord := range firstPageBuffer {
					if err := h.Templates["table"].ExecuteTemplate(paginatedFile, "row", bufferedRecord); err != nil {
						log.Fatal(err)
					}
				}
				pageRowCount = len(firstPageBuffer)
				firstPageBuffer = nil
			}
			continue
		}

		if pageRowCount == RowsPerPage {
			closePaginatedPage(true)
			pageIdx++
			openPaginatedPage(pageIdx)
			pageRowCount = 0
		}

		if err := h.Templates["table"].ExecuteTemplate(paginatedFile, "row", record); err != nil {
			log.Fatal(err)
		}
		pageRowCount++
	}

	if paginationEnabled {
		closePaginatedPage(false)
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

	tmpl, err := template.ParseFS(tmplFS, "templates/schema.tmpl")
	if err != nil {
		log.Fatal(err)
	}

	data := struct {
		TableName  string
		SourceFile string
		Database   string

		Columns []tables.Column
	}{
		TableName:  filename,
		SourceFile: h.SourceFilename,
		Database:   h.DatabaseName,

		Columns: schema,
	}

	if err := tmpl.ExecuteTemplate(file, "schema", data); err != nil {
		log.Fatal(err)
	}
}

func writeTOCHeader(tocTmpl *template.Template, indexFile *os.File, sourceFilename string, databaseName string) {
	data := struct {
		Filename     string
		DatabaseName string
	}{
		Filename:     sourceFilename,
		DatabaseName: databaseName,
	}
	if err := tocTmpl.ExecuteTemplate(indexFile, "header", data); err != nil {
		log.Fatal(err)
	}

}

func writeTOC(tocTmpl *template.Template, indexFile *os.File, table *db.Table,
	rowCount int, includePaginated bool, includeSchema bool, includeIndexes bool) {

	exportPaths := make([]string, 0, len(table.Indexes)+2)

	exportPaths = append(exportPaths, filepath.ToSlash(filepath.Join(table.Type, table.Name, table.Name+".html")))
	if includePaginated {
		exportPaths = append(exportPaths, filepath.ToSlash(filepath.Join(table.Type, table.Name, fmt.Sprintf("%s_0.html", table.Name))))
	}
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
		RowCount       int
		PageIDsPerType map[string][]uint32
		ExportPaths    []string
	}{
		Name:           table.Name,
		Type:           table.Type,
		RowCount:       rowCount,
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
	tmpl, err := template.ParseFS(tmplFS, "templates/index_records.tmpl")
	if err != nil {
		log.Fatal(err)
	}
	msg := fmt.Sprintf("Exporting index %s data from %s to %s", index.Name, tableName, fpath)
	fmt.Printf("%s \n", msg)
	mslogger.Mslogger.Info(msg)

	var headers []string
	for _, col := range index.Columns {
		headers = append(headers, col.Name)
	}

	data := struct {
		TableName  string
		IndexName  string
		SourceFile string
		Database   string
		Headers    []string
	}{
		TableName:  tableName,
		IndexName:  index.Name,
		SourceFile: h.SourceFilename,
		Database:   h.DatabaseName,
		Headers:    headers,
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
