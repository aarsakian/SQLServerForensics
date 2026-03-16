package exporter

import (
	"MSSQLParser/db"
	"MSSQLParser/db/tables"
	mslogger "MSSQLParser/logger"
	"MSSQLParser/utils"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"text/template"
)

type HTMLExporter struct {
}

type tableTemplateData struct {
	Headers     []string
	PrevPage    string
	NextPage    string
	CurPage     string
	Name        string
	IsPaginated bool
}

func paginatedPageName(filename string, page int) string {
	return fmt.Sprintf("%s_%d.html", filename, page)
}

func WriteHTML(wg *sync.WaitGroup, records <-chan utils.Record, filename string,
	folder string, headers []string) {
	defer wg.Done()

	var paginatedFile *os.File
	var paginatedfpath string

	msg := fmt.Sprintf("Exporting data from %s", filename)
	fmt.Printf(msg + " \n")
	mslogger.Mslogger.Info(msg)

	tmpl, err := template.ParseFiles("templates/table.tmpl")
	if err != nil {
		log.Fatal(err)
	}

	data := tableTemplateData{Headers: headers,
		Name: filename, IsPaginated: false}

	fpath := filepath.Join(folder, fmt.Sprintf("%s.html", filename))
	file, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	if err = tmpl.ExecuteTemplate(file, "header", data); err != nil {
		log.Fatal(err)
	}

	nofRows := 0
	RowsPerPage := 1000

	data.IsPaginated = false

	for record := range records {
		// Render paginated HTML header
		if nofRows%RowsPerPage == 0 {
			data.IsPaginated = true
			paginatedfpath = filepath.Join(folder, fmt.Sprintf("%s_%d.html", filename, nofRows/RowsPerPage))
			paginatedFile, err = os.OpenFile(paginatedfpath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)

			data.NextPage = paginatedPageName(filename, nofRows/RowsPerPage+1)
			data.CurPage = fmt.Sprintf("%d", nofRows/RowsPerPage+1)

			if nofRows/RowsPerPage-1 < 0 {
				data.PrevPage = ""
			} else {
				data.PrevPage = paginatedPageName(filename, nofRows/RowsPerPage-1)
			}

			if err = tmpl.ExecuteTemplate(paginatedFile, "header", data); err != nil {
				log.Fatal(err)
			}
			defer paginatedFile.Close()
		}

		// Render paginated HTML row
		data.IsPaginated = true
		if err := tmpl.ExecuteTemplate(paginatedFile, "row", record.Vals); err != nil {
			log.Fatal(err)
		}

		// Render full HTML row
		data.IsPaginated = false
		if err := tmpl.ExecuteTemplate(file, "row", record.Vals); err != nil {
			log.Fatal(err)
		}
		nofRows++

		// Render paginated HTML footer
		if nofRows%RowsPerPage == 0 {
			data.IsPaginated = true
			if err := tmpl.ExecuteTemplate(paginatedFile, "footer", data); err != nil {
				log.Fatal(err)
			}
		}
	}

	// Render full HTML footer
	data.IsPaginated = false
	if err := tmpl.ExecuteTemplate(file, "footer", data); err != nil {
		log.Fatal(err)
	}

}

func writeSchema(schema []tables.Column, folder string, filename string) {
	fpath := filepath.Join(folder, fmt.Sprintf("%s_schema.html", filename))
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

func writeTOC(tmpl *template.Template, indexFile *os.File, table db.Table) {

	exportPaths := make([]string, 0, len(table.Indexes)+2)

	exportPaths = append(exportPaths, filepath.ToSlash(filepath.Join(table.Type, table.Name+".html")))
	exportPaths = append(exportPaths, filepath.ToSlash(filepath.Join(table.Type, fmt.Sprintf("%s_0.html", table.Name))))
	exportPaths = append(exportPaths, filepath.ToSlash(filepath.Join(table.Type, table.Name+"_schema.html")))

	for _, index := range table.Indexes {
		exportPaths = append(exportPaths, filepath.ToSlash(filepath.Join(table.Type, fmt.Sprintf("%s_index.html", index.Name))))
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
	if err := tmpl.ExecuteTemplate(indexFile, "row", rowData); err != nil {
		log.Fatal(err)
	}
}

func writeIndexRecords(wg *sync.WaitGroup, tableName string, folder string,
	indexRecords <-chan utils.Record, index tables.Index) {

	defer wg.Done()
	fpath := filepath.Join(folder, fmt.Sprintf("%s_index.html", index.Name))
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
	msg := fmt.Sprintf("Exporting index %s data from %s", index.Name, tableName)
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
