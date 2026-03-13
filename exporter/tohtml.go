package exporter

import (
	"MSSQLParser/db"
	mslogger "MSSQLParser/logger"
	"MSSQLParser/utils"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"text/template"
)

func WriteHTML(wg *sync.WaitGroup, records <-chan utils.Record, filename string,
	folder string, headers []string) {
	defer wg.Done()

	fpath := filepath.Join(folder, fmt.Sprintf("%s.html", filename))
	file, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)

	if err != nil {
		mslogger.Mslogger.Error(fmt.Sprintf("failed to open file %s", err))
		fmt.Printf("%s\n", err)
	}
	defer file.Close()

	tmpl, err := template.ParseFiles("templates/table.tmpl")
	if err != nil {
		log.Fatal(err)
	}

	msg := fmt.Sprintf("Exporting data from %s", filename)
	fmt.Printf(msg + " \n")
	mslogger.Mslogger.Info(msg)

	data := struct{ Headers []string }{Headers: headers}
	if err := tmpl.ExecuteTemplate(file, "header", data); err != nil {
		log.Fatal(err)
	}

	for record := range records {
		if err := tmpl.ExecuteTemplate(file, "row", record.Vals); err != nil {
			log.Fatal(err)
		}
	}

}

func writeSchema(schema []db.Column, folder string, filename string) {
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

		Columns []db.Column
	}{
		TableName: filename,

		Columns: schema,
	}

	if err := tmpl.ExecuteTemplate(file, "schema", data); err != nil {
		log.Fatal(err)
	}
}

func writeIndex(tmpl *template.Template, indexFile *os.File, table db.Table) {
	htmlPath := filepath.ToSlash(filepath.Join(table.Type, table.Name+".html"))
	schemaPath := filepath.ToSlash(filepath.Join(table.Type, table.Name+"_schema.html"))
	rowData := struct {
		Name           string
		Type           string
		PageIDsPerType map[string][]uint32
		HTMLPath       string
		SchemaPath     string
	}{
		Name:           table.Name,
		Type:           table.Type,
		PageIDsPerType: table.PageIDsPerType,
		HTMLPath:       htmlPath,
		SchemaPath:     schemaPath,
	}
	if err := tmpl.ExecuteTemplate(indexFile, "row", rowData); err != nil {
		log.Fatal(err)
	}
}
