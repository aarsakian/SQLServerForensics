package exporter

import (
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
