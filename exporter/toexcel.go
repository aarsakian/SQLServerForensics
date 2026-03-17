package exporter

import (
	"MSSQLParser/db/tables"
	"MSSQLParser/utils"
	"archive/zip"
	"encoding/xml"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"sync"
)

type XLSXExporter struct {
	Path string
}

var contentTypesXML = []byte(`<?xml version="1.0" encoding="UTF-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
	<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
</Types>`)

var relsXML = []byte(`<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>`)

var workbookXML = []byte(`<?xml version="1.0" encoding="UTF-8"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
          xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
    <sheet name="Sheet1" sheetId="1" r:id="rId1"/>
  </sheets>
</workbook>`)

var workbookRelsXML = []byte(`<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
	<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>`)

var stylesXML = []byte(`<?xml version="1.0" encoding="UTF-8"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
	<fonts count="1">
		<font>
			<sz val="11"/>
			<name val="Calibri"/>
			<family val="2"/>
		</font>
	</fonts>
	<fills count="4">
		<fill><patternFill patternType="none"/></fill>
		<fill><patternFill patternType="gray125"/></fill>
		<fill><patternFill patternType="solid"><fgColor rgb="FFFFD6D6"/><bgColor indexed="64"/></patternFill></fill>
		<fill><patternFill patternType="solid"><fgColor rgb="FFFFFFBF"/><bgColor indexed="64"/></patternFill></fill>
	</fills>
	<borders count="1">
		<border><left/><right/><top/><bottom/><diagonal/></border>
	</borders>
	<cellStyleXfs count="1">
		<xf numFmtId="0" fontId="0" fillId="0" borderId="0"/>
	</cellStyleXfs>
	<cellXfs count="3">
		<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>
		<xf numFmtId="0" fontId="0" fillId="2" borderId="0" xfId="0" applyFill="1"/>
		<xf numFmtId="0" fontId="0" fillId="3" borderId="0" xfId="0" applyFill="1"/>
	</cellXfs>
</styleSheet>`)

type workbook struct {
	XMLName xml.Name       `xml:"workbook"`
	Xmlns   string         `xml:"xmlns,attr"`
	XmlnsR  string         `xml:"xmlns:r,attr"`
	Sheets  workbookSheets `xml:"sheets"`
}

type workbookSheets struct {
	Sheet workbookSheet `xml:"sheet"`
}

type workbookSheet struct {
	Name    string `xml:"name,attr"`
	SheetID string `xml:"sheetId,attr"`
	RID     string `xml:"r:id,attr"`
}

// --- Sheet XML generation ---

type worksheet struct {
	XMLName   xml.Name  `xml:"worksheet"`
	Xmlns     string    `xml:"xmlns,attr"`
	SheetData sheetData `xml:"sheetData"`
}

type sheetData struct {
	Rows []row `xml:"row"`
}

type row struct {
	R     int    `xml:"r,attr"`
	Cells []cell `xml:"c"`
}

type cell struct {
	R string `xml:"r,attr"`
	S string `xml:"s,attr,omitempty"`
	T string `xml:"t,attr,omitempty"`
	V string `xml:"v"`
}

func styleForRecord(record utils.Record) string {
	if record.Carved {
		return "1"
	}
	if record.Logged {
		return "2"
	}
	return ""
}

func excelColumnName(col int) string {
	name := ""
	for col > 0 {
		col--
		name = string(rune('A'+(col%26))) + name
		col /= 26
	}
	return name
}

func excelCellRef(row int, col int) string {
	return fmt.Sprintf("%s%d", excelColumnName(col), row)
}

func sanitizeSheetName(name string) string {
	clean := strings.Map(func(r rune) rune {
		switch r {
		case ':', '\\', '/', '?', '*', '[', ']':
			return '_'
		default:
			return r
		}
	}, strings.TrimSpace(name))

	if clean == "" {
		return "Sheet1"
	}

	runes := []rune(clean)
	if len(runes) > 31 {
		clean = string(runes[:31])
	}

	return clean
}

func buildWorkbookXML(sheetName string) ([]byte, error) {
	wb := workbook{
		Xmlns:  "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
		XmlnsR: "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
		Sheets: workbookSheets{Sheet: workbookSheet{Name: sanitizeSheetName(sheetName), SheetID: "1", RID: "rId1"}},
	}

	out, err := xml.Marshal(wb)
	if err != nil {
		return nil, err
	}

	return append([]byte(`<?xml version="1.0" encoding="UTF-8"?>`+"\n"), out...), nil
}

func createMinimalXLSX(zw *zip.Writer, sheetName string) error {

	// 1) [Content_Types].xml
	if err := writeFile(zw, "[Content_Types].xml", contentTypesXML); err != nil {
		return err
	}

	// 2) _rels/.rels
	if err := writeFile(zw, "_rels/.rels", relsXML); err != nil {
		return err
	}

	workbookData, err := buildWorkbookXML(sheetName)
	if err != nil {
		return err
	}

	// 3) xl/workbook.xml
	if err := writeFile(zw, "xl/workbook.xml", workbookData); err != nil {
		return err
	}

	// 4) xl/_rels/workbook.xml.rels
	if err := writeFile(zw, "xl/_rels/workbook.xml.rels", workbookRelsXML); err != nil {
		return err
	}

	// 5) xl/styles.xml
	if err := writeFile(zw, "xl/styles.xml", stylesXML); err != nil {
		return err
	}

	return nil
}

func writeFile(zw *zip.Writer, name string, data []byte) error {
	w, err := zw.Create(name)
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func (xlsxExporter XLSXExporter) WriteRecords(wg *sync.WaitGroup, records <-chan utils.Record, headers []string, sheetName string) {
	defer wg.Done()
	ws := worksheet{
		Xmlns: "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

	rowIndex := 1
	cells := make([]cell, 0, len(headers))
	for colIndex, col := range headers {
		cells = append(cells, cell{
			R: excelCellRef(rowIndex, colIndex+1),
			T: "str",
			V: col,
		})

	}
	ws.SheetData.Rows = append(ws.SheetData.Rows,
		row{R: rowIndex, Cells: cells})

	for record := range records {
		cells := make([]cell, 0, len(record.Vals))
		styleID := styleForRecord(record)

		for colIndex, col := range record.Vals {

			cells = append(cells, cell{
				R: excelCellRef(rowIndex+1, colIndex+1),
				S: styleID,
				T: "str",
				V: col})
		}
		ws.SheetData.Rows = append(ws.SheetData.Rows, row{R: rowIndex + 1, Cells: cells})
		rowIndex++
	}

	out, _ := xml.Marshal(ws)

	// prepend XML header
	sheetData := append([]byte(`<?xml version="1.0" encoding="UTF-8"?>`+"\n"), out...)

	f, err := os.Create(path.Join(xlsxExporter.Path,
		fmt.Sprintf("%s.xlsx", sheetName)))
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	zw := zip.NewWriter(f)
	defer zw.Close()

	err = createMinimalXLSX(zw, sheetName)
	if err != nil {
		log.Fatal(err)
	}
	if err := writeFile(zw, "xl/worksheets/sheet1.xml", sheetData); err != nil {
		log.Fatal(err)
	}

}

func (xlsxExporter XLSXExporter) WriteSchema(columns []tables.Column, sheetName string) {

}

func (xlsxExporter XLSXExporter) WriteIndexRecords(wg *sync.WaitGroup, sheetName string, records <-chan utils.Record, index tables.Index) {

}
