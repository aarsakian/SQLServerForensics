package exporter

import (
	"MSSQLParser/db/tables"
	"MSSQLParser/logger"
	"MSSQLParser/utils"
	"archive/zip"
	"encoding/xml"
	"fmt"
	"log"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
)

type sheetEntry struct {
	name  string
	data  []byte
	order int
}

type XLSXExporter struct {
	Path   string
	zw     *zip.Writer
	f      *os.File
	mu     sync.Mutex
	sheets []sheetEntry
}

var relsXML = []byte(`<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
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

func genContentTypesXML(sheets []sheetEntry) []byte {
	var sb strings.Builder
	sb.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
	sb.WriteString("<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\">\n")
	sb.WriteString("  <Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/>\n")
	sb.WriteString("  <Default Extension=\"xml\" ContentType=\"application/xml\"/>\n")
	sb.WriteString("  <Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/>\n")
	for i := range sheets {
		sb.WriteString(fmt.Sprintf("  <Override PartName=\"/xl/worksheets/sheet%d.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>\n", i+1))
	}
	sb.WriteString("  <Override PartName=\"/xl/styles.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml\"/>\n")
	sb.WriteString("</Types>")
	return []byte(sb.String())
}

func genWorkbookXML(sheets []sheetEntry) []byte {
	var sb strings.Builder
	sb.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
	sb.WriteString("<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">\n")
	sb.WriteString("  <sheets>\n")
	for i, s := range sheets {
		var escapedName strings.Builder
		xml.EscapeText(&escapedName, []byte(s.name)) //nolint:errcheck
		sb.WriteString(fmt.Sprintf("    <sheet name=\"%s\" sheetId=\"%d\" r:id=\"rId%d\"/>\n", escapedName.String(), i+1, i+1))
	}
	sb.WriteString("  </sheets>\n")
	sb.WriteString("</workbook>")
	return []byte(sb.String())
}

func genWorkbookRelsXML(sheets []sheetEntry) []byte {
	var sb strings.Builder
	sb.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
	sb.WriteString("<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">\n")
	for i := range sheets {
		sb.WriteString(fmt.Sprintf("  <Relationship Id=\"rId%d\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" Target=\"worksheets/sheet%d.xml\"/>\n", i+1, i+1))
	}
	sb.WriteString(fmt.Sprintf("  <Relationship Id=\"rId%d\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles\" Target=\"styles.xml\"/>\n", len(sheets)+1))
	sb.WriteString("</Relationships>")
	return []byte(sb.String())
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

func (xlsxExporter *XLSXExporter) addSheet(name string, data []byte, order int) {
	xlsxExporter.mu.Lock()
	defer xlsxExporter.mu.Unlock()
	xlsxExporter.sheets = append(xlsxExporter.sheets, sheetEntry{
		name:  sanitizeSheetName(name),
		data:  data,
		order: order,
	})
}

func writeFile(zw *zip.Writer, name string, data []byte) error {
	w, err := zw.Create(name)
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func (xlsxExporter *XLSXExporter) WriteRecords(wg *sync.WaitGroup, records <-chan utils.Record, headers []string, sheetName string) {
	defer wg.Done()

	msg := fmt.Sprintf("Exporting data to %s.xlsx", path.Join(xlsxExporter.Path, sheetName))
	fmt.Printf("%s\n", msg)
	logger.Mslogger.Info(msg)

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
	ws.SheetData.Rows = append(ws.SheetData.Rows, row{R: rowIndex, Cells: cells})

	for record := range records {
		cells := make([]cell, 0, len(record.Vals))
		styleID := styleForRecord(record)
		for colIndex, col := range record.Vals {
			cells = append(cells, cell{
				R: excelCellRef(rowIndex+1, colIndex+1),
				S: styleID,
				T: "str",
				V: col,
			})
		}
		ws.SheetData.Rows = append(ws.SheetData.Rows, row{R: rowIndex + 1, Cells: cells})
		rowIndex++
	}

	out, _ := xml.Marshal(ws)
	sheetXML := append([]byte(`<?xml version="1.0" encoding="UTF-8"?>`+"\n"), out...)
	xlsxExporter.addSheet(sheetName, sheetXML, 1) // order: 1 for records
}

func (xlsxExporter *XLSXExporter) WriteSchema(columns []tables.Column, sheetName string) {
	ws := worksheet{
		Xmlns: "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

	cells := make([]cell, 0, 9)

	for colidx, headername := range []string{"Column Name", "Data Type",
		"Length", "Nullable", "Ansi Padded",
		"Identity", "Computed", "Persisted", "Collation"} {

		cells = append(cells, cell{
			R: excelCellRef(1, colidx+1),
			T: "str",
			V: headername,
		})
	}
	ws.SheetData.Rows = append(ws.SheetData.Rows, row{R: 1, Cells: cells})

	for colIndex, col := range columns {

		cells = append(cells, cell{
			R: excelCellRef(colIndex+2, 1),
			T: "str",
			V: col.Name,
		})
		cells = append(cells, cell{
			R: excelCellRef(colIndex+2, 2),
			T: "str",
			V: col.Type,
		})
		cells = append(cells, cell{
			R: excelCellRef(colIndex+2, 3),
			T: "str",
			V: fmt.Sprintf("%d", col.Size),
		})
		cells = append(cells, cell{
			R: excelCellRef(colIndex+2, 4),
			T: "str",
			V: fmt.Sprintf("%t", col.IslNullable),
		})
		cells = append(cells, cell{
			R: excelCellRef(colIndex+2, 5),
			T: "str",

			V: fmt.Sprintf("%t", col.IsAnsiPadded),
		})
		cells = append(cells, cell{
			R: excelCellRef(colIndex+2, 6),
			T: "str",
			V: fmt.Sprintf("%t", col.IsIdentity),
		})
		cells = append(cells, cell{
			R: excelCellRef(colIndex+2, 7),
			T: "str",
			V: fmt.Sprintf("%t", col.IsComputed),
		})
		cells = append(cells, cell{
			R: excelCellRef(colIndex+2, 8),
			T: "str",
			V: fmt.Sprintf("%t", col.IsPersisted),
		})
		cells = append(cells, cell{
			R: excelCellRef(colIndex+2, 9),
			T: "str",
			V: fmt.Sprintf("%d", col.CollationId),
		})

		ws.SheetData.Rows = append(ws.SheetData.Rows, row{R: colIndex + 2, Cells: cells})
		cells = nil
	}

	out, _ := xml.Marshal(ws)
	sheetXML := append([]byte(`<?xml version="1.0" encoding="UTF-8"?>`+"\n"), out...)
	xlsxExporter.addSheet(sheetName+" schema", sheetXML, 2) // order: 2 for schema
}

func (xlsxExporter *XLSXExporter) WriteIndexRecords(wg *sync.WaitGroup, sheetName string,
	records <-chan utils.Record, index tables.Index) {
	defer wg.Done()

	ws := worksheet{
		Xmlns: "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

	headerCells := make([]cell, 0, len(index.Columns))
	for colIndex, col := range index.Columns {
		headerCells = append(headerCells, cell{
			R: excelCellRef(1, colIndex+1),
			T: "str",
			V: col.Name,
		})
	}
	ws.SheetData.Rows = append(ws.SheetData.Rows, row{R: 1, Cells: headerCells})

	rowIndex := 1
	for record := range records {
		styleID := styleForRecord(record)
		recCells := make([]cell, 0, len(record.Vals))
		for colIndex, col := range record.Vals {
			recCells = append(recCells, cell{
				R: excelCellRef(rowIndex+1, colIndex+1),
				S: styleID,
				T: "str",
				V: col,
			})
		}
		ws.SheetData.Rows = append(ws.SheetData.Rows, row{R: rowIndex + 1, Cells: recCells})
		rowIndex++
	}

	out, _ := xml.Marshal(ws)
	sheetXML := append([]byte(`<?xml version="1.0" encoding="UTF-8"?>`+"\n"), out...)
	xlsxExporter.addSheet(sheetName+"_index_"+index.Name, sheetXML, 3) // order: 3 for indexes
}

func (xlsxExporter *XLSXExporter) InitializeXlsxTemplates(sheetName string) {
	f, err := os.Create(path.Join(xlsxExporter.Path,
		fmt.Sprintf("%s.xlsx", sheetName)))
	if err != nil {
		log.Fatal(err)
	}
	xlsxExporter.f = f
	xlsxExporter.zw = zip.NewWriter(f)
}

func (xlsxExporter *XLSXExporter) Close() error {
	sheets := xlsxExporter.sheets

	// Sort sheets by order (1=records, 2=schema, 3=indexes)
	sort.Slice(sheets, func(i, j int) bool {
		return sheets[i].order < sheets[j].order
	})

	// _rels/.rels
	if err := writeFile(xlsxExporter.zw, "_rels/.rels", relsXML); err != nil {
		return err
	}
	// xl/styles.xml
	if err := writeFile(xlsxExporter.zw, "xl/styles.xml", stylesXML); err != nil {
		return err
	}
	// one xl/worksheets/sheetN.xml per collected sheet
	for i, s := range sheets {
		if err := writeFile(xlsxExporter.zw, fmt.Sprintf("xl/worksheets/sheet%d.xml", i+1), s.data); err != nil {
			return err
		}
	}
	// [Content_Types].xml — lists all sheets
	if err := writeFile(xlsxExporter.zw, "[Content_Types].xml", genContentTypesXML(sheets)); err != nil {
		return err
	}
	// xl/workbook.xml — one <sheet> element per sheet
	if err := writeFile(xlsxExporter.zw, "xl/workbook.xml", genWorkbookXML(sheets)); err != nil {
		return err
	}
	// xl/_rels/workbook.xml.rels — worksheet + styles relationships
	if err := writeFile(xlsxExporter.zw, "xl/_rels/workbook.xml.rels", genWorkbookRelsXML(sheets)); err != nil {
		return err
	}

	if xlsxExporter.zw != nil {
		if err := xlsxExporter.zw.Close(); err != nil {
			return err
		}
		xlsxExporter.zw = nil
	}
	if xlsxExporter.f != nil {
		if err := xlsxExporter.f.Close(); err != nil {
			return err
		}
		xlsxExporter.f = nil
	}
	return nil
}
