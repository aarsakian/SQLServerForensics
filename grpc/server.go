package msegrpc

// .\grpcurl.exe -plaintext -d '{\"mtf_file\":\"C:\\Users\\arsak\\Dev\\Shared-mssql\\data\\AdventureWorks2022.bak\"}' 127.0.0.1:50001 comms.FileProcessor/ProcessMTF
import (
	"MSSQLParser/comms"
	"MSSQLParser/manager"
	"context"
	"math"
	"path/filepath"

	"github.com/aarsakian/MTF_Reader/mtf"
)

type CommsServer struct {
	comms.UnimplementedFileProcessorServer
}

func (commsServer CommsServer) Process(_ context.Context,
	fileDetails *comms.FileDetails) (*comms.TablesMSG, error) {
	var tables []*comms.Table

	pm := manager.ProcessManager{}

	pm.ProcessDBFiles([]string{fileDetails.MdfFile}, []string{fileDetails.LdfFile},
		-1, 0, math.MaxUint32, 0, false)
	dbnames := pm.GetDatabaseNames()
	for _, dbname := range dbnames {
		tables = append(tables, &comms.Table{Name: dbname})
	}
	return &comms.TablesMSG{Tables: tables}, nil
}

func (commsServer CommsServer) ProcessMTF(_ context.Context,
	mtfDetails *comms.MTFDetails) (*comms.TablesMSG, error) {

	var tables []*comms.Table
	pm := manager.ProcessManager{}

	var mdffiles []string
	mtf_s := mtf.MTF{Fname: mtfDetails.MtfFile}
	mtf_s.Process()
	mtf_s.Export("MDF")
	mdffiles = append(mdffiles, filepath.Join("MDF", mtf_s.GetExportFileName()))

	pm.ProcessDBFiles(mdffiles, []string{},
		-1, 0, math.MaxUint32, 0, false)
	dbnames := pm.GetDatabaseNames()
	for _, dbname := range dbnames {
		tables = append(tables, &comms.Table{Name: dbname})
	}
	return &comms.TablesMSG{Tables: tables}, nil
}
