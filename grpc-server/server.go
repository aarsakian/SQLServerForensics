package msegrpc

//protoc -I .\comms\ --go_out comms --go_opt paths=source_relative --go-grpc_out comms --go-grpc_opt paths=source_relative   .\comms\comms.proto
// grpcurl.exe -plaintext -d '{\"mtf_file\":\"C:\\Users\\arsak\\Dev\\Shared-mssql\\data\\AdventureWorks2022.bak\"}' 127.0.0.1:50001 mssqlparser_comms.FileProcessor/ProcessMTF
import (
	mssqlparser_comms "MSSQLParser/comms"
	"MSSQLParser/db"
	"MSSQLParser/manager"
	"MSSQLParser/utils"
	"context"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aarsakian/MTF_Reader/mtf"
	"google.golang.org/grpc"
)

type Server struct {
	mssqlparser_comms.UnimplementedFileProcessorServer
}

func (mssqlparser_commsServer Server) Process(
	fileDetails *mssqlparser_comms.FileDetails,
	stream grpc.ServerStreamingServer[mssqlparser_comms.Table]) error {

	pm := manager.ProcessManager{}

	pm.TableConfiguration = manager.TableProcessorConfiguration{
		SelectedTables:  strings.Split("", ","),
		SelectedType:    "",
		SelectedPages:   utils.StringsToIntArray(""),
		SelectedColumns: strings.Split("", ","),
	}

	pm.ProcessDBFiles([]string{fileDetails.Mdffile}, []string{fileDetails.Ldffile},
		-1, 0, math.MaxUint32, 0, false)

	wg := new(sync.WaitGroup)
	represults := make(map[string]chan db.Table) //max number of tables for report
	expresults := make(map[string]chan db.Table)

	wg.Add(2)

	pm.ProcessDBTables(wg, represults, expresults, 0)
	var err error
	for _, database := range pm.Databases {
		wg.Add(1)
		go func(wgs *sync.WaitGroup) {
			defer wgs.Done()

			fmt.Println(database.Name)
			for table := range expresults[database.Name] {

				tableSer := mssqlparser_comms.Table{Name: table.Name, Type: table.Type}

				for _, col := range table.Schema {
					tableSer.Cols = append(tableSer.Cols,
						&mssqlparser_comms.Col{Name: col.Name, Type: col.Type})

				}
				if err = stream.Send(&tableSer); err != nil {
					break
				}
			}

		}(wg)
	}

	pm.ShowDBs(wg, represults)

	wg.Wait()

	return err

}

func (mssqlparser_commsServer Server) ProcessMTF(_ context.Context,
	mtfDetails *mssqlparser_comms.MTFDetails) (*mssqlparser_comms.TablesMSG, error) {

	var tables []*mssqlparser_comms.Table
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
		tables = append(tables, &mssqlparser_comms.Table{Name: dbname})
	}
	return &mssqlparser_comms.TablesMSG{Tables: tables}, nil
}
