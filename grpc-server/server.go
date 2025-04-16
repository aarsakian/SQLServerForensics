package msegrpc

//protoc -I .\comms\ --go_out comms --go_opt paths=source_relative --go-grpc_out comms --go-grpc_opt paths=source_relative   .\comms\comms.proto
// grpcurl.exe -plaintext -d '{\"mtf_file\":\"C:\\Users\\arsak\\Dev\\Shared-mssql\\data\\AdventureWorks2022.bak\"}' 127.0.0.1:50001 mssqlparser_comms.FileProcessor/ProcessMTF
import (
	"MSSQLParser/channels"
	mssqlparser_comms "MSSQLParser/comms"
	"MSSQLParser/db"
	mslogger "MSSQLParser/logger"
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
	pm manager.ProcessManager
}

func (mssqlparser_commsServer *Server) Process(
	fileDetails *mssqlparser_comms.FileDetails,
	stream grpc.ServerStreamingServer[mssqlparser_comms.Table]) error {

	mssqlparser_commsServer.pm = manager.ProcessManager{}

	mssqlparser_commsServer.pm.TableConfiguration = manager.TableProcessorConfiguration{
		SelectedTables:  strings.Split("", ","),
		SelectedType:    "",
		SelectedPages:   utils.StringsToIntArray(""),
		SelectedColumns: strings.Split("", ","),
	}

	mssqlparser_commsServer.pm.ProcessDBFiles([]string{fileDetails.Mdffile}, []string{fileDetails.Ldffile},
		-1, 0, math.MaxUint32, 0, false)

	var err error

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for dbidx, database := range mssqlparser_commsServer.pm.Databases {
		srcCH := make(chan db.Table, 100000)
		broadcaster := channels.NewBroadcastServer(ctx, srcCH)

		listener1 := broadcaster.Subscribe()
		listener2 := broadcaster.Subscribe()

		msg := fmt.Sprintf("table contents of %s ", database.Name)

		mslogger.Mslogger.Info(msg)
		wg := new(sync.WaitGroup)
		wg.Add(2)

		go database.ProcessTables(ctx, mssqlparser_commsServer.pm.TableConfiguration.SelectedTables,
			mssqlparser_commsServer.pm.TableConfiguration.SelectedType,
			srcCH, mssqlparser_commsServer.pm.TableConfiguration.SelectedPages, 0)

		go func(wgs *sync.WaitGroup) {
			defer wgs.Done()
			for table := range listener2 {

				mssqlparser_commsServer.pm.Databases[dbidx].Tables = append(
					mssqlparser_commsServer.pm.Databases[dbidx].Tables, table)
			}
		}(wg)

		go func(wgs *sync.WaitGroup) {
			defer wgs.Done()
			for table := range listener1 {

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

		wg.Wait()
	}

	return err

}

func (mssqlparser_commsServer Server) GetTableContents(askedTable *mssqlparser_comms.Table,
	stream grpc.ServerStreamingServer[mssqlparser_comms.Row]) error {

	wg := new(sync.WaitGroup)
	var err error

	for _, database := range mssqlparser_commsServer.pm.Databases {
		fmt.Println("asked table", askedTable.Name, len(database.Tables))
		for _, table := range database.Tables {

			if table.Name != askedTable.Name {
				continue
			}
			fmt.Println("found", database.Name, table.Name)
			wg.Add(1)
			records := make(chan utils.Record, 1000)
			selectedTableRow := []int{}
			colnames := []string{}
			go table.GetRecords(wg, selectedTableRow, colnames, records)

			wg.Add(1)
			go func(wgs *sync.WaitGroup) {
				defer wgs.Done()
				for record := range records {

					if err = stream.Send(&mssqlparser_comms.Row{Vals: record}); err != nil {
						break
					}
				}
			}(wg)
			wg.Wait()

		}
	}
	return err
}
func (mssqlparser_commsServer *Server) ProcessMTF(_ context.Context,
	mtfDetails *mssqlparser_comms.MTFDetails) (*mssqlparser_comms.Tables, error) {

	var tables []*mssqlparser_comms.Table
	mssqlparser_commsServer.pm = manager.ProcessManager{}

	var mdffiles []string
	mtf_s := mtf.MTF{Fname: mtfDetails.MtfFile}
	mtf_s.Process()
	mtf_s.Export("MDF")
	mdffiles = append(mdffiles, filepath.Join("MDF", mtf_s.GetExportFileName()))

	mssqlparser_commsServer.pm.ProcessDBFiles(mdffiles, []string{},
		-1, 0, math.MaxUint32, 0, false)
	dbnames := mssqlparser_commsServer.pm.GetDatabaseNames()
	for _, dbname := range dbnames {
		tables = append(tables, &mssqlparser_comms.Table{Name: dbname})
	}
	return &mssqlparser_comms.Tables{Tables: tables}, nil
}
