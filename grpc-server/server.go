package msegrpc

//protoc -I .\comms\ --go_out comms --go_opt paths=source_relative --go-grpc_out comms --go-grpc_opt paths=source_relative   .\comms\comms.proto
// grpcurl.exe -plaintext -d '{\"mtf_file\":\"C:\\Users\\arsak\\Dev\\Shared-mssql\\data\\AdventureWorks2022.bak\"}' 127.0.0.1:50001 mssqlparser_comms.FileProcessor/ProcessMTF
import (
	"MSSQLParser/channels"
	mssqlparser_comms "MSSQLParser/comms"
	"MSSQLParser/db"
	"MSSQLParser/exporter"
	mslogger "MSSQLParser/logger"
	"MSSQLParser/manager"
	"MSSQLParser/utils"
	"context"
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/aarsakian/MTF_Reader/mtf"

	"google.golang.org/grpc"
)

type Server struct {
	mssqlparser_comms.UnimplementedFileProcessorServiceServer
	pm            manager.ProcessManager
	mu            sync.Mutex
	ActiveStreams map[string]grpc.BidiStreamingServer[mssqlparser_comms.Message,
		mssqlparser_comms.Message]
}

func (mssqlparser_commsServer *Server) MessageStream(instream mssqlparser_comms.FileProcessorService_MessageStreamServer) error {

	mssqlparser_commsServer.pm = manager.ProcessManager{}

	mssqlparser_commsServer.pm.TableConfiguration = manager.TableProcessorConfiguration{
		SelectedTables:  strings.Split("", ","),
		SelectedType:    "",
		SelectedPages:   utils.StringsToIntArray(""),
		SelectedColumns: strings.Split("", ","),
	}

	return nil

}

func (mssqlparser_commsServer *Server) Process(
	fileDetails *mssqlparser_comms.FileDetails,
	stream mssqlparser_comms.FileProcessorService_ProcessServer) error {

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()
	var err error
	if err = stream.Send(&mssqlparser_comms.TableResponse{
		MessageType: &mssqlparser_comms.TableResponse_Message{
			Message: &mssqlparser_comms.Message{Content: "Processing database"}}}); err != nil {
		return err
	}
	mssqlparser_commsServer.pm.ProcessDBFiles([]string{fileDetails.Mdffile}, []string{fileDetails.Ldffile},
		-1, 0, math.MaxUint32, 0, false)

	for dbidx, database := range mssqlparser_commsServer.pm.Databases {
		srcCH := make(chan db.Table, 100000)
		broadcaster := channels.NewBroadcastServer(ctx, srcCH)

		listener1 := broadcaster.Subscribe()
		listener2 := broadcaster.Subscribe()

		msg := fmt.Sprintf("table contents of %s ", database.Name)

		mslogger.Mslogger.Info(msg)

		if err = stream.Send(&mssqlparser_comms.TableResponse{
			MessageType: &mssqlparser_comms.TableResponse_Message{
				Message: &mssqlparser_comms.Message{Content: msg}}}); err != nil {
			return err
		}

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
				msg := fmt.Sprintf("Processing Table %s", table.Name)
				if err = stream.Send(&mssqlparser_comms.TableResponse{
					MessageType: &mssqlparser_comms.TableResponse_Message{
						Message: &mssqlparser_comms.Message{Content: msg}}}); err != nil {
					break
				}

				tableSer := mssqlparser_comms.Table{Name: table.Name, Type: table.Type, NofRows: uint32(len(table.Rows))}

				for _, col := range table.Schema {
					tableSer.Cols = append(tableSer.Cols,
						&mssqlparser_comms.Col{Name: col.Name, Type: col.Type})

				}
				if err = stream.Send(&mssqlparser_comms.TableResponse{
					MessageType: &mssqlparser_comms.TableResponse_Table{
						Table: &tableSer}}); err != nil {
					break
				}
			}

		}(wg)

		wg.Wait()
		if err = stream.Send(&mssqlparser_comms.TableResponse{
			MessageType: &mssqlparser_comms.TableResponse_Message{
				Message: &mssqlparser_comms.Message{Content: "Completed!"}}}); err != nil {
			break
		}
	}

	return err

}

func (mssqlparser_commsServer *Server) GetTableContents(askedTable *mssqlparser_comms.Table,
	stream mssqlparser_comms.FileProcessorService_GetTableContentsServer) error {

	wg := new(sync.WaitGroup)
	var err error

	for _, database := range mssqlparser_commsServer.pm.Databases {
		fmt.Println("asked table", askedTable.Name, len(database.Tables))
		for _, table := range database.Tables {

			if table.Name != askedTable.Name {
				continue
			}

			wg.Add(1)
			records := make(chan utils.Record, 1000)
			selectedTableRow := []int{}
			colnames := []string{}
			go table.GetRecords(wg, selectedTableRow, colnames, records)

			wg.Add(1)
			go func(wgs *sync.WaitGroup) {
				defer wgs.Done()

				for record := range records {
					fmt.Println("Sending ", record)
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

func (mssqlparser_commsServer *Server) ExportDatabase(askedDB *mssqlparser_comms.Database,
	stream mssqlparser_comms.FileProcessorService_ExportDatabaseServer) error {

	var err error
	wg := new(sync.WaitGroup)
	fmt.Println("asked DB", askedDB)
	for i, r := range askedDB.Name {
		fmt.Printf("client[%d]: '%c' (U+%04X)\n", i, r, r)
	}

	for _, database := range mssqlparser_commsServer.pm.Databases {

		if database.Name != askedDB.Name {
			continue
		}
		for _, table := range database.Tables {

			wg.Add(1)
			records := make(chan utils.Record, 1000)
			selectedTableRow := []int{}
			colnames := []string{}
			go table.GetRecords(wg, selectedTableRow, colnames, records)

			wg.Add(1)
			msg := fmt.Sprintf("Exporting Table %s", table.Name)
			fmt.Println(msg)
			if err = stream.Send(&mssqlparser_comms.Message{Content: msg}); err != nil {
				break
			}
			go exporter.WriteCSV(wg, records, table.Name, "")
			wg.Wait()
		}

	}

	return err

}

func (mssqlparser_commsServer *Server) ExportTable(ctx context.Context, askedTable *mssqlparser_comms.Table) (
	*mssqlparser_comms.Message, error) {

	var err error
	wg := new(sync.WaitGroup)

	for _, database := range mssqlparser_commsServer.pm.Databases {
		fmt.Println("asked table", askedTable.Name, len(database.Tables))
		for _, table := range database.Tables {

			if table.Name != askedTable.Name {
				continue
			}

			wg.Add(1)
			records := make(chan utils.Record, 1000)
			selectedTableRow := []int{}
			colnames := []string{}
			go table.GetRecords(wg, selectedTableRow, colnames, records)

			wg.Add(1)
			//if err = stream.Send(&mssqlparser_comms.Message{Content: "exporting"}); err != nil {
			//	break
			//}
			go exporter.WriteCSV(wg, records, table.Name, "")
			wg.Wait()
		}

	}

	return &mssqlparser_comms.Message{Content: fmt.Sprintf("Exported Table %s", askedTable.Name)}, err

}

func (mssqlparser_commsServer *Server) GetTableAllocationInfo(askedTable *mssqlparser_comms.Table,
	stream mssqlparser_comms.FileProcessorService_GetTableAllocationInfoServer) error {

	var err error

	for _, database := range mssqlparser_commsServer.pm.Databases {
		fmt.Println("asked table", askedTable.Name, len(database.Tables))
		for _, table := range database.Tables {

			if table.Name != askedTable.Name {
				continue
			}

			for pageType, pagesType := range table.PageIDsPerType {
				slices.Sort(pagesType)
				for _, pageID := range pagesType {
					fmt.Println("Sending ", pageType)
					if err = stream.Send(&mssqlparser_comms.Page{ID: pageID, Type: pageType}); err != nil {
						break
					}
				}

			}

		}
	}
	return err

}

func (mssqlparser_commsServer *Server) ProcessBak(bakfile *mssqlparser_comms.MTF,
	stream mssqlparser_comms.FileProcessorService_ProcessServer) error {

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	var err error
	var mdffiles []string

	mtf_s := mtf.MTF{Fname: bakfile.MtfFile}

	if err = stream.Send(&mssqlparser_comms.TableResponse{
		MessageType: &mssqlparser_comms.TableResponse_Message{
			Message: &mssqlparser_comms.Message{Content: "Processing Bak file"}}}); err != nil {
		return err
	}

	mtf_s.Process()
	mtf_s.Export("MDF")
	mdffiles = append(mdffiles, filepath.Join("MDF", mtf_s.GetExportFileName()))

	mssqlparser_commsServer.pm.ProcessDBFiles(mdffiles, []string{}, -1, 0, math.MaxUint32, 0, false)

	for dbidx, database := range mssqlparser_commsServer.pm.Databases {
		srcCH := make(chan db.Table, 100000)
		broadcaster := channels.NewBroadcastServer(ctx, srcCH)

		listener1 := broadcaster.Subscribe()
		listener2 := broadcaster.Subscribe()

		msg := fmt.Sprintf("table contents of %s ", database.Name)

		mslogger.Mslogger.Info(msg)

		if err = stream.Send(&mssqlparser_comms.TableResponse{
			MessageType: &mssqlparser_comms.TableResponse_Message{
				Message: &mssqlparser_comms.Message{Content: msg}}}); err != nil {
			return err
		}

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
				msg := fmt.Sprintf("Processing Table %s", table.Name)
				if err = stream.Send(&mssqlparser_comms.TableResponse{
					MessageType: &mssqlparser_comms.TableResponse_Message{
						Message: &mssqlparser_comms.Message{Content: msg}}}); err != nil {
					break
				}

				tableSer := mssqlparser_comms.Table{Name: table.Name, Type: table.Type, NofRows: uint32(len(table.Rows))}

				for _, col := range table.Schema {
					tableSer.Cols = append(tableSer.Cols,
						&mssqlparser_comms.Col{Name: col.Name, Type: col.Type})

				}
				if err = stream.Send(&mssqlparser_comms.TableResponse{
					MessageType: &mssqlparser_comms.TableResponse_Table{
						Table: &tableSer}}); err != nil {
					break
				}
			}

		}(wg)

		wg.Wait()
		if err = stream.Send(&mssqlparser_comms.TableResponse{
			MessageType: &mssqlparser_comms.TableResponse_Message{
				Message: &mssqlparser_comms.Message{Content: "Completed!"}}}); err != nil {
			break
		}
	}

	return err

}
