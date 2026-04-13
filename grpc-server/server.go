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
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/aarsakian/MTF_Reader/mtf"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	mssqlparser_comms.UnimplementedFileProcessorServiceServer
	pm            manager.ProcessManager
	mu            sync.Mutex
	ActiveStreams map[string]grpc.BidiStreamingServer[mssqlparser_comms.Message,
		mssqlparser_comms.Message]
}

func (mssqlparser_commsServer *Server) SetConfig(ctx context.Context, config *mssqlparser_comms.Config) (
	*mssqlparser_comms.Message, error) {
	mssqlparser_commsServer.pm = manager.ProcessManager{}

	mssqlparser_commsServer.pm.Initialize(false,
		false, false, false, false, false, false,
		false, false, false, false, false,
		"", false, false, false, 0, -1, []int{}, config.Carve,
		false, false, "", false, []string{},
		"csv", false, config.ExporPath, false, false, "", "", "")

	mssqlparser_commsServer.pm.TableConfiguration = manager.TableProcessorConfiguration{
		SelectedTables:  strings.Split("", ","),
		SelectedType:    "",
		SelectedPages:   utils.StringsToIntArray(""),
		SelectedColumns: strings.Split("", ","),
	}

	return &mssqlparser_comms.Message{Content: "configuration set"}, nil

}

func (mssqlparser_commsServer *Server) UpdateConfig(ctx context.Context, config *mssqlparser_comms.Config) (
	*mssqlparser_comms.Message, error) {

	mssqlparser_commsServer.pm.SetExportPath(config.ExporPath)
	mssqlparser_commsServer.pm.SetShowCarve(config.Carve)

	return &mssqlparser_comms.Message{Content: "configuration set"}, nil

}

func (mssqlparser_commsServer *Server) Process(
	fileDetails *mssqlparser_comms.FileDetails,
	stream mssqlparser_comms.FileProcessorService_ProcessServer) error {

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()
	var err error

	if fileDetails.Carve {
		mslogger.Mslogger.Info("Carving enabled")

	} else {
		mslogger.Mslogger.Info("Carving disabled")
	}

	if err = stream.Send(&mssqlparser_comms.TableResponse{
		MessageType: &mssqlparser_comms.TableResponse_Message{
			Message: &mssqlparser_comms.Message{Content: fmt.Sprintf("Processing %s  LDF: %s Carve %t ", fileDetails.Mdffile,
				fileDetails.Ldffile, fileDetails.Carve)}}}); err != nil {
		return err
	}
	mssqlparser_commsServer.pm.ProcessDBFiles([]string{fileDetails.Mdffile}, []string{fileDetails.Ldffile},
		[]int{}, 0, math.MaxUint32, fileDetails.Carve)

	for guid, database := range mssqlparser_commsServer.pm.Databases {
		srcCH := make(chan *db.Table, 100000)
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
			srcCH, mssqlparser_commsServer.pm.TableConfiguration.SelectedPages)

		go func(wgs *sync.WaitGroup) {
			defer wgs.Done()
			for table := range listener2 {

				database.Tables = append(database.Tables, table)
				mssqlparser_commsServer.pm.Databases[guid] = database
			}
		}(wg)

		go func(wgs *sync.WaitGroup) {
			defer wgs.Done()
			for table := range listener1 {
				msg := fmt.Sprintf("Processing Table %s", table.Name)
				fmt.Printf(msg + " \n")
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

			stream.Send(&mssqlparser_comms.Row{
				Vals:            table.GetHeader(colnames),
				Carved:          false,
				Logged:          false,
				LoggedOperation: "",
			})

			go table.GetRecords(wg, selectedTableRow, colnames, records)

			wg.Add(1)
			go func(wgs *sync.WaitGroup) {
				defer wgs.Done()

				for record := range records {
					fmt.Println("Sending ", record)

					if err = stream.Send(&mssqlparser_comms.Row{
						Vals:            record.Vals,
						Carved:          record.Carved,
						Logged:          record.Logged,
						LoggedOperation: record.LoggedOperation}); err != nil {
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
	fmt.Println("asked DB", askedDB)

	format, ok := mapExportFormat(askedDB.Format)
	if !ok {
		return status.Errorf(codes.InvalidArgument, "unsupported export format: %s", askedDB.Format.String())
	}

	for _, database := range mssqlparser_commsServer.pm.Databases {

		if database.Name != askedDB.Name {
			continue
		}

		mssqlparser_commsServer.pm.Exporter.Format = format
		mssqlparser_commsServer.pm.Exporter.Path = askedDB.ExportPath
		tablesCH := make(chan *db.Table, len(database.Tables))

		for _, table := range database.Tables {
			msg := fmt.Sprintf("Queueing Table %s for %s export to %s", table.Name, strings.ToUpper(format), askedDB.ExportPath)
			fmt.Println(msg)
			if err = stream.Send(&mssqlparser_comms.Message{Content: msg}); err != nil {
				return err
			}
			tablesCH <- table
		}
		close(tablesCH)

		wg := new(sync.WaitGroup)
		wg.Add(1)
		databaseFolder := filepath.Dir(database.Fname)
		sourceFilename := filepath.Base(database.Fname)
		go mssqlparser_commsServer.pm.Exporter.Export(wg, []int{}, mssqlparser_commsServer.pm.TableConfiguration.SelectedColumns,
			database.Name, sourceFilename, databaseFolder, tablesCH)
		wg.Wait()

		msg := fmt.Sprintf("Completed export for database %s in %s format to %s", askedDB.Name,
			strings.ToUpper(format), askedDB.ExportPath)
		if err = stream.Send(&mssqlparser_comms.Message{Content: msg}); err != nil {
			return err
		}

		return nil

	}

	return status.Errorf(codes.NotFound, "database %s not found", askedDB.Name)

}

func mapExportFormat(format mssqlparser_comms.ExportFormat) (string, bool) {
	switch format {
	case mssqlparser_comms.ExportFormat_CSV:
		return "csv", true
	case mssqlparser_comms.ExportFormat_XLSX:
		return "xlsx", true
	case mssqlparser_comms.ExportFormat_HTML:
		return "html", true
	default:
		return "", false
	}

}

func (mssqlparser_commsServer *Server) ExportTable(ctx context.Context, askedTable *mssqlparser_comms.Table) (
	*mssqlparser_comms.Message, error) {

	format, ok := mapExportFormat(askedTable.Format)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "unsupported export format: %s", askedTable.Format.String())
	}

	mssqlparser_commsServer.pm.Exporter.Format = format

	for _, database := range mssqlparser_commsServer.pm.Databases {
		fmt.Println("asked table", askedTable.Name, len(database.Tables))
		for _, table := range database.Tables {

			if table.Name != askedTable.Name {
				continue
			}

			tablesCH := make(chan *db.Table, 1)
			tablesCH <- table
			close(tablesCH)

			wg := new(sync.WaitGroup)
			wg.Add(1)
			databaseFolder := filepath.Dir(database.Fname)
			sourceFilename := filepath.Base(database.Fname)
			go mssqlparser_commsServer.pm.Exporter.Export(wg, []int{}, mssqlparser_commsServer.pm.TableConfiguration.SelectedColumns,
				database.Name, sourceFilename, databaseFolder, tablesCH)
			wg.Wait()

			return &mssqlparser_comms.Message{
				Content: fmt.Sprintf("Exported Table %s in %s format to %s",
					askedTable.Name, strings.ToUpper(format),
					mssqlparser_commsServer.pm.Exporter.Path)}, nil
		}

	}

	return nil, status.Errorf(codes.NotFound, "table %s not found", askedTable.Name)

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

	mssqlparser_commsServer.pm.ProcessDBFiles(mdffiles, []string{}, []int{},
		0, math.MaxUint32, false)

	for guid, database := range mssqlparser_commsServer.pm.Databases {
		srcCH := make(chan *db.Table, 100000)
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
			srcCH, mssqlparser_commsServer.pm.TableConfiguration.SelectedPages)

		go func(wgs *sync.WaitGroup) {
			defer wgs.Done()
			for table := range listener2 {

				database.Tables[table.ObjectId] = table
				mssqlparser_commsServer.pm.Databases[guid] = database
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
