package msegrpc

import (
	"MSSQLParser/comms"
	"context"
	"fmt"
)

type CommsServer struct {
	comms.UnimplementedFileProcessorServer
}

func (commsServer CommsServer) Process(_ context.Context,
	fileDetails *comms.FileDetails) (*comms.TablesMSG, error) {
	var tables []*comms.Table

	tables = append(tables, &comms.Table{Name: "sda"})
	fmt.Printf("SDSs")
	return &comms.TablesMSG{Tables: tables}, nil
}
