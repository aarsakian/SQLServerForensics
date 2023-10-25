package servicer

import (
	"fmt"
	"unsafe"

	"golang.org/x/sys/windows"
)

type SERVICE_STATUS_PROCESS struct {
	dwServiceType             int32
	dwCurrentState            int32
	dwControlsAccepted        int32
	dwWin32ExitCode           int32
	dwServiceSpecificExitCode int32
	dwCheckPoint              int32
	dwWaitHint                int32
	dwProcessId               int32
	dwServiceFlags            int32
}

func StopService() {
	serviceNamePtr, err := windows.UTF16PtrFromString("MSSQL$SQLEXPRESS")
	if err != nil {
		fmt.Printf("Error converting string %s", err)
	}

	ssStatus := SERVICE_STATUS_PROCESS{}
	var bytesNeeded uint32

	buffSize := uint32(unsafe.Sizeof(ssStatus))

	schSCManager, err := windows.OpenSCManager(nil, nil, windows.SC_MANAGER_ALL_ACCESS)
	if err != nil {
		fmt.Printf("OpenSCManager failed (%d)\n", windows.GetLastError())
		return
	}

	// Get a handle to the service.

	schService, err := windows.OpenService(
		schSCManager,   // SCM database
		serviceNamePtr, // name of service
		windows.SERVICE_STOP|
			windows.SERVICE_QUERY_STATUS|
			windows.SERVICE_ENUMERATE_DEPENDENTS)

	if err != nil {
		fmt.Printf("OpenService failed (%d)\n", windows.GetLastError())
		windows.CloseServiceHandle(schSCManager)
		return
	}

	// Make sure the service is not already stopped.

	if windows.QueryServiceStatusEx(
		schService,
		windows.SC_STATUS_PROCESS_INFO,
		(*byte)(unsafe.Pointer(&ssStatus)),
		buffSize,
		&bytesNeeded) != nil {
		fmt.Printf("QueryServiceStatusEx failed %s\n", windows.GetLastError())
		goto stop_cleanup
	}

stop_cleanup:
	windows.CloseServiceHandle(schService)
	windows.CloseServiceHandle(schSCManager)

}
