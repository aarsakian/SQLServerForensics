package servicer

import (
	"fmt"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

func StartService() {
	var startTime time.Time
	var oldCheckPoint uint32
	serviceNamePtr, err := windows.UTF16PtrFromString("MSSQL$SQLEXPRESS")
	if err != nil {
		fmt.Printf("Error converting string %s", err)
	}

	ssStatus := windows.SERVICE_STATUS_PROCESS{}
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
		windows.SERVICE_ALL_ACCESS)

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

	if ssStatus.CurrentState == windows.SERVICE_START || ssStatus.CurrentState == windows.SERVICE_START_PENDING {
		fmt.Printf("Cannot stop the service because it is already running")
		goto stop_cleanup
	}

	startTime = time.Now()
	oldCheckPoint = ssStatus.CheckPoint
	for ssStatus.CurrentState == windows.SERVICE_STOP_PENDING {
		waitTime := ssStatus.WaitHint
		if waitTime < 1000 {
			waitTime = 10000
		} else if waitTime > 1000 {
			waitTime = 10000
		}

		time.Sleep(time.Duration(waitTime))

		if windows.QueryServiceStatusEx(
			schService,
			windows.SC_STATUS_PROCESS_INFO,
			(*byte)(unsafe.Pointer(&ssStatus)),
			buffSize,
			&bytesNeeded) != nil {
			fmt.Printf("QueryServiceStatusEx failed %s\n", windows.GetLastError())
			goto stop_cleanup
		}

		if ssStatus.CheckPoint > oldCheckPoint {
			startTime = time.Now()
			oldCheckPoint = ssStatus.CheckPoint
		} else {
			if time.Since(startTime) > time.Duration(ssStatus.WaitHint) {

				fmt.Printf("Timeout waiting for service to stop\n")
				goto stop_cleanup
				return
			}
		}

	}

stop_cleanup:
	windows.CloseServiceHandle(schService)
	windows.CloseServiceHandle(schSCManager)
}

func StopService() {
	dwTimeout := time.Duration(30 * time.Millisecond)
	var dwWaitTime uint32
	startTime := time.Now()

	serviceNamePtr, err := windows.UTF16PtrFromString("MSSQL$SQLEXPRESS")
	if err != nil {
		fmt.Printf("Error converting string %s", err)
	}

	ssStatus := windows.SERVICE_STATUS_PROCESS{}
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

	if ssStatus.CurrentState == windows.SERVICE_STOP {
		fmt.Printf("Cannot stop the service because it is already stopped")
		goto stop_cleanup
	}

	for ssStatus.CurrentState == windows.SERVICE_STOP_PENDING {

		fmt.Printf("Service stop pending...\n")

		// Do not wait longer than the wait hint. A good interval is
		// one-tenth of the wait hint but not less than 1 second
		// and not more than 10 seconds.

		dwWaitTime = ssStatus.WaitHint / 10

		if dwWaitTime < 1000 {
			dwWaitTime = 1000
		} else if dwWaitTime > 10000 {
			dwWaitTime = 10000
		}

		time.Sleep(time.Duration(dwWaitTime) * time.Millisecond)

		if windows.QueryServiceStatusEx(
			schService,
			windows.SC_STATUS_PROCESS_INFO,
			(*byte)(unsafe.Pointer(&ssStatus)),
			buffSize,
			&bytesNeeded) != nil {
			fmt.Printf("QueryServiceStatusEx failed (%d)\n", windows.GetLastError())
			goto stop_cleanup
		}

		if ssStatus.CurrentState == windows.SERVICE_STOPPED {
			fmt.Printf("Service stopped successfully.\n")
			goto stop_cleanup
		}

		if time.Since(startTime) > dwTimeout {

			fmt.Printf("Service stop timed out.\n")
			goto stop_cleanup
		}
	}

	stopDependentServices()

	// Send a stop code to the service.

	if windows.ControlService(
		schService,
		windows.SERVICE_CONTROL_STOP,
		(*windows.SERVICE_STATUS)(unsafe.Pointer(&ssStatus))) != nil {
		fmt.Printf("ControlService failed (%d)\n", windows.GetLastError())
		goto stop_cleanup
	}

	// Wait for the service to stop.

	for ssStatus.CurrentState != windows.SERVICE_STOPPED {
		time.Sleep(time.Duration(ssStatus.WaitHint))
		if windows.QueryServiceStatusEx(
			schService,
			windows.SC_STATUS_PROCESS_INFO,
			(*byte)(unsafe.Pointer(&ssStatus)),
			buffSize,
			&bytesNeeded) != nil {

			fmt.Printf("QueryServiceStatusEx failed (%d)\n", windows.GetLastError())
			goto stop_cleanup
		}

		if ssStatus.CurrentState == windows.SERVICE_STOPPED {
			fmt.Printf("Service stopped!\n")
			break

		}

		if time.Since(startTime) > dwTimeout {

			fmt.Printf("Wait timed out\n")
			goto stop_cleanup
		}

		fmt.Printf("Service stopped successfully\n")
	}

stop_cleanup:
	windows.CloseServiceHandle(schService)
	windows.CloseServiceHandle(schSCManager)

}

func stopDependentServices() bool {

	var dwBytesNeeded uint32
	var nofServicesP uint32

	var lpDependencies *windows.ENUM_SERVICE_STATUS = new(windows.ENUM_SERVICE_STATUS) //allocate memory
	var ess windows.ENUM_SERVICE_STATUS
	var hDepService windows.Handle
	var schService windows.Handle
	var ssp windows.SERVICE_STATUS

	schSCManager, err := windows.OpenSCManager(
		nil,                           // local computer
		nil,                           // ServicesActive database
		windows.SC_MANAGER_ALL_ACCESS) // full access rights

	if err != nil {

		fmt.Printf("OpenSCManager failed (%d)\n", windows.GetLastError())
		return false
	}

	dwStartTime := time.Now()
	dwTimeout := 30000 // 30-second time-out

	// Pass a zero-length buffer to get the required buffer size. dwBytesNeeded,
	if windows.EnumDependentServices(schService, windows.SERVICE_ACTIVE,
		lpDependencies, 0, &dwBytesNeeded, &nofServicesP) != nil {

		// If the Enum call succeeds, then there are no dependent
		// services, so do nothing.
		return true
	} else {
		//defer HeapFree(GetProcessHeap(), 0, lpDependencies)
		if windows.GetLastError() != windows.ERROR_MORE_DATA {
			return false // Unexpected error
		}
		// Allocate a buffer for the dependencies.
		buf := make([]byte, dwBytesNeeded)
		lpDependencies = (*windows.ENUM_SERVICE_STATUS)(unsafe.Pointer(&buf))
		if lpDependencies != nil {
			return false
		}

		// Enumerate the dependencies.
		if windows.EnumDependentServices(schService, windows.SERVICE_ACTIVE,
			lpDependencies, dwBytesNeeded, &dwBytesNeeded,
			&nofServicesP) != nil {
			return false
		}
		sizeOflpDependencies := unsafe.Sizeof(lpDependencies) //uintpntr
		for i := uintptr(0); i < uintptr(nofServicesP); i++ {
			baseP := uintptr(unsafe.Pointer(&lpDependencies))
			ess = *(*windows.ENUM_SERVICE_STATUS)(unsafe.Pointer(baseP + sizeOflpDependencies*i))
			// Open the service.
			hDepService, err = windows.OpenService(schSCManager,
				ess.ServiceName,
				windows.SERVICE_STOP|windows.SERVICE_QUERY_STATUS)
			defer windows.CloseServiceHandle(hDepService)
			if err != nil {
				return false
			}

			// Send a stop code.
			if windows.ControlService(hDepService,
				windows.SERVICE_CONTROL_STOP,
				&ssp) != nil {
				return false
			}
			buffSize := uint32(unsafe.Sizeof(ssp))

			// Wait for the service to stop.
			for ssp.CurrentState != windows.SERVICE_STOPPED {

				time.Sleep(time.Duration(ssp.WaitHint))
				if windows.QueryServiceStatusEx(
					hDepService,
					windows.SC_STATUS_PROCESS_INFO,
					(*byte)(unsafe.Pointer(&ssp)),
					buffSize,
					&dwBytesNeeded) != nil {
					return false
				}

				if ssp.CurrentState == windows.SERVICE_STOPPED {
					break
				}

				if time.Since(dwStartTime) > time.Duration(dwTimeout) {
					return false
				}

			}
		}

		// Always release the service handle.
		return true
	}

	// Always free the enumeration buffer.

}
