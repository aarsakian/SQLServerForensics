package logger

import (
	"log"
	"os"
)

type MSLogger struct {
	info    *log.Logger
	warning *log.Logger
	error_  *log.Logger
	active  bool
}

var Mslogger MSLogger

func InitializeLogger(active bool, logfilename string) {
	if active {

		file, err := os.OpenFile(logfilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			log.Fatal(err)
		}

		info := log.New(file, "MSSQLParser|INFO: ", log.Ldate|log.Ltime)
		warning := log.New(file, "MSSQLParser|WARNING: ", log.Ldate|log.Ltime)
		error_ := log.New(file, "MSSQLParser|ERROR: ", log.Ldate|log.Ltime)
		Mslogger = MSLogger{info: info, warning: warning, error_: error_, active: active}
	} else {
		Mslogger = MSLogger{active: active}
	}

}

func (msLogger MSLogger) Info(msg string) {
	if msLogger.active {
		msLogger.info.Println(msg)
	}
}

func (msLogger MSLogger) Error(msg any) {
	if msLogger.active {
		msLogger.error_.Println(msg)
	}
}

func (msLogger MSLogger) Warning(msg string) {
	if msLogger.active {
		msLogger.warning.Println(msg)
	}
}
