package logger

import (
	"log"
	"os"
)

type MSLogger struct {
	info    *log.Logger
	warning *log.Logger
	error_  *log.Logger
	level   string
	file    *os.File
}

var Mslogger MSLogger

func ValidLevel(level string) bool {
	return level == "info" || level == "warning" || level == "error"
}

func InitializeLogger(level string, logfilename string) {
	if Mslogger.file != nil {
		Mslogger.file.Close()
	}

	if ValidLevel(level) {

		file, err := os.OpenFile(logfilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			log.Fatal(err)
		}

		info := log.New(file, "MSSQLParser|INFO: ", log.Ldate|log.Ltime)
		warning := log.New(file, "MSSQLParser|WARNING: ", log.Ldate|log.Ltime)
		error_ := log.New(file, "MSSQLParser|ERROR: ", log.Ldate|log.Ltime)
		Mslogger = MSLogger{info: info, warning: warning, error_: error_, level: level, file: file}
	} else {
		Mslogger = MSLogger{}
	}

}

func (msLogger MSLogger) Info(msg string) {
	if msLogger.level == "info" {
		msLogger.info.Println(msg)
	}
}

func (msLogger MSLogger) Error(msg any) {
	if msLogger.level == "info" || msLogger.level == "warning" || msLogger.level == "error" {
		msLogger.error_.Println(msg)
	}
}

func (msLogger MSLogger) Warning(msg string) {
	if msLogger.level == "info" || msLogger.level == "warning" {
		msLogger.warning.Println(msg)
	}
}
