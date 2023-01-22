package logger

import (
	"log"
	"os"
	"time"
)

type MSLogger struct {
	info    *log.Logger
	warning *log.Logger
	error   *log.Logger
	active  bool
}

func InitializeLogger() MSLogger {
	now := time.Now()
	logfilename := "logs" + now.String() + ".txt"
	file, err := os.OpenFile(logfilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}

	info := log.New(file, "INFO: ", log.Ldate|log.Ltime)
	warning := log.New(file, "WARNING: ", log.Ldate|log.Ltime|log.Lshortfile)
	error := log.New(file, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
	return MSLogger{info: info, warning: warning, error: error}
}

func (msLogger *MSLogger) SetStatus(active bool) {
	msLogger.active = active
}

func (msLogger MSLogger) Info(msg string) {
	if msLogger.active {
		msLogger.info.Println(msg)
	}
}

func (msLogger MSLogger) Error(msg any) {
	if msLogger.active {
		msLogger.error.Println(msg)
	}
}

func (msLogger MSLogger) Warning(msg string) {
	if msLogger.active {
		msLogger.warning.Println(msg)
	}
}
