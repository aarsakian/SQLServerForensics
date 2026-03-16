package exporter

import (
	mslogger "MSSQLParser/logger"
	"MSSQLParser/utils"
	"fmt"
	"log"
	"os"
	"path"
	"sync"
)

func writeBlobs(wg *sync.WaitGroup, blobs chan utils.Blob, folder string) {
	defer wg.Done()

	base_path := path.Join(folder, "images")
	err := os.MkdirAll(base_path, 0750)
	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}
	for blob := range blobs {
		if len(blob.Content) == 0 {
			continue
		}
		blobfilename := blob.GetFilename()
		file, err := os.Create(path.Join(base_path, blobfilename))

		if err != nil {
			mslogger.Mslogger.Error(fmt.Sprintf("failed to open file %s", err))
		}
		defer file.Close()
		_, err = file.Write(blob.Content)

		if err != nil {
			log.Fatal(err)
		}
	}

}
