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

func writeImages(wg *sync.WaitGroup, images chan utils.Image, tablename string, folder string) {
	defer wg.Done()

	base_path := path.Join(folder, "images", tablename)
	err := os.MkdirAll(base_path, 0750)
	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}
	for image := range images {
		if len(image.Content) == 0 {
			continue
		}
		imagefilename := image.GetFilename()
		file, err := os.Create(path.Join(base_path, imagefilename))

		if err != nil {
			mslogger.Mslogger.Error(fmt.Sprintf("failed to open file %s", err))
		}
		defer file.Close()
		_, err = file.Write(image.Content)

		if err != nil {
			log.Fatal(err)
		}
	}

}
