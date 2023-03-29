package exporter

import (
	mslogger "MSSQLParser/logger"
	"MSSQLParser/utils"
	"fmt"
	"log"
	"os"
	"path"
)

func writeImages(images utils.Images, fileprefix string, folder string) {
	base_path := path.Join(folder, "images")
	err := os.Mkdir(base_path, 0750)
	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}
	for idx, image := range images {
		if len(image) == 0 {
			continue
		}
		file, err := os.Create(fmt.Sprintf("%s_%d.img", path.Join(base_path, fileprefix), idx))
		defer file.Close()
		if err != nil {
			mslogger.Mslogger.Error(fmt.Sprintf("failed to open file %s", err))
		}

		_, err = file.Write(image)

		if err != nil {
			log.Fatal(err)
		}
	}
}
