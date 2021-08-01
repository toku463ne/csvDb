package csvdb

import (
	"fmt"
	"os"
	"strings"

	"github.com/pkg/errors"
)

func (td *TableDef) getPath() string {
	path := fmt.Sprintf("%s/%s.csv", td.dataDir, td.name)
	if td.useGzip {
		path += ".gz"
	}
	return path
}

func (td *TableDef) init(name, rootDir string) {
	td.name = name
	//td.dataDir = fmt.Sprintf("%s/%s", rootDir, name)
	td.dataDir = rootDir
	td.iniFile = fmt.Sprintf("%s/%s.%s", rootDir, name, cTblIniExt)
}

func (td *TableDef) load(iniFile string) error {
	// iniFile = baseDir/tableType.tableName
	pos := strings.LastIndex(iniFile, "/")
	if pos == -1 {
		pos = strings.LastIndex(iniFile, "\\")
		if pos == -1 {
			return errors.New("Not a proper path : " + iniFile)
		}
	}
	dataDir := iniFile[:pos]

	fileName := iniFile[pos+1:]
	tokens := strings.Split(fileName, ".")
	if len(tokens) != 3 {
		return errors.New("Not a proper filename format : " + iniFile)
	}
	pos = strings.Index(iniFile, cTblIniExt)
	if pos == -1 {
		return errors.New("Not a proper extension : " + iniFile)
	}

	td.name = tokens[0]
	td.iniFile = iniFile
	td.dataDir = dataDir
	//td.path = td.getPath()
	return nil
}

func (td *TableDef) Drop() error {
	if pathExist(td.path) {
		if err := os.Remove(td.path); err != nil {
			return err
		}
	}

	if pathExist(td.iniFile) {
		if err := os.Remove(td.iniFile); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}
