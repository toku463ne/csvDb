package csvdb

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

func (td *tableDef) init(name, rootDir string) {
	td.name = name
	td.dataDir = fmt.Sprintf("%s/%s", rootDir, name)
	td.iniFile = fmt.Sprintf("%s.%s", td.dataDir, cTblIniExt)
}

func (td *tableDef) load(iniFile string) error {
	// iniFile = baseDir/tableType.tableName
	pos := strings.LastIndex(iniFile, "/")
	if pos == -1 {
		pos = strings.LastIndex(iniFile, "\\")
		if pos == -1 {
			return errors.New("Not a proper path : " + iniFile)
		}
	}
	fileName := iniFile[pos+1:]
	tokens := strings.Split(fileName, ".")
	if len(tokens) != 3 {
		return errors.New("Not a proper filename format : " + iniFile)
	}
	pos = strings.LastIndex(iniFile, cTblIniExt)
	if pos == -1 {
		return errors.New("Not a proper extension : " + iniFile)
	}
	dataDir := iniFile[:pos]

	td.name = tokens[0]
	td.iniFile = iniFile
	td.dataDir = dataDir
	return nil
}
