package csvdb

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

// NewCsvDB(baseDir) create a new csvDB object
func NewCsvDB(baseDir string) (*csvDB, error) {
	db := new(csvDB)
	db.tables = map[string]*tableDef{}
	db.baseDir = baseDir
	if _, err := os.Stat(baseDir); os.IsNotExist(err) {
		os.Mkdir(baseDir, 0755)
	} else if err != nil {
		return nil, err
	}

	iniFiles, err := filepath.Glob(fmt.Sprintf("%s/*.%s", baseDir, cTblIniExt))
	if err != nil {
		return nil, err
	}
	for _, iniFile := range iniFiles {
		td := new(tableDef)
		if err := td.load(iniFile); err == nil {
			db.tables[td.name] = td
		} else {
			return nil, err
		}
	}

	return db, nil
}

func (db *csvDB) CreateCsvTable(name string,
	columns []string, useGzip bool) (*csvTable, error) {
	td := new(tableDef)
	td.init(name, db.baseDir)

	if pathExist(td.iniFile) {
		return nil, errors.New(fmt.Sprintf("The table %s exists", name))
	}
	t := new(csvTable)
	if err := t.initAndSave(name, db.baseDir, columns, useGzip); err != nil {
		return nil, err
	}
	db.tables[name] = t.tableDef
	return t, nil
}

// DropAllTables() drop all tables in the csvDB object
func (db *csvDB) DropAllTables() error {
	for _, t := range db.tables {
		if err := db.DropTable(t.name); err != nil {
			return err
		}
	}
	return nil
}

func (db *csvDB) DropTable(name string) error {
	td := db.tables[name]
	if td == nil {
		return nil
	}

	if pathExist(td.dataDir) {
		if err := os.RemoveAll(td.dataDir); err != nil {
			return err
		}
	}

	if pathExist(td.iniFile) {
		if err := os.Remove(td.iniFile); err != nil {
			return errors.WithStack(err)
		}
	}
	delete(db.tables, name)
	return nil
}
