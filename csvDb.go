package csvdb

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

// NewCsvDB(baseDir) create a new CsvDB object
func NewCsvDB(baseDir string) (*CsvDB, error) {
	db := new(CsvDB)
	db.tables = map[string]*TableDef{}
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
		td := new(TableDef)
		if err := td.load(iniFile); err == nil {
			db.tables[td.name] = td
		} else {
			return nil, err
		}
	}

	return db, nil
}

func (db *CsvDB) CreateCsvTable(name string,
	columns []string, useGzip bool, bufferSize int) (*CsvTable, error) {
	td := new(TableDef)
	td.init(name, db.baseDir)

	if pathExist(td.iniFile) {
		return nil, errors.New(fmt.Sprintf("The table %s exists", name))
	}
	t := new(CsvTable)
	if err := t.initAndSave(name, db.baseDir, columns, useGzip); err != nil {
		return nil, err
	}

	if bufferSize == 0 {
		t.bufferSize = cDefaultBuffSize
	} else {
		t.bufferSize = bufferSize
	}

	db.tables[name] = t.TableDef
	return t, nil
}

func (db *CsvDB) GetTable(name string) (*CsvTable, error) {
	td := new(TableDef)
	td.init(name, db.baseDir)
	t := new(CsvTable)
	if err := t.load(td.iniFile, db.baseDir); err != nil {
		return nil, err
	}
	return t, nil
}

func (db *CsvDB) GetTableNames() []string {
	tableNames := make([]string, len(db.tables))
	i := 0
	for tableName, _ := range db.tables {
		tableNames[i] = tableName
		i++
	}
	return tableNames
}

// DropAllTables() drop all tables in the CsvDB object
func (db *CsvDB) DropAllTables() error {
	for _, t := range db.tables {
		if err := db.DropTable(t.name); err != nil {
			return err
		}
	}
	return nil
}

func (db *CsvDB) DropTable(name string) error {
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

func (db *CsvDB) TableExists(name string) bool {
	_, ok := db.tables[name]
	return ok
}

func (db *CsvDB) CreateCsvTableIfNotExists(name string,
	columns []string, useGzip bool, bufferSize int) (*CsvTable, error) {
	if !db.TableExists(name) {
		return db.CreateCsvTable(name, columns, useGzip, bufferSize)
	}
	return db.GetTable(name)
}
