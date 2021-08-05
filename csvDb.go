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
	db.groups = make(map[string]*CsvTableGroup)
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
		g := new(CsvTableGroup)
		if err := g.load(iniFile); err != nil {
			return nil, err
		}
		db.groups[g.groupName] = g
	}

	return db, nil
}

func (db *CsvDB) CreateGroup(groupName string,
	columns []string, useGzip bool, bufferSize int) (*CsvTableGroup, error) {
	g, err := newCsvTableGroup(groupName, db.baseDir, columns, useGzip, bufferSize)
	if err != nil {
		return nil, err
	}
	db.groups[groupName] = g
	return g, nil
}

func (db *CsvDB) createTable(groupName, tableName string,
	columns []string, useGzip bool, bufferSize int) (*CsvTable, error) {

	if groupName == "" {
		groupName = tableName
	}

	g, ok := db.groups[groupName]
	var err error
	if ok {
		if db.TableExists(groupName, tableName) {
			return nil, errors.New(fmt.Sprintf("The table %s exists", tableName))
		}
	} else {
		g, err = newCsvTableGroup(groupName, db.baseDir, columns, useGzip, bufferSize)
		if err != nil {
			return nil, err
		}
	}

	t, err := g.CreateTable(tableName)
	if err != nil {
		return nil, err
	}

	db.groups[groupName] = g
	return t, nil
}

func (db *CsvDB) CreateTable(tableName string,
	columns []string, useGzip bool, bufferSize int) (*CsvTable, error) {
	return db.createTable("", tableName, columns, useGzip, bufferSize)
}

func (db *CsvDB) GetTable(tableName string) (*CsvTable, error) {
	return db.getTable("", tableName)
}

func (db *CsvDB) getTable(groupName, tableName string) (*CsvTable, error) {
	if groupName == "" {
		groupName = tableName
	}
	g, ok := db.groups[groupName]
	if ok {
		return g.GetTable(tableName)
	} else {
		return g.CreateTable(tableName)
	}
	return nil, nil
}

// DropAllTables() drop all tables in the CsvDB object
func (db *CsvDB) DropAll() error {
	for _, g := range db.groups {
		if err := g.Drop(); err != nil {
			return err
		}
	}
	return nil
}

func (db *CsvDB) dropTable(groupName, tableName string) error {
	if groupName == "" {
		groupName = tableName
	}
	g, ok := db.groups[groupName]
	if ok {
		return g.DropTable(tableName)
	}
	return nil
}

func (db *CsvDB) DropTable(tableName string) error {
	return db.dropTable("", tableName)
}

func (db *CsvDB) GroupExists(groupName string) bool {
	_, ok := db.groups[groupName]
	return ok
}

func (db *CsvDB) TableExists(groupName, tableName string) bool {
	if groupName == "" {
		groupName = tableName
	}
	g := db.groups[groupName]
	return g.TableExists(tableName)
}

func (db *CsvDB) CreateTableIfNotExists(groupName, tableName string,
	columns []string, useGzip bool, bufferSize int) (*CsvTable, error) {
	if groupName == "" {
		groupName = tableName
	}
	g, ok := db.groups[groupName]
	var err error
	if !ok {
		g, err = db.CreateGroup(groupName, columns, useGzip, bufferSize)
		if err != nil {
			return nil, err
		}
	}
	return g.CreateTableIfNotExists(tableName)
}
