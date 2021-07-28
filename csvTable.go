package csvdb

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/go-ini/ini"
	"github.com/pkg/errors"
)

func (t *CsvTable) initAndSave(name, rootDir string,
	columns []string, useGzip bool, bufferSize int) error {
	t.TableDef = new(TableDef)
	t.TableDef.init(name, rootDir)

	t.columns = columns
	t.useGzip = useGzip
	t.path = t.getPath()

	if bufferSize == 0 {
		t.bufferSize = cDefaultBuffSize
	} else {
		t.bufferSize = bufferSize
	}
	t.buff = newInsertBuffer(t.bufferSize)

	colMap := map[string]int{}
	for i, col := range columns {
		colMap[col] = i
	}
	t.colMap = colMap

	if err := t.saveTableToIni(); err != nil {
		return err
	}
	return nil
}

func (t *CsvTable) saveTableToIni() error {
	file, err := os.OpenFile(t.iniFile, os.O_CREATE, 0640)
	if err != nil {
		return err
	}
	defer file.Close()

	cfg, err := ini.Load(t.iniFile)
	if err != nil {
		return errors.WithStack(err)
	}
	cfg.Section("conf").Key("name").SetValue(t.name)
	cfg.Section("conf").Key("columns").SetValue(strings.Join(t.columns, ","))
	cfg.Section("conf").Key("useGzip").SetValue(strconv.FormatBool(t.useGzip))
	cfg.Section("conf").Key("bufferSize").SetValue(strconv.Itoa(t.bufferSize))

	if err := cfg.SaveTo(t.iniFile); err != nil {
		return errors.WithStack(err)
	}

	if _, err := os.Stat(t.dataDir); os.IsNotExist(err) {
		if err := os.MkdirAll(t.dataDir, 0755); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (t *CsvTable) load(iniFile, rootDir string) error {
	t.TableDef = new(TableDef)
	if err := t.TableDef.load(iniFile); err != nil {
		return err
	}
	cfg, err := ini.Load(iniFile)
	if err != nil {
		return err
	}
	for _, k := range cfg.Section("conf").Keys() {
		switch k.Name() {
		case "name":
			name := k.MustString("")
			if name == "" {
				return errors.New("Not available ini file")
			}
			t.name = name
		case "columns":
			columns := strings.Split(k.MustString(""), ",")
			t.columns = columns
			colMap := map[string]int{}
			for i, col := range columns {
				colMap[col] = i
			}
			t.colMap = colMap

		case "useGzip":
			t.useGzip = k.MustBool(false)

		case "bufferSize":
			t.bufferSize = k.MustInt(cDefaultBuffSize)
		}
	}
	t.buff = newInsertBuffer(t.bufferSize)

	return nil
}

func (t *CsvTable) Count(conditionCheckFunc func([]string) bool) int {
	reader, err := newCsvReader(t.path)
	if err != nil {
		return -1
	}
	cnt := 0
	defer reader.close()
	for reader.next() {
		v := reader.values
		if conditionCheckFunc == nil {
			cnt++
		} else if conditionCheckFunc(v) {
			cnt++
		}
	}
	if reader.err != nil && reader.err != io.EOF {
		return -1
	}
	return cnt
}

func (t *CsvTable) SelectRows(conditionCheckFunc func([]string) bool,
	colNames []string) (*csvRows, error) {
	return newCsvRows(conditionCheckFunc,
		t.path, t.columns, colNames)
}

func (t *CsvTable) Select1Row(conditionCheckFunc func([]string) bool,
	colNames []string, args ...interface{}) error {
	r, err := t.SelectRows(conditionCheckFunc, colNames)
	if err != nil {
		return err
	}
	for r.Next() {
		return r.Scan(args...)
	}
	return errors.New("No record found")
}

func (t *CsvTable) readRows(conditionCheckFunc func([]string) bool) ([][]string, error) {
	reader, err := newCsvReader(t.path)
	if err != nil {
		return nil, err
	}
	found := [][]string{}
	defer reader.close()
	for reader.next() {
		v := reader.values
		if conditionCheckFunc == nil {
			found = append(found, v)
		} else if conditionCheckFunc(v) {
			found = append(found, v)
		}
	}
	if reader.err != nil {
		return nil, reader.err
	}
	return found, nil
}

func (t *CsvTable) InsertRow(columns []string, args ...interface{}) error {
	if columns == nil && len(args) != len(t.columns) {
		return errors.New("len of args do not match to table columns")
	}
	if columns != nil && len(columns) != len(args) {
		return errors.New("len of columns and args do not match")
	}

	row := make([]string, len(t.columns))
	if columns == nil {
		for i, v := range args {
			row[i] = asString(v)
		}
	} else {
		for i, col := range columns {
			j, ok := t.colMap[col]
			if !ok {
				return errors.New(fmt.Sprintf("column %s does not exist", col))
			}
			row[j] = asString(args[i])
		}
	}

	if t.buff.register(row) {
		t.Flush()
	}

	return nil
}

func (t *CsvTable) Flush() error {
	writer, err := t.openW(CWriteModeAppend)
	if err != nil {
		return err
	}
	defer writer.close()
	for i, row := range t.buff.rows {
		if err := writer.write(row); err != nil {
			t.buff.init()
			return err
		}
		if i >= t.buff.pos {
			break
		}
	}
	t.buff.init()
	writer.flush()
	return nil
}

func (t *CsvTable) openW(writeMode string) (*CsvWriter, error) {
	writer, err := newCsvWriter(t.path, writeMode)
	if err != nil {
		return nil, err
	}
	return writer, nil
}
