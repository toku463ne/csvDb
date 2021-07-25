package csvdb

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/go-ini/ini"
	"github.com/pkg/errors"
)

func (t *CsvTable) initAndSave(name, rootDir string,
	columns []string, useGzip bool) error {
	t.TableDef = new(TableDef)
	t.TableDef.init(name, rootDir)

	t.columns = columns
	t.useGzip = useGzip

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
		}
	}
	return nil
}

func (t *CsvTable) validatepartitionID(partitionID string) error {
	if strings.Contains(partitionID, ".") {
		return errors.New("partitionID cannot include '.'")
	}
	if partitionID == "" {
		return errors.New("partitionID be ''")
	}
	if strings.Contains(partitionID, "*") {
		return errors.New("partitionID include '*'")
	}
	return nil
}

func (t *CsvTable) getPartitionPath(partitionID string) string {
	path := ""
	filename := fmt.Sprintf("%s.csv", partitionID)

	if t.useGzip {
		filename = fmt.Sprintf("%s.gz", filename)
	}

	path = fmt.Sprintf("%s/%s", t.dataDir, filename)
	return path
}

func (t *CsvTable) GetPartitionIDs() []string {
	_, filenames := getSortedGlob(t.getPartitionPath("*"))
	if len(filenames) == 0 {
		return nil
	}
	partitionIDs := make([]string, len(filenames))

	i := 0
	for _, filename := range filenames {
		partitionIDs[i] = t.getPartitionID(filename)
		i++
	}
	return partitionIDs
}

func (t *CsvTable) getPartitionID(path string) string {
	tokens := strings.Split(path, ".")
	return tokens[0]
}

func (t *CsvTable) GetDefaultPartition() *Partition {
	p, _ := t.GetPartition(cDefaultPartitionID)
	return p
}

func (t *CsvTable) GetPartition(partitionID string) (*Partition, error) {
	if err := t.validatepartitionID(partitionID); err != nil {
		return nil, errors.WithStack(err)
	}

	p := new(Partition)
	p.partitionID = partitionID
	p.tableName = t.name
	p.path = t.getPartitionPath(partitionID)
	p.colMap = t.colMap
	p.useGzip = t.useGzip
	return p, nil
}

func (t *CsvTable) GetColumns() []string {
	return t.columns
}

func (t *CsvTable) GetColMap() map[string]int {
	return t.colMap
}
