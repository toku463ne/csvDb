package csvdb

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/go-ini/ini"
	"github.com/pkg/errors"
)

func (t *csvTable) initAndSave(name, rootDir string,
	columns []string, useGzip bool) error {
	t.tableDef = new(tableDef)
	t.tableDef.init(name, rootDir)

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

func (t *csvTable) saveTableToIni() error {
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

func (t *csvTable) load(iniFile, rootDir string) error {
	if err := t.tableDef.load(iniFile); err != nil {
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

func (t *csvTable) validatePartitionID(partitionID string) error {
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

func (t *csvTable) getPartitionPath(partitionID string) string {
	path := ""
	filename := fmt.Sprintf("%s.csv", partitionID)

	if t.useGzip {
		filename = fmt.Sprintf("%s.gz", filename)
	}

	path = fmt.Sprintf("%s/%s", t.dataDir, filename)
	return path
}

func (t *csvTable) GetPartitionIDs() []string {
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

func (t *csvTable) getPartitionID(path string) string {
	tokens := strings.Split(path, ".")
	return tokens[0]
}

func (t *csvTable) GetDefaultPartition() *partition {
	p, _ := t.GetPartition(cDefaultPartitionID)
	return p
}

func (t *csvTable) GetPartition(partitionID string) (*partition, error) {
	if err := t.validatePartitionID(partitionID); err != nil {
		return nil, errors.WithStack(err)
	}

	p := new(partition)
	p.partitionID = partitionID
	p.tableName = t.name
	p.path = t.getPartitionPath(partitionID)
	p.colMap = t.colMap
	p.useGzip = t.useGzip
	return p, nil
}
