package csvdb

import (
	"compress/gzip"
	"encoding/csv"
	"os"
)

type CsvDB struct {
	tables  map[string]*TableDef
	baseDir string
}

type TableDef struct {
	name    string
	iniFile string
	dataDir string
}

type CsvTable struct {
	*TableDef
	useGzip bool
	columns []string
	colMap  map[string]int
}

type Partition struct {
	tableName   string
	partitionID string
	colMap      map[string]int
	useGzip     bool
	path        string
}

type CsvCursor struct {
	filenames          []string
	currReadingFileIdx int
	currReader         *CsvReader
	err                error
}

type CsvReader struct {
	fr       *os.File
	zr       *gzip.Reader
	reader   *csv.Reader
	values   []string
	err      error
	filename string
	mode     string
}

type CsvWriter struct {
	fw     *os.File
	zw     *gzip.Writer
	writer *csv.Writer
	path   string
	mode   string
}
