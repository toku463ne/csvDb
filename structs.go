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
	bufferSize int
	useGzip    bool
	columns    []string
	colMap     map[string]int
}

type Partition struct {
	tableName   string
	partitionID string
	columns     []string
	colMap      map[string]int
	useGzip     bool
	path        string
	bufferSize  int
	rows        [][]string
	rowsPos     int
}

type Rows struct {
	cur   *CsvCursor
	condF func([]string) bool
}

type CsvCursor struct {
	filenames          []string
	currReadingFileIdx int
	currReader         *CsvReader
	Err                error
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
