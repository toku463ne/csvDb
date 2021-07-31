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
	path    string
}

type CsvTable struct {
	*TableDef
	buff       *insertBuff
	bufferSize int
	useGzip    bool
	columns    []string
	colMap     map[string]int
}

type CsvRows struct {
	reader             *CsvReader
	selectedColIndexes []int
	tableCols          []string
	conditionCheckFunc func([]string) bool
}

type insertBuff struct {
	rows   [][]string
	pos    int
	isFull bool
	size   int
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
