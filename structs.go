package csvdb

import (
	"compress/gzip"
	"encoding/csv"
	"os"
)

type csvDB struct {
	tables  map[string]*tableDef
	baseDir string
}

type tableDef struct {
	name    string
	iniFile string
	dataDir string
}

type csvTable struct {
	*tableDef
	useGzip bool
	columns []string
	colMap  map[string]int
}

type partition struct {
	tableName   string
	partitionID string
	colMap      map[string]int
	useGzip     bool
	path        string
}

type csvCursor struct {
	filenames          []string
	currReadingFileIdx int
	currReader         *csvReader
	err                error
}

type csvReader struct {
	fr       *os.File
	zr       *gzip.Reader
	reader   *csv.Reader
	values   []string
	err      error
	filename string
	mode     string
}

type csvWriter struct {
	fw     *os.File
	zw     *gzip.Writer
	writer *csv.Writer
	path   string
	mode   string
}
