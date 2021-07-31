package csvdb

import (
	"fmt"

	"github.com/pkg/errors"
)

func newCsvRows(conditionCheckFunc func([]string) bool,
	path string, tableCols, selectedCols []string) (*CsvRows, error) {
	reader, err := newCsvReader(path)
	if err != nil {
		return nil, err
	}
	r := new(CsvRows)
	r.reader = reader
	r.conditionCheckFunc = conditionCheckFunc
	r.tableCols = tableCols

	colIndexes := make([]int, len(selectedCols))
	for i, cols := range selectedCols {
		ok := false
		for j, colt := range tableCols {
			if colt == cols {
				colIndexes[i] = j
				ok = true
				break
			}
		}
		if !ok {
			return nil, errors.New(fmt.Sprintf("col %s is not in the table", cols))
		}
	}
	r.selectedColIndexes = colIndexes
	return r, nil
}

func (r *CsvRows) Next() bool {
	for r.reader.next() {
		if r.conditionCheckFunc == nil || r.conditionCheckFunc(r.reader.values) {
			return true
		}
	}
	return false
}

func (r *CsvRows) Err() error {
	return r.reader.err
}

func (r *CsvRows) Scan(args ...interface{}) error {
	if r.selectedColIndexes == nil || len(r.selectedColIndexes) == 0 {
		if len(args) != len(r.tableCols) {
			return errors.New(fmt.Sprintf("Got %d args while expected %d",
				len(args), len(r.tableCols)))
		}
		for i, _ := range r.tableCols {
			src := r.reader.values[i] //r.tableCols[i]
			dst := args[i]
			if err := convFromString(src, dst); err != nil {
				return err
			}
		}
	} else {
		if len(args) != len(r.selectedColIndexes) {
			return errors.New(fmt.Sprintf("Got %d args while expected %d",
				len(args), len(r.selectedColIndexes)))
		}
		for argidx, colidx := range r.selectedColIndexes {
			src := r.reader.values[colidx]
			dst := args[argidx]
			if err := convFromString(src, dst); err != nil {
				return err
			}
		}
	}
	return nil
}
