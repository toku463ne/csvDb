package csvdb

import (
	"os"
	"strconv"
)

func (p *Partition) openW(writeMode string) (*CsvWriter, error) {
	writer, err := newCsvWriter(p.path, writeMode)
	if err != nil {
		return nil, err
	}
	return writer, nil
}

func (p *Partition) OpenCur() *CsvCursor {
	_, filenames := getSortedGlob(p.path)
	cur := new(CsvCursor)
	cur.filenames = filenames
	cur.currReadingFileIdx = -1
	return cur
	//return nil
}

func (p *Partition) Drop() error {
	if _, err := os.Stat(p.path); os.IsNotExist(err) {
		return nil
	}
	err := os.Remove(p.path)
	return err
}

func (p *Partition) InsertRows(rows [][]string, writeMode string) error {
	writer, err := p.openW(writeMode)
	if err != nil {
		return err
	}
	defer writer.close()
	for _, row := range rows {
		if err := writer.write(row); err != nil {
			return err
		}
	}
	writer.flush()
	return nil
}

func (p *Partition) Query(condF func([]string) bool) ([][]string, error) {
	cur := p.OpenCur()
	filenames := cur.filenames
	found := [][]string{}
	defer cur.Close()
	for _, filename := range filenames {
		reader, err := newCsvReader(filename)
		if err != nil {
			return nil, err
		}
		defer reader.close()
		for reader.next() {
			v := reader.values
			if condF == nil || condF(v) {
				found = append(found, v)
			}
		}
	}
	return found, nil
}

func (p *Partition) minmax(fieldname string,
	condF func([]string) bool) (float64, float64, []string, []string, error) {
	rows, err := p.Query(condF)
	if err != nil {
		return 0.0, 0.0, nil, nil, err
	}
	if rows == nil {
		return 0.0, 0.0, nil, nil, nil
	}
	maxVal := float64(0.0)
	minVal := float64(0.0)
	var maxRow []string
	var minRow []string
	fIdx := p.colMap[fieldname]
	for i, row := range rows {
		vstr := row[fIdx]
		v, err := strconv.ParseFloat(vstr, 64)
		if err != nil {
			continue
		}
		if i == 0 {
			maxVal = v
			minVal = v
			maxRow = row
			minRow = row
		} else {
			if v > maxVal {
				maxVal = v
				maxRow = row
			}
			if v < minVal {
				minVal = v
				minRow = row
			}
		}
	}
	return minVal, maxVal, minRow, maxRow, nil
}

func (p *Partition) Min(fieldname string,
	condF func([]string) bool) (float64, []string, error) {
	mi, _, miR, _, err := p.minmax(fieldname, condF)
	return mi, miR, err
}

func (p *Partition) Max(fieldname string,
	condF func([]string) bool) (float64, []string, error) {
	_, ma, _, maR, err := p.minmax(fieldname, condF)
	return ma, maR, err
}

func (p *Partition) Count(condF func([]string) bool) (int, error) {
	rows, err := p.Query(condF)
	if err != nil {
		return -1, err
	}
	if rows == nil {
		return 0, nil
	}
	return len(rows), nil
}

func (p *Partition) Sum(colname string,
	condF func([]string) bool) (float64, error) {
	rows, err := p.Query(condF)
	if err != nil {
		return -1.0, err
	}
	if rows == nil {
		return 0.0, nil
	}

	colIdx := p.colMap[colname]

	s := 0.0
	for _, row := range rows {
		vstr := row[colIdx]
		v, err := strconv.ParseFloat(vstr, 64)
		if err != nil {
			continue
		}
		s += v
	}
	return s, nil
}

func (p *Partition) Select1rec(condF func([]string) bool) ([]string, error) {
	rows, err := p.Query(condF)
	if err != nil {
		return nil, err
	}
	if rows == nil {
		return nil, nil
	}
	for _, v := range rows {
		return v, nil
	}
	return nil, nil
}
