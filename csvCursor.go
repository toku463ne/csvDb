package csvdb

func (cur *CsvCursor) Values() []string {
	if cur.currReader != nil {
		return cur.currReader.values
	}
	return nil
}

func (cur *CsvCursor) Next() bool {
	if cur == nil {
		return false
	}
	if cur.currReader != nil {
		ret := cur.currReader.next()
		cur.Err = cur.currReader.err
		return ret
	}
	if cur.filenames == nil {
		cur.Err = nil
		return false
	}
	cur.currReadingFileIdx++
	if cur.currReadingFileIdx >= len(cur.filenames) {
		cur.Err = nil
		return false
	}

	reader, err := newCsvReader(cur.filenames[cur.currReadingFileIdx])
	if err != nil {
		cur.Err = err
		return false
	}

	ret := reader.next()
	cur.Err = reader.err
	cur.currReader = reader
	return ret
}

func (cur *CsvCursor) Close() {
	if cur == nil {
		return
	}
	if cur.currReader != nil {
		cur.currReader.close()
	}
	cur.currReader = nil
	cur.currReadingFileIdx = -1
	cur.Err = nil
}
