package csvdb

func (cur *csvCursor) values() []string {
	if cur.currReader != nil {
		return cur.currReader.values
	}
	return nil
}

func (cur *csvCursor) next() bool {
	if cur == nil {
		return false
	}
	if cur.currReader != nil {
		ret := cur.currReader.next()
		cur.err = cur.currReader.err
		return ret
	}
	if cur.filenames == nil {
		cur.err = nil
		return false
	}
	cur.currReadingFileIdx++
	if cur.currReadingFileIdx >= len(cur.filenames) {
		cur.err = nil
		return false
	}

	reader, err := newCsvReader(cur.filenames[cur.currReadingFileIdx])
	if err != nil {
		cur.err = err
		return false
	}

	ret := reader.next()
	cur.err = reader.err
	cur.currReader = reader
	return ret
}

func (cur *csvCursor) close() {
	if cur == nil {
		return
	}
	if cur.currReader != nil {
		cur.currReader.close()
	}
	cur.currReader = nil
	cur.currReadingFileIdx = -1
	cur.err = nil
}
