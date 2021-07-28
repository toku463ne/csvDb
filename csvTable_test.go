package csvdb

import "testing"

func TestCsvTable1(t *testing.T) {
	checkIDsCount := func(tb *CsvTable, title string, startID, endID, expectedCnt int) error {
		f := func(row []string) bool {
			var v int
			conv(row[0], &v)
			if v >= startID && v <= endID {
				return true
			}
			return false
		}
		gotCnt := tb.Count(f)
		return getGotExpErr(title, gotCnt, expectedCnt)
	}
	checkRow := func(title string, got []interface{}, want []interface{}) error {
		for i, g := range got {
			return getGotExpErr(title, g, want[i])
		}
		return nil
	}

	rootDir, err := ensureTestDir("TestCsvTable1")
	if err != nil {
		t.Errorf("%v", err)
	}
	name := "test1"
	bufferSize := 3

	db, err := NewCsvDB(rootDir)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	err = db.DropAllTables()
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	tb, err := db.CreateCsvTable(name,
		[]string{"id", "name", "class"}, false, bufferSize)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	rows := [][]interface{}{
		{1, "class1"},
		{2, "class2"},
	}

	for _, row := range rows {
		if err := tb.InsertRow([]string{"id", "class"}, row...); err != nil {
			t.Errorf("%v", err)
			return
		}
	}

	if err := checkIDsCount(tb, "not committed", 1, 2, -1); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := tb.InsertRow(nil, 3, "user3", "class3"); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := checkIDsCount(tb, "committed", 1, 3, 3); err != nil {
		t.Errorf("%v", err)
		return
	}
	var id int
	var user string
	var class string
	if err := tb.Select1Row(func(v []string) bool { return v[0] == "1" },
		nil, &id, &user, &class); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := checkRow("1st row", []interface{}{id, user, class},
		[]interface{}{1, "", "class1"}); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := tb.Select1Row(func(v []string) bool { return v[0] == "2" },
		[]string{"id", "class"}, &id, &class); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := checkRow("2nd row", []interface{}{id, class},
		[]interface{}{2, "class2"}); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := tb.Select1Row(func(v []string) bool { return v[0] == "3" },
		nil, &id, &user, &class); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := checkRow("3rd row", []interface{}{id, user, class},
		[]interface{}{3, "user3", "class3"}); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := tb.InsertRow([]string{"id", "name"}, 4, "user4"); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := checkIDsCount(tb, "not committed", 1, 4, 3); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := tb.Flush(); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := checkIDsCount(tb, "committed", 1, 4, 4); err != nil {
		t.Errorf("%v", err)
		return
	}

	tb = nil

	tb, err = db.GetTable("test1")
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := checkIDsCount(tb, "readed", 1, 4, 4); err != nil {
		t.Errorf("%v", err)
		return
	}

	rows = [][]interface{}{
		{5, "user5", "class5"},
		{6, "user6", "class6"},
		{7, "user7", "class7"},
	}

	for _, row := range rows {
		if err := tb.InsertRow(nil, row...); err != nil {
			t.Errorf("%v", err)
			return
		}
	}
	if err := checkIDsCount(tb, "commit after read", 1, 7, 7); err != nil {
		t.Errorf("%v", err)
		return
	}
}
