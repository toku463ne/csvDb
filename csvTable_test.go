package csvdb

import (
	"strconv"
	"testing"
)

func TestCsvTable1(t *testing.T) {
	checkIDsCount := func(tb *CsvTable, title string, startID, endID, expectedCnt int) error {
		f := func(row []string) bool {
			var v int
			convFromString(row[0], &v)
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

	err = db.DropAll()
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	tb, err := db.CreateTable(name,
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

	if err := checkIDsCount(tb, "not committed", 1, 2, 0); err != nil {
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
	if err := tb.Flush(); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := checkIDsCount(tb, "2nd flush after commit", 1, 4, 4); err != nil {
		t.Errorf("%v", err)
		return
	}

	tb = nil

	tb, err = db.GetTable("test1")
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	if tb == nil {
		t.Errorf("table test1 does not exist")
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

	var sum int
	if err := tb.Sum(nil, "id", &sum); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := getGotExpErr("sum", sum, 28); err != nil {
		t.Errorf("%v", err)
		return
	}

	var v int
	if err := tb.Max(func(row []string) bool {
		v, _ := strconv.Atoi(row[0])
		return v < 6
	}, "id", &v); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := getGotExpErr("max", v, 5); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := tb.Min(func(row []string) bool {
		v, _ := strconv.Atoi(row[0])
		return v > 1
	}, "id", &v); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := getGotExpErr("min", v, 2); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := tb.Update(func(v []string) bool {
		return v[2] == "class2"
	},
		map[string]interface{}{
			"name": "user2",
		}); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := checkIDsCount(tb, "count after update", 1, 7, 7); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := tb.Select1Row(func(v []string) bool { return v[0] == "2" },
		nil, &id, &user, &class); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := checkRow("updated row", []interface{}{id, user, class},
		[]interface{}{2, "user2", "class2"}); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := tb.Delete(func(v []string) bool { return (v[2] == "class1" || v[1] == "user7") }); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := checkIDsCount(tb, "count after delete", 1, 7, 5); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := checkIDsCount(tb, "count deleted row", 1, 1, 0); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := checkIDsCount(tb, "count deleted row", 7, 7, 0); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := tb.Upsert(func(v []string) bool {
		return v[0] == "8"
	}, map[string]interface{}{
		"id":    8,
		"name":  "user8",
		"class": "class8",
	}); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := checkIDsCount(tb, "count after upsert", 1, 8, 6); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := checkIDsCount(tb, "count upserted row", 8, 8, 1); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := tb.Upsert(func(v []string) bool {
		return v[0] == "5"
	}, map[string]interface{}{
		"id":    5,
		"name":  "user5upserted",
		"class": "class5upsesrted",
	}); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := checkIDsCount(tb, "count after 2nd upsert", 1, 8, 6); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := tb.Select1Row(func(v []string) bool { return v[0] == "5" },
		nil, &id, &user, &class); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := checkRow("updated row by upsert", []interface{}{id, user, class},
		[]interface{}{5, "user5upserted", "class5upserted"}); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := tb.Truncate(); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := checkIDsCount(tb, "count after truncate", 1, 8, 0); err != nil {
		t.Errorf("%v", err)
		return
	}
}

func TestCsvTableGzip(t *testing.T) {
	checkIDsCount := func(tb *CsvTable, title string, startID, endID, expectedCnt int) error {
		f := func(row []string) bool {
			var v int
			convFromString(row[0], &v)
			if v >= startID && v <= endID {
				return true
			}
			return false
		}
		gotCnt := tb.Count(f)
		return getGotExpErr(title, gotCnt, expectedCnt)
	}

	rootDir, err := ensureTestDir("TestCsvTableGzip")
	if err != nil {
		t.Errorf("%v", err)
	}
	name := "testgzip"
	bufferSize := 3

	db, err := NewCsvDB(rootDir)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	err = db.DropAll()
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	tb, err := db.CreateTable(name,
		[]string{"id", "name", "class"}, true, bufferSize)
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

	if err := checkIDsCount(tb, "not committed", 1, 2, 0); err != nil {
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
}

func TestCsvTableGroup(t *testing.T) {
	checkIDsCount := func(tb *CsvTable, title string, startID, endID, expectedCnt int) error {
		f := func(row []string) bool {
			var v int
			convFromString(row[0], &v)
			if v >= startID && v <= endID {
				return true
			}
			return false
		}
		gotCnt := tb.Count(f)
		return getGotExpErr(title, gotCnt, expectedCnt)
	}

	rootDir, err := ensureTestDir("TestGroupTable3")
	if err != nil {
		t.Errorf("%v", err)
	}
	name := "grptest3"
	bufferSize := 3

	db, err := NewCsvDB(rootDir)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	err = db.DropAll()
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	g, err := db.CreateGroup(name,
		[]string{"id", "name", "class"}, true, bufferSize)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	tb1, err := g.CreateTable("table1")
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	rows := [][]interface{}{
		{1, "class1"},
		{2, "class2"},
		{3, "class3"},
	}
	for _, row := range rows {
		if err := tb1.InsertRow([]string{"id", "class"}, row...); err != nil {
			t.Errorf("%v", err)
			return
		}
	}

	if err := checkIDsCount(tb1, "check data inserted to table1", 1, 3, 3); err != nil {
		t.Errorf("%v", err)
		return
	}

	tb2, err := g.CreateTable("table2")
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	rows = [][]interface{}{
		{4, "user1"},
		{5, "user2"},
		{6, "user3"},
	}
	for _, row := range rows {
		if err := tb2.InsertRow([]string{"id", "name"}, row...); err != nil {
			t.Errorf("%v", err)
			return
		}
	}

	if err := checkIDsCount(tb2, "check data inserted to table2", 1, 6, 3); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := getGotExpErr("number of tables", len(g.tableDefs), 2); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := getGotExpErr("total count", g.Count(nil), 6); err != nil {
		t.Errorf("%v", err)
		return
	}
}
