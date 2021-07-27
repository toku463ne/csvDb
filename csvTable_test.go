package csvdb

import (
	"strconv"
	"strings"
	"testing"
)

func TestCsvTable(t *testing.T) {
	rootDir, err := ensureTestDir("TestCsvTable")
	if err != nil {
		t.Errorf("%v", err)
	}
	name := "test1"

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
		[]string{"id", "name"}, false, 0)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	p1, err := tb.GetPartition("001")
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := p1.InsertRows([][]string{
		{"1", "test1"},
		{"2", "test2"},
	}, CWriteModeAppend); err != nil {
		t.Errorf("Error inserting table test : %v", err)
	}
	cur := p1.OpenCur()

	for cur.Next() {
		if cur.Err != nil {
			t.Errorf("Error getting data : %v", cur.Err)
		}
		v := cur.Values()
		if v[0] != "1" {
			t.Errorf("data error want=%s got=%s", "1", v[0])
		}
		if v[1] != "test1" {
			t.Errorf("data error want=%s got=%s", "test1", v[1])
		}
		break
	}

	cur.Close()

	p2, err := tb.GetPartition("002")
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := p2.InsertRows([][]string{
		{"3", "test3"},
		{"4", "test4"},
	}, CWriteModeAppend); err != nil {
		t.Errorf("Error inserting table test : %v", err)
	}

	if cnt, err := tb.Count(nil); err != nil {
		t.Errorf("%v", err)
		return
	} else {
		if cnt != 4 {
			t.Errorf("cnt error want=%d got=%d", 4, cnt)
		}
	}

	cur = p2.OpenCur()
	for cur.Next() {
		if cur.Err != nil {
			t.Errorf("Error getting data : %v", cur.Err)
		}
		v := cur.Values()
		if v[0] != "3" {
			t.Errorf("data error want=%s got=%s", "3", v[0])
		}
		if v[1] != "test3" {
			t.Errorf("data error want=%s got=%s", "test3", v[1])
		}
		break
	}

	cur.Close()

	//found, err := p2.GetStringData(map[string]string{"id": "3"})
	found, err := p2.GetStringData(func(v []string) bool { return v[0] == "3" })

	if err != nil {
		t.Errorf("Error setting read files : %v", err)
	}
	if found == nil {
		t.Errorf("Found no rows")
	}
	for _, v := range found {
		if v[0] != "3" {
			t.Errorf("data error want=%s got=%s", "3", v[0])
		}
		if v[1] != "test3" {
			t.Errorf("data error want=%s got=%s", "test3", v[1])
		}
		break
	}

	count, err := p2.Count(nil)
	if err != nil {
		t.Errorf("%v", err)
	}
	if count != 2 {
		t.Errorf("count error want=%d got=%d", 4, count)
	}

	if err := p2.Update(func(v []string) bool { return v[0] == "3" },
		map[string]string{"name": "test103"}); err != nil {
		t.Errorf("%v", err)
	}

	found, err = p2.GetStringData(func(v []string) bool { return v[0] == "3" })
	if err != nil {
		t.Errorf("Error setting read files : %v", err)
	}
	for _, v := range found {
		if v[1] != "test103" {
			t.Errorf("data error want=%s got=%s", "test103", v[1])
		}
		break
	}

	if err := p2.Delete(func(v []string) bool { return v[0] == "3" }); err != nil {
		t.Errorf("%v", err)
	}
	found, err = p2.GetStringData(func(v []string) bool { return v[0] == "3" })
	if err != nil {
		t.Errorf("Error setting read files : %v", err)
	}
	if len(found) > 0 {
		t.Errorf("The deleted record exists!")
	}
	if cnt, err := p2.Count(nil); err != nil {
		t.Errorf("%v", err)
		return
	} else if cnt != 1 {
		t.Errorf("count error want=%d got=%d", 1, cnt)
		return
	}

	p3, err := tb.GetPartition("003")
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	count, err = p3.Count(nil)
	if err != nil {
		t.Errorf("%v", err)
	}
	if count != 0 {
		t.Errorf("count error want=%d got=%d", 0, count)
	}

	err = db.DropTable("test1")
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	_, err = db.CreateCsvTable(name,
		[]string{"id", "name"}, false, 0)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

}

func TestCsvGzipTable(t *testing.T) {
	rootDir, err := ensureTestDir("TestCsvGzipTable")
	if err != nil {
		t.Errorf("%v", err)
	}

	db, err := NewCsvDB(rootDir)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := db.DropAllTables(); err != nil {
		t.Errorf("%v", err)
		return
	}

	tb, err := db.CreateCsvTable("test2",
		[]string{"id", "name"}, true, 0)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	p2, err := tb.GetPartition("002")
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := p2.InsertRows([][]string{
		{"3", "test3"},
		{"4", "test4"},
	}, CWriteModeAppend); err != nil {
		t.Errorf("Error inserting table test : %v", err)
	}

	path := tb.getPartitionPath("002")
	tokens := strings.Split(path, ".")
	ext := tokens[len(tokens)-1]
	if ext != "gz" {
		t.Errorf("Not a gzip : %v", ext)
		return
	}

	//found, err := p2.GetStringData(map[string]string{"id": "3"})
	found, err := p2.GetStringData(func(v []string) bool { return v[0] == "3" })
	if err != nil {
		t.Errorf("Error setting read files : %v", err)
	}
	if found == nil {
		t.Errorf("Found no rows")
		return
	}
	for _, v := range found {
		if v[0] != "3" {
			t.Errorf("data error want=%s got=%s", "3", v[0])
		}
		if v[1] != "test3" {
			t.Errorf("data error want=%s got=%s", "test3", v[1])
		}
		break
	}

}

func TestCsvTable2(t *testing.T) {
	rootDir, err := ensureTestDir("TestCsvTable2")
	if err != nil {
		t.Errorf("%v", err)
	}
	name := "test2"

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
		[]string{"id", "name"}, false, 0)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	p1, err := tb.GetPartition("001")
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := p1.InsertRows([][]string{
		{"1", "test1"},
		{"2", "test2"},
	}, CWriteModeAppend); err != nil {
		t.Errorf("Error inserting table test : %v", err)
	}

	p2, err := tb.GetPartition("002")
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := p2.InsertRows([][]string{
		{"3", "test1"},
		{"4", "test2"},
	}, CWriteModeAppend); err != nil {
		t.Errorf("Error inserting table test : %v", err)
	}

	if err := tb.InsertRows([][]string{
		{"5", "test1"},
		{"6", "test2"},
	}, CWriteModeAppend); err != nil {
		t.Errorf("Error inserting table test : %v", err)
	}

	if cnt, err := tb.Count(nil); err != nil {
		t.Errorf("%v", err)
		return
	} else {
		if cnt != 6 {
			t.Errorf("cnt error want=%d got=%d", 6, cnt)
			return
		}
	}

	if err := tb.Update(func(v []string) bool { return v[1] == "test1" },
		map[string]string{"name": "test101"}); err != nil {
		t.Errorf("%v", err)
		return
	}

	if cnt, err := tb.Count(func(v []string) bool { return v[1] == "test101" }); err != nil {
		t.Errorf("%v", err)
		return
	} else {
		if cnt != 3 {
			t.Errorf("cnt error want=%d got=%d", 3, cnt)
			return
		}
	}

	if err := tb.Delete(func(v []string) bool { return v[1] == "test2" }); err != nil {
		t.Errorf("%v", err)
		return
	}

	if cnt, err := tb.Count(nil); err != nil {
		t.Errorf("%v", err)
		return
	} else {
		if cnt != 3 {
			t.Errorf("cnt error want=%d got=%d", 3, cnt)
			return
		}
	}

	p := tb.GetDefaultPartition()
	res, err := p.Select1rec(nil)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	if res[0] != "5" {
		t.Errorf("cnt error want=%s got=%s", "5", res[0])
		return
	}

}

func TestCsvTable3(t *testing.T) {
	rootDir, err := ensureTestDir("TestCsvTable3")
	if err != nil {
		t.Errorf("%v", err)
	}
	name := "test3"

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
		[]string{"id", "name", "class"}, false, 3)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	p := tb.GetDefaultPartition()

	rows := [][]interface{}{
		{1, "user1"},
		{2, "user2"},
	}

	for _, row := range rows {
		p.InsertRow([]string{"id", "name"}, row...)
	}

	cnt, err := tb.Count(nil)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := getGotExpErr("count", cnt, 0); err != nil {
		t.Errorf("%v", err)
		return
	}

	rows = [][]interface{}{
		{3, "user3"},
		{4, "user4"},
	}
	for _, row := range rows {
		p.InsertRow([]string{"id", "name"}, row...)
	}

	cnt, err = tb.Count(nil)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := getGotExpErr("count", cnt, 3); err != nil {
		t.Errorf("%v", err)
		return
	}

	err = p.Flush()
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	cnt, err = tb.Count(nil)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := getGotExpErr("count", cnt, 4); err != nil {
		t.Errorf("%v", err)
		return
	}

	rows = [][]interface{}{
		{5, "class5"},
		{6, "class6"},
	}
	for _, row := range rows {
		p.InsertRow([]string{"id", "class"}, row...)
	}
	err = p.Flush()
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	cnt, err = tb.Count(nil)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := getGotExpErr("count", cnt, 6); err != nil {
		t.Errorf("%v", err)
		return
	}

	rows = [][]interface{}{
		{7, "user7", "class7"},
		{8, "user8", "class8"},
		{9, "user9", "class9"},
	}
	for _, row := range rows {
		p.InsertRow(nil, row...)
	}

	condF := func(v []string) bool {
		i, err := strconv.Atoi(v[0])
		if err != nil {
			return false
		}
		if i > 5 && i < 8 {
			return true
		}
		return false
	}

	r := p.Query(condF)
	for r.Next() {
		var id int
		var name string
		var class string
		if err := r.Scan(&id, &name, &class); err != nil {
			t.Errorf("%v", err)
			return
		}
		if id <= 5 || id >= 8 {
			t.Errorf("id=%d is not expected", id)
			return
		}
		if id == 6 {
			if err := getGotExpErr("name", name, ""); err != nil {
				t.Errorf("%v", err)
				return
			}
			if err := getGotExpErr("class", class, "class6"); err != nil {
				t.Errorf("%v", err)
				return
			}
		}
		if id == 7 {
			if err := getGotExpErr("name", name, "user7"); err != nil {
				t.Errorf("%v", err)
				return
			}
			if err := getGotExpErr("class", class, "class7"); err != nil {
				t.Errorf("%v", err)
				return
			}
		}
	}
}
