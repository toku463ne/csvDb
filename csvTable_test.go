package csvdb

import (
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
		[]string{"id", "name"}, false)
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
	}, cWriteModeAppend); err != nil {
		t.Errorf("Error inserting table test : %v", err)
	}
	cur := p1.OpenCur()

	for cur.next() {
		if cur.err != nil {
			t.Errorf("Error getting data : %v", cur.err)
		}
		v := cur.values()
		if v[0] != "1" {
			t.Errorf("data error want=%s got=%s", "1", v[0])
		}
		if v[1] != "test1" {
			t.Errorf("data error want=%s got=%s", "test1", v[1])
		}
		break
	}

	cur.close()

	p2, err := tb.GetPartition("002")
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := p2.InsertRows([][]string{
		{"3", "test3"},
		{"4", "test4"},
	}, cWriteModeAppend); err != nil {
		t.Errorf("Error inserting table test : %v", err)
	}

	cur = p2.OpenCur()
	for cur.next() {
		if cur.err != nil {
			t.Errorf("Error getting data : %v", cur.err)
		}
		v := cur.values()
		if v[0] != "3" {
			t.Errorf("data error want=%s got=%s", "3", v[0])
		}
		if v[1] != "test3" {
			t.Errorf("data error want=%s got=%s", "test3", v[1])
		}
		break
	}

	cur.close()

	//found, err := p2.Query(map[string]string{"id": "3"})
	found, err := p2.Query(func(v []string) bool { return v[0] == "3" })

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
		[]string{"id", "name"}, false)
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
		[]string{"id", "name"}, true)
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
	}, cWriteModeAppend); err != nil {
		t.Errorf("Error inserting table test : %v", err)
	}

	path := tb.getPartitionPath("002")
	tokens := strings.Split(path, ".")
	ext := tokens[len(tokens)-1]
	if ext != "gz" {
		t.Errorf("Not a gzip : %v", ext)
		return
	}

	//found, err := p2.Query(map[string]string{"id": "3"})
	found, err := p2.Query(func(v []string) bool { return v[0] == "3" })
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
