package csvdb

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/pkg/errors"
)

func newRows(cur *CsvCursor, condF func([]string) bool) *Rows {
	r := new(Rows)
	r.cur = cur
	r.condF = condF
	return r
}

func (r *Rows) Next() bool {
	for r.cur.Next() {
		values := r.cur.currReader.values
		if r.condF == nil {
			return true
		}
		if r.condF != nil && r.condF(values) {
			return true
		}
	}
	return false
}

func (r *Rows) Err() error {
	return r.cur.Err
}

// from
// https://github.com/golang/go/blob/master/src/database/sql/convert.go
func (r *Rows) Scan(dests ...interface{}) error {
	src := r.cur.currReader.values
	if len(src) != len(dests) {
		return errors.New("len of src and dests don't match")
	}

	errNilPtr := errors.New("destination pointer is nil")
	for i, dest := range dests {
		s := src[i]
		sv := reflect.ValueOf(s)
		dpv := reflect.ValueOf(dest)

		if dpv.Kind() != reflect.Ptr {
			return errors.New("destination not a pointer")
		}
		if dpv.IsNil() {
			return errNilPtr
		}

		dv := reflect.Indirect(dpv)

		if dv.Kind() == sv.Kind() && sv.Type().ConvertibleTo(dv.Type()) {
			dv.Set(sv.Convert(dv.Type()))
			continue
		}

		switch dv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			i64, err := strconv.ParseInt(s, 10, dv.Type().Bits())
			if err != nil {
				return err
			}
			dv.SetInt(i64)

		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if src == nil {
				return fmt.Errorf("converting NULL to %s is unsupported", dv.Kind())
			}
			u64, err := strconv.ParseUint(s, 10, dv.Type().Bits())
			if err != nil {
				return err
			}
			dv.SetUint(u64)

		case reflect.Float32, reflect.Float64:
			if src == nil {
				return fmt.Errorf("converting NULL to %s is unsupported", dv.Kind())
			}
			f64, err := strconv.ParseFloat(s, dv.Type().Bits())
			if err != nil {
				return err
			}
			dv.SetFloat(f64)

		case reflect.String:
			if src == nil {
				return fmt.Errorf("converting NULL to %s is unsupported", dv.Kind())
			}
			dv.SetString(s)
		}
	}
	return nil
}
