package csvdb

import (
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/pkg/errors"
)

func extError(err error, msg string) error {
	return errors.WithStack(errors.Wrapf(err, msg))
}

func _pivot(a []int64, i, j int) int {
	k := i + 1
	for k <= j && a[i] == a[k] {
		k++
	}
	if k > j {
		return -1
	}
	if a[i] >= a[k] {
		return i
	}
	return k
}

func _partition(a []int64, s []string, i, j int, x int64) int {
	l := i
	r := j

	for l <= r {
		for l <= j && a[l] < x {
			l++
		}
		for r >= i && a[r] >= x {
			r--
		}
		if l > r {
			break
		}
		t := a[l]
		s1 := s[l]
		a[l] = a[r]
		s[l] = s[r]
		a[r] = t
		s[r] = s1
		l++
		r--
	}
	return l
}

func quickSort(a []int64, s []string, i, j int) {
	if i == j {
		return
	}
	p := _pivot(a, i, j)
	if p != -1 {
		k := _partition(a, s, i, j, a[p])
		quickSort(a, s, i, k-1)
		quickSort(a, s, k, j)
	}
}

func _pivotFloatInt(a []float64, i, j int) int {
	k := i + 1
	for k <= j && a[i] == a[k] {
		k++
	}
	if k > j {
		return -1
	}
	if a[i] >= a[k] {
		return i
	}
	return k
}

func _partitionFloatInt(a []float64, s []int, i, j int, x float64) int {
	l := i
	r := j

	for l <= r {
		for l <= j && a[l] < x {
			l++
		}
		for r >= i && a[r] >= x {
			r--
		}
		if l > r {
			break
		}
		t := a[l]
		s1 := s[l]
		a[l] = a[r]
		s[l] = s[r]
		a[r] = t
		s[r] = s1
		l++
		r--
	}
	return l
}

func quickSortFloatInt(a []float64, s []int, i, j int) {
	if i == j {
		return
	}
	p := _pivotFloatInt(a, i, j)
	if p != -1 {
		k := _partitionFloatInt(a, s, i, j, a[p])
		quickSortFloatInt(a, s, i, k-1)
		quickSortFloatInt(a, s, k, j)
	}
}

func timespecToTime(ts syscall.Timespec) time.Time {
	return time.Unix(int64(ts.Sec), int64(ts.Nsec))
}

func getSortedGlob(pathRegex string) ([]int64, []string) {
	fileNames, _ := filepath.Glob(pathRegex)
	if fileNames == nil {
		return nil, nil
	}
	filesEpoch := make([]int64, len(fileNames))

	for i, fileName := range fileNames {
		file, _ := os.Stat(fileName)
		//ts, _ := times.Stat(fileName)
		t := file.ModTime()
		//t := ts.BirthTime()
		filesEpoch[i] = t.Unix()
	}

	quickSort(filesEpoch, fileNames, 0, len(fileNames)-1)
	return filesEpoch, fileNames
}

func pathExist(path string) bool {
	if _, err := os.Stat(path); err != nil {
		return false
	}
	return true
}
