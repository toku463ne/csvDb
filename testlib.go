package csvdb

import (
	"fmt"
	"os"
)

func ensureTestDir(testname string) (string, error) {
	userDir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}

	rootDir := fmt.Sprintf("%s/goCsvDb/%s", userDir, testname)
	if _, err := os.Stat(rootDir); os.IsNotExist(err) {
		os.MkdirAll(rootDir, 0755)
	} else if os.IsExist(err) {
		os.RemoveAll(rootDir)
		os.MkdirAll(rootDir, 0755)
	}

	return rootDir, nil
}
