package cp

import (
	"os"
	"testing"
)

func TestOsStatFile(t *testing.T) {
	// stat no exist file
	f := &OsFs{}
	file := "testStat"
	_, err := f.statFile(file, 0)
	assertTrue(t, err == os.ErrNotExist)
}
