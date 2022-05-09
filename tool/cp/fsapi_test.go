package cp

import (
	"os"
	"syscall"
	"testing"
)

func TestOsStatFile(t *testing.T) {
	// stat no exist file
	f := &OsFs{}
	file := "testStat"
	_, err := f.statFile(file, 0)
	assertTrue(t, err == os.ErrNotExist)
}

func TestCheckMode(t *testing.T) {
	stat := &syscall.Stat_t{
		Mode: 0777,
		Uid:  0,
		Gid:  0,
	}

	getUser()
	u.Uid = "1010"
	u.Gid = "1010"

	var err error
	err = checkMode(stat, read)
	assertTrue(t, err == nil)

	err = checkMode(stat, write)
	assertTrue(t, err == nil)

	stat.Mode = 0775
	err = checkMode(stat, read)
	assertTrue(t, err == nil)

	err = checkMode(stat, write)
	assertTrue(t, err != nil)

	stat.Mode = 0774
	err = checkMode(stat, read)
	assertTrue(t, err != nil)

	u.Gid = "0"
	err = checkMode(stat, read)
	assertTrue(t, err == nil)

	err = checkMode(stat, write)
	assertTrue(t, err == nil)
}
