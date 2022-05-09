package cp

import (
	"fmt"
	"log"
	"os"
	"os/user"
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var once = sync.Once{}
var u *user.User

func getUser() *user.User {
	// var u *user.User
	var err error

	once.Do(func() {
		u, err = user.Current()
		if err != nil {
			log.Fatalf("get current user failed, err %s", err.Error())
		}
		fmt.Sprintln("get user info")
	})

	return u
}

func PrintUsage() {
	fmt.Printf("use-age: %s cp localpath cid://path\n", os.Args[0])
	fmt.Printf("use-age: %s cp cid://path localpath\n", os.Args[0])
	fmt.Printf("use-age: %s sync localpath cid://path\n", os.Args[0])
	fmt.Printf("use-age: %s sync cid://path localpath\n", os.Args[0])

	fmt.Printf("use-age: %s show all\n", os.Args[0])
	fmt.Printf("use-age: %s show idc idc-name\n", os.Args[0])
	fmt.Printf("use-age: %s show cluster cluster-name\n", os.Args[0])

	fmt.Printf("use-age: %s ls cid://path\n", os.Args[0])
}

func (w *Walker) ExecuteCmd() {
	switch os.Args[1] {
	case "show":
		w.showCmd()
	case "ls":
		w.lsCmd()
	default:
		PrintUsage()
	}
}

func (w *Walker) showCmd() {
	cfg := &config{}
	err := loadConfig(cfg, getCfgPath())
	if err != nil {
		return
	}

	cfgs := make([]clusterCfg, 0)
	if os.Args[2] == "idc" && os.Args[3] != "" {
		idc := os.Args[3]
		for _, c := range cfg.ClusterCfg {
			if strings.Contains(c.Idc, idc) {
				cfgs = append(cfgs, c)
			}
		}
	} else if os.Args[2] == "cluster" && os.Args[3] != "" {
		cluster := os.Args[3]
		for _, c := range cfg.ClusterCfg {
			if strings.Contains(c.Cluster, cluster) {
				cfgs = append(cfgs, c)
			}
		}
	} else if os.Args[2] == "all" {
		cfgs = cfg.ClusterCfg
	} else {
		PrintUsage()
		os.Exit(1)
	}

	maxLen := 10
	for _, c := range cfgs {
		if len(c.Cluster) > maxLen {
			maxLen = len(c.Cluster)
		}
		if len(c.ClusterId) > maxLen {
			maxLen = len(c.ClusterId)
		}
		if len(c.Volname) > maxLen {
			maxLen = len(c.Volname)
		}
		if len(c.Idc) > maxLen {
			maxLen = len(c.Idc)
		}
	}

	maxLen += 5
	maxLenStr := strconv.Itoa(maxLen)

	printFmt := "\t%-" + maxLenStr + "s%-" + maxLenStr + "s%-" + maxLenStr + "s%-" + maxLenStr + "s%-" + maxLenStr + "s\n"

	fmt.Printf(printFmt, "cid", "volume", "cluster", "idc", "desc")
	for _, c := range cfgs {
		fmt.Printf(printFmt, c.ClusterId, c.Volname, c.Cluster, c.Idc, c.Desc)
	}
}

func (w *Walker) lsCmd() {
	if os.Args[2] == "" {
		PrintUsage()
		os.Exit(1)
	}

	filepath := w.SrcDir.dir

	ino, err := w.srcApi.getParentInoByPath(filepath)
	if err != nil {
		panic(fmt.Sprintf("look up path %s err %s", filepath, err.Error()))
	}

	// log.Println("get inode", ino)

	fmt.Printf("%-12s%-10s%-10s%-20s%-24s%-10s\n", "mode", "user", "group", "size", "time", "name")

	printFile := func(st *syscall.Stat_t, name string) {
		mode := fileMode(st.Mode)
		timeStr := time.Unix(st.Mtim.Unix()).Format("2006-01-02 15:04:05")

		userInfo, err := user.LookupId(fmt.Sprintf("%d", st.Uid))
		if err != nil {
			fmt.Printf("can't find target uid %d, err %s\n", st.Uid, err.Error())
			userInfo = &user.User{Username: "unknown"}
		}

		// fmt.Printf("user name %s uid %s, uname %s\n", userInfo.Name, userInfo.Uid, userInfo.Username)

		group, err := user.LookupGroupId(fmt.Sprintf("%d", st.Gid))
		if err != nil {
			fmt.Printf("can't find target gid %d, err %s\n", st.Gid, err.Error())
			group = &user.Group{Name: "unknown"}
		}

		fmt.Printf("%-12s%-10s%-10s%-20d%-24s%-10s\n", mode.String(), userInfo.Username, group.Name, st.Size, timeStr, name)
		// fmt.Printf("%-12s%-10d%-10d%-20d%-24s%-10s\n", mode.String(), st.Uid, st.Gid, st.Size, timeStr, name)
	}

	stat, err := w.srcApi.statFile(filepath, ino)
	if err != nil {
		log.Fatalf("stat filepath %s err %s", filepath, err.Error())
	}

	// log.Println("get stat", stat)

	mode := fileMode(stat.Mode)
	if !mode.IsDir() {
		printFile(stat, filepath)
		return
	}

	items, err := w.srcApi.readDir(filepath, stat.Ino)
	if err != nil {
		log.Fatalf("read dir failed, dir %s, err %s", filepath, err.Error())
	}

	parIno := stat.Ino
	dirCh := make(chan DirItem, len(items))
	wg := sync.WaitGroup{}

	for idx := 0; idx < w.workerCnt; idx++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range dirCh {
				subPath := path.Join(filepath, item.Name)
				st, err := w.srcApi.statFile(subPath, parIno)
				if err != nil {
					log.Fatalf("stat path %s err %s", subPath, err.Error())
				}
				printFile(st, item.Name)
			}
		}()
	}

	for idx := range items {
		dirCh <- items[idx]
	}

	close(dirCh)
	wg.Wait()
}
