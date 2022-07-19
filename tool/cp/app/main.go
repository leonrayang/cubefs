package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/tool/cp"
	clog "github.com/chubaofs/chubaofs/util/log"

	"net/http"
	_ "net/http/pprof"
)

var (
	configVersion = flag.Bool("v", false, "show version")
	cnt           = flag.Int("fc", 10, "execute file concurrent cnt")
)

var opMap = map[string]int{"cp": 4, "sync": 4, "del": 3, "show": 2, "ls": 3, "mkdir": 3, "file": 4}

func main() {
	flag.Parse()

	if *configVersion {
		fmt.Print(proto.DumpVersion("cfs-tool"))
		os.Exit(0)
	}

	if len(os.Args) < 3 {
		cp.PrintUsage()
		os.Exit(1)
	}

	go func() {
		http.ListenAndServe(":18710", nil)
	}()

	argLen, ok := opMap[os.Args[1]]
	if !ok || len(os.Args) < argLen {
		cp.PrintUsage()
		os.Exit(1)
	}

	defer clog.LogFlush()

	if os.Args[1] != "file" {
		executeCmd(os.Args)
		return
	}

	executeByFile()
}

func executeByFile() {
	infos := readFile(os.Args[2])

	wg := sync.WaitGroup{}
	ch := make(chan []string, 128)
	for idx := 0; idx < *cnt; idx++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for e := range ch {
				executeCmd(e)
			}
		}()
	}

	for _, info := range infos {
		arr := []string{"file", os.Args[3], info.src}
		if info.dest != "" {
			arr = []string{"file", os.Args[3], info.src, info.dest}
		}
		ch <- arr
	}

	close(ch)
	wg.Wait()
}

type pathInfo struct {
	src  string
	dest string
}

func readFile(filename string) (infos []pathInfo) {
	fi, err := os.Open(filename)
	if err != nil {
		log.Fatalf("open file [%s] failed\n", filename)
	}
	defer fi.Close()

	br := bufio.NewReader(fi)
	for {
		line, _, err := br.ReadLine()
		if err == io.EOF {
			break
		}
		str := string(line)
		arr := strings.Split(str, "|")

		if len(arr) != 1 && len(arr) != 2 {
			log.Fatal(fmt.Sprintf("path file [%s] is not legal, should contain src, or src|dest", filename))
		}

		var src, dest string
		src = arr[0]
		if len(arr) == 2 {
			dest = arr[1]
		}

		info := pathInfo{
			src:  src,
			dest: dest,
		}

		infos = append(infos, info)
	}

	if len(infos) == 0 {
		log.Fatal(fmt.Sprintf("path file [%s] no vaild path list", filename))
		return
	}

	return
}

func executeCmd(args []string) {
	defer clog.LogWarnf("start execute %v", args)

	if len(args) < 3 {
		cp.PrintUsage()
		os.Exit(1)
	}

	argLen, ok := opMap[args[1]]
	if !ok || len(args) < argLen {
		cp.PrintUsage()
		log.Fatalf("args %v\n", args)
		os.Exit(1)
	}

	op := cp.CopyOp
	if args[1] == "sync" {
		op = cp.SyncOp
	} else if args[1] == "del" {
		op = cp.DelteOp
	}

	var srcDir, destDir string

	if argLen == 4 {
		srcDir = args[2]
		destDir = args[3]
	}

	if argLen == 3 {
		srcDir = args[2]
	}

	cfg := cp.ParseConfig(srcDir, destDir, op)
	walker := cp.InitWalker(cfg)

	if args[1] == "cp" || args[1] == "sync" {
		walker.Execute()
	} else if args[1] == "del" {
		walker.ExecuteDel()
	} else {
		walker.ExecuteCmd(args)
	}
}
