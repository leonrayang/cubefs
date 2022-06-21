package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/tool/cp"
)

var (
	configVersion = flag.Bool("v", false, "show version")
)

var opMap = map[string]int{"cp": 4, "sync": 4, "del": 3, "show": 2, "ls": 3, "mkdir": 3}

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

	argLen, ok := opMap[os.Args[1]]
	if !ok || len(os.Args) < argLen {
		cp.PrintUsage()
		os.Exit(1)
	}

	op := cp.CopyOp
	if os.Args[1] == "sync" {
		op = cp.SyncOp
	} else if os.Args[1] == "del" {
		op = cp.DelteOp
	}

	var srcDir, destDir string

	if argLen == 4 {
		srcDir = os.Args[2]
		destDir = os.Args[3]
	}

	if argLen == 3 {
		srcDir = os.Args[2]
	}

	cfg := cp.ParseConfig(srcDir, destDir, op)
	walker := cp.InitWalker(cfg)

	if os.Args[1] == "cp" || os.Args[1] == "sync" {
		walker.Execute()
	} else if os.Args[1] == "del" {
		walker.ExecuteDel()
	} else {
		walker.ExecuteCmd()
	}
}
