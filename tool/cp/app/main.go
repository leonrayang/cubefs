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

	if os.Args[1] != "cp" && os.Args[1] != "sync" && os.Args[1] != "show" && os.Args[1] != "ls" &&
		os.Args[1] != "del" {
		cp.PrintUsage()
		os.Exit(1)
	}

	if (os.Args[1] == "cp" || os.Args[1] == "sync") && len(os.Args) < 4 {
		cp.PrintUsage()
		os.Exit(1)
	}

	if os.Args[1] == "del" && len(os.Args) < 3 {
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

	if os.Args[1] == "cp" || os.Args[1] == "sync" {
		srcDir = os.Args[2]
		destDir = os.Args[3]
	}

	if os.Args[1] == "ls" || os.Args[1] == "del" {
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
