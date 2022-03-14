package main

import (
	"flag"
	"fmt"
	"log"
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

	if len(os.Args) < 4 {
		log.Println("usage: ./cfs-tool op src dest")
		os.Exit(1)
	}

	if os.Args[1] != "cp" && os.Args[1] != "sync" {
		log.Fatal("only support cp & sync op")
	}

	op := cp.CopyOp
	if os.Args[1] == "sync" {
		op = cp.SyncOp
	}

	srcDir := os.Args[2]
	destDir := os.Args[3]

	cfg := cp.ParseConfig(srcDir, destDir, op)
	walker := cp.InitWalker(cfg)
	walker.Execute()
}
