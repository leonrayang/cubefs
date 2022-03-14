package main

import (
	"flag"
	"log"
	"os"

	"github.com/chubaofs/chubaofs/tool/cp"
)

func main() {
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

	cp.WorkerCnt = *flag.Int("workerCnt", 0, "thread count to copy or sync files")
	cp.WalkCnt = *flag.Int("walkCnt", 0, "thread count to walk dirs")
	cp.QueueSize = *flag.Int("queueSize", 0, "use to store files, waiting to consume")
	cp.CfgPath = *flag.String("cfgPath", "", "config file path")
	flag.Parse()

	cfg := cp.ParseConfig(srcDir, destDir, op)
	walker := cp.InitWalker(cfg)
	walker.Execute()
}
