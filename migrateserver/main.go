package main

import (
	"fmt"
	"github.com/cubefs/cubefs/migrateserver/config"
	"github.com/cubefs/cubefs/migrateserver/server"
	"github.com/cubefs/cubefs/proto"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("need input cfg path")
		os.Exit(1)
	}
	cfg, err := config.ParseConfig(os.Args[1])
	if err != nil {
		fmt.Printf("load cfg failed %v\n", err.Error())
		os.Exit(1)
	}
	proto.InitBufferPool(32768)
	svr := server.NewMigrateServer(cfg)
	go svr.StartHttpServer()
	signalC := make(chan os.Signal)
	signal.Notify(signalC, syscall.SIGSEGV, syscall.SIGABRT, syscall.SIGTERM)
	signal.Ignore(syscall.SIGPIPE)
	signal.Ignore(syscall.SIGHUP)
	go func() {
		sig := <-signalC
		svr.Logger.Warn("recive signal, exit", zap.Any("sig", sig))
		svr.Close()
	}()
	svr.Run()
}
