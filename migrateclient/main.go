package main

import (
	"fmt"
	"github.com/cubefs/cubefs/migrateclient/client"
	"github.com/cubefs/cubefs/migrateclient/config"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"go.uber.org/zap"
	syslog "log"
	"os"
	"os/signal"
	"path"
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
	cli := client.NewMigrateClient(cfg)
	if err = cli.Register(); err != nil {
		fmt.Printf("register failed %v\n", err.Error())
		os.Exit(1)
	}
	go cli.StartHttpServer()
	proto.InitBufferPool(32768)
	signalC := make(chan os.Signal)
	signal.Notify(signalC, syscall.SIGSEGV, syscall.SIGABRT, syscall.SIGTERM)
	signal.Ignore(syscall.SIGPIPE)
	signal.Ignore(syscall.SIGHUP)

	go func() {
		sig := <-signalC
		cli.Logger.Warn("receive signal, exit", zap.Any("sig", sig))
		cli.Close()
	}()
	outputFilePath := path.Join(path.Dir(cfg.LogCfg.LogFile), "output")
	outputFile, err := os.OpenFile(outputFilePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		err = errors.NewErrorf("Open output file failed: %v\n", err)
		fmt.Println(err)
		os.Exit(1)
	}
	defer func() {
		outputFile.Sync()
		outputFile.Close()
	}()
	syslog.SetOutput(outputFile)
	syslog.Println("output ready")
	cli.Run()
}
