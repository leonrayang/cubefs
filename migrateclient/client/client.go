package client

import (
	"errors"
	"fmt"
	"github.com/cubefs/cubefs/migrateclient/config"
	cbfsProto "github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/liblog"
	sdkLog "github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/migrate/cubefssdk"
	"github.com/cubefs/cubefs/util/migrate/falconroute"
	"github.com/cubefs/cubefs/util/migrate/proto"
	"github.com/cubefs/cubefs/util/migrate/util"
	"go.uber.org/zap"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type MigrateClient struct {
	severAddr          string
	Logger             *zap.Logger
	NodeId             int32
	routerMap          map[string]*falconroute.Router
	stopCh             chan bool
	pendingTaskCh      chan proto.Task //待处理任务
	successTaskCh      chan proto.Task //成功的任务
	failedTaskCh       chan proto.Task //失败的任务
	extraTaskCh        chan proto.Task //无法处理的
	maxJobCnt          int32
	curJobCnt          int32
	copyGoroutineLimit int
	copyQueueLimit     int
	sdkManager         *cubefssdk.SdkManager
}

func NewMigrateClient(cfg *config.Config) *MigrateClient {
	cli := &MigrateClient{
		routerMap:          make(map[string]*falconroute.Router),
		severAddr:          cfg.Server,
		stopCh:             make(chan bool),
		pendingTaskCh:      make(chan proto.Task, 102400),
		successTaskCh:      make(chan proto.Task, 102400),
		failedTaskCh:       make(chan proto.Task, 102400),
		extraTaskCh:        make(chan proto.Task, 102400),
		maxJobCnt:          int32(cfg.JobCnt),
		curJobCnt:          0,
		copyGoroutineLimit: cfg.CopyGoroutineLimit,
		copyQueueLimit:     cfg.CopyQueueLimit,
	}
	cli.Logger, _ = liblog.NewZapLoggerWithLevel(cfg.LogCfg)
	for _, route := range cfg.FalconRoute {
		cli.Logger.Debug("Add Router", zap.Any("cluster", route.Name),
			zap.Any("Host", route.Addr), zap.Any("FalconAddr", cfg.FalconAddr))
		cli.routerMap[route.Name] = &falconroute.Router{Host: route.Addr, FalconAddr: cfg.FalconAddr}
	}
	go func() {
		if cfg.PprofPort != "" {
			cli.Logger.Info("Start pprof with port:", zap.Any("port", cfg.PprofPort))
			http.ListenAndServe(":"+cfg.PprofPort, nil)
		} else {
			pprofListener, err := net.Listen("tcp", ":0")
			if err != nil {
				cli.Logger.Error("Listen pprof failed", zap.Any("err", err))
				os.Exit(1)
			}
			cli.Logger.Info("Start pprof with port:", zap.Any("port", pprofListener.Addr().(*net.TCPAddr).Port))
			http.Serve(pprofListener, nil)
		}
	}()
	//初始化sdk的log
	var (
		level  sdkLog.Level
		logDir = cfg.SdkLogCfg.LogDir
	)
	if logDir == "" {
		logDir = "/home/service/var/sdkForMigrate"
	}

	if cfg.SdkLogCfg.LogLevel == "" {
		level = sdkLog.DebugLevel
	} else {
		level = strToLevel(cfg.SdkLogCfg.LogLevel)
	}
	//TODO:是否需要log？
	cbfsProto.InitBufferPool(int64(32768))
	sdkLog.InitLog(logDir, "migrate", level, nil)
	cli.sdkManager = cubefssdk.NewCubeFSSdkManager(cli.Logger)
	return cli
}

func (cli *MigrateClient) Register() error {
	url := fmt.Sprintf("http://%s%s", cli.severAddr, proto.RegisterUrl)

	logger := cli.Logger
	req := &proto.RegisterReq{JobCnt: int(cli.maxJobCnt)}
	logger.Debug("register req", zap.String("url", url), zap.Any("req", req))
	resp := &proto.RegisterResp{}
	err := util.DoPostWithJson(url, req, resp, logger)
	if err != nil {
		logger.Fatal("do register failed", zap.String("url", url), zap.Any("req", req), zap.Error(err))
		return err
	}

	if resp.NodeId <= 0 {
		logger.Fatal("register resp illegal", zap.Int32("nodeId", resp.NodeId))
		return errors.New(fmt.Sprintf("register failed, illegal nodeID %v", resp.NodeId))
	}

	logger.Info("register resp", zap.Any("resp", resp))

	cli.NodeId = resp.NodeId
	return nil
}

func (cli *MigrateClient) Close() {
	cli.Logger.Info("Close...")
	close(cli.stopCh)
}

func (cli *MigrateClient) Run() {
	cli.Logger.Info("start client success", zap.Int("pid", os.Getpid()), zap.Any("nodeID", cli.NodeId))
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		cli.execute()
	}()
	wg.Wait()
}

func (cli *MigrateClient) execute() {
	go cli.scheduleFetchTasks()
	busyRetry := 0 //无法处理时进行重试
	logger := cli.Logger
	for {
		select {
		case <-cli.stopCh:
			logger.Info("receive stop, exit run")
			return
		case task := <-cli.pendingTaskCh:
			for {
				//处理分配的任务
				if atomic.AddInt32(&cli.curJobCnt, 1) <= cli.maxJobCnt {
					go func(t proto.Task) {
						defer atomic.AddInt32(&cli.curJobCnt, -1)
						cli.executeTask(t)
					}(task)
					busyRetry = 0
					break
				}
				//阻塞太久的任务就处理,让master重新选择一个worker处理
				atomic.AddInt32(&cli.curJobCnt, -1)
				busyRetry++
				time.Sleep(time.Second)
				if busyRetry > 20 {
					busyRetry = 0
					select {
					case cli.extraTaskCh <- task:
						logger.Warn("no consume after 20s, send back to server", zap.String("task", task.String()))
					default:
						logger.Error("extraTaskCh is full, discard it", zap.String("task", task.String()))
					}
					cli.extraTaskCh <- task
					break
				}
			}
		}
	}
}

func (cli *MigrateClient) scheduleFetchTasks() {
	ticker := time.NewTicker(proto.FetchTaskInterval)
	logger := cli.Logger
	for {
		select {
		case <-ticker.C:
			//w.logger.Info("ScheduleGetTasks start to fetch tasks")
		case <-cli.stopCh:
			logger.Warn("ScheduleGetTasks exit with stop signal")
			return
		}
		//可用拷贝数目
		////idleCnt := cli.maxJobCnt - int32(len(cli.pendingTaskCh))
		//if idleCnt < 0 {
		//	idleCnt = 0
		//}
		idleCnt := cli.maxJobCnt - atomic.LoadInt32(&cli.curJobCnt)
		successTasks := cli.getSuccessTasks()
		failedTasks := cli.getFailedTasks()
		extraTasks := cli.getExtraTasks()
		//上报成功，和失败的任务，获取新的任务
		resp := cli.fetchTasks(int(idleCnt), successTasks, failedTasks, extraTasks)
		if len(resp.Tasks) > 0 {
			cli.handleTasks(resp.Tasks)
		}
	}
}

func (cli *MigrateClient) getSuccessTasks() []proto.Task {
	tasks := make([]proto.Task, 0)
	for {
		select {
		case task := <-cli.successTaskCh:
			tasks = append(tasks, task)
		default:
			return tasks
		}
	}
}

func (cli *MigrateClient) getFailedTasks() []proto.Task {
	tasks := make([]proto.Task, 0)
	for {
		select {
		case task := <-cli.failedTaskCh:
			tasks = append(tasks, task)
		default:
			return tasks
		}
	}
}

func (cli *MigrateClient) getExtraTasks() []proto.Task {
	tasks := make([]proto.Task, 0)
	for {
		select {
		case task := <-cli.extraTaskCh:
			tasks = append(tasks, task)
		default:
			return tasks
		}
	}
}

func (cli *MigrateClient) handleTasks(tasks []proto.Task) {
	for _, task := range tasks {
		cli.pendingTaskCh <- task
	}
}

func (cli *MigrateClient) fetchTasks(idleCnt int, succTasks, failTasks, extraTasks []proto.Task) *proto.FetchTasksResp {
	url := fmt.Sprintf("http://%s%s", cli.severAddr, proto.FetchTasksUrl)

	req := &proto.FetchTasksReq{
		NodeId:     cli.NodeId,
		IdleCnt:    idleCnt,
		SuccTasks:  succTasks,
		FailTasks:  failTasks,
		ExtraTasks: extraTasks,
	}

	logger := cli.Logger.With()

	resp := &proto.FetchTasksResp{}
	err := util.DoPostWithJson(url, req, resp, logger)
	if err != nil {
		if strings.Contains(err.Error(), "MigrateClient not exis") {
			logger.Fatal("please restart")
		}
		logger.Error("do getTasks failed", zap.String("url", url), zap.Any("req", req), zap.Error(err))
		return &proto.FetchTasksResp{}
	}
	return resp
}

func (cli *MigrateClient) executeTask(task proto.Task) {
	var err error
	defer func() {
		if err == nil {
			cli.successTaskCh <- task
		} else {
			task.ErrorMsg = err.Error()
			cli.failedTaskCh <- task
		}
	}()
	if task.WorkMode == proto.JobMove {
		err = cli.doMoveOperation(task)
	}

	if task.WorkMode == proto.JobCopyFile {
		err = cli.doCopySingleFileOperation(task)
	}

	if task.WorkMode == proto.JobCopyDir {
		err, task.MigrateSize = cli.doCopyDirOperation(task)
	}

	if task.WorkMode == proto.JobMigrateDir {
		err, task.MigrateSize = cli.doMigrateDirOperation(task)
	}
}

func strToLevel(s string) (level sdkLog.Level) {
	switch strings.ToLower(s) {
	case "debug":
		level = sdkLog.DebugLevel
	case "info":
		level = sdkLog.InfoLevel
	case "warn":
		level = sdkLog.WarnLevel
	case "error":
		level = sdkLog.ErrorLevel
	case "fatal":
		level = sdkLog.FatalLevel
	case "critical":
		level = sdkLog.CriticalLevel
	default:
		level = sdkLog.InfoLevel
	}
	return
}
