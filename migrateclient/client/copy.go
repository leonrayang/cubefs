package client

import (
	"context"
	"errors"
	"fmt"
	"github.com/cubefs/cubefs/util/liblog"
	"github.com/cubefs/cubefs/util/migrate/cubefssdk"
	"github.com/cubefs/cubefs/util/migrate/falconroute"
	"github.com/cubefs/cubefs/util/migrate/proto"
	"go.uber.org/zap"
	gopath "path"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	TinyFileSize = 1024 * 1024
)

func (cli *MigrateClient) doCopySingleFileOperation(task proto.Task) error {
	var (
		srcRouter falconroute.RouteInfo
		dstRouter falconroute.RouteInfo
		router    *falconroute.Router
		ok        bool
		clusterId = task.SourceCluster
		srcPath   = task.Source
		dstPath   = task.Target
		logger    = cli.Logger
		err       error
	)
	virSrcPath := falconroute.GetVirtualPathFromAbsDir(srcPath)
	virDstPath := falconroute.GetVirtualPathFromAbsDir(dstPath)
	//server做了路由检测，这里可以省略大部分步骤
	if router, ok = cli.routerMap[clusterId]; !ok {
		logger.Error("route not found in config", zap.String("clusterId", clusterId))
		return errors.New(fmt.Sprintf("route not found in config %s", clusterId))
	}

	srcRouter, err = router.GetRoute(virSrcPath, logger)
	if err != nil {
		logger.Error("get route failed", zap.Any("virSrcPath", virSrcPath), zap.Error(err))
		return errors.New(fmt.Sprintf("get route %s failed:%s ", virSrcPath, err.Error()))
	}

	dstRouter, err = router.GetRoute(virDstPath, logger)
	if err != nil {
		logger.Error("get router failed", zap.Any("virDstPath", virDstPath), zap.Error(err))
		return errors.New(fmt.Sprintf("get router %s failed:%s ", virDstPath, err.Error()))
	}

	if err := execCopyFileCommand(cli.sdkManager, srcPath, dstPath, srcRouter.Pool, srcRouter.Endpoint,
		dstRouter.Pool, dstRouter.Endpoint, logger, task.TaskId, cli.CheckDebugEnable, task.OverWrite, cli.localAddr); err == nil {
		return nil
	} else {
		return errors.New(fmt.Sprintf("Execute copy single file operation failed: %s", err.Error()))
	}
}

func execCopyFileCommand(manager *cubefssdk.SdkManager, source, target, srcVol, srcEndpoint, dstVol,
	dstEndpoint string, logger *zap.Logger, taskId string, debugFunc cubefssdk.EnableDebugMsg, overWrite bool, addr string) (err error) {
	//无论是否为文件或者文件夹，执行的mv操作一致
	logger.Debug("execCopyFileCommand", zap.Any("src", source), zap.Any("dst", target), zap.Any("srcVol", srcVol), zap.Any("dstVol", dstVol))
	cli, err := manager.GetCubeFSSdk(srcVol, srcEndpoint)
	if err != nil {
		return err
	}
	cliDst, err := manager.GetCubeFSSdk(dstVol, dstEndpoint)
	if err != nil {
		return err
	}
	return cli.CopyFileToDir(source, target, cliDst, taskId, debugFunc, nil, overWrite, addr)
}

func (cli *MigrateClient) doCopyDirOperation(task proto.Task) (error, uint64) {
	var (
		srcRouter falconroute.RouteInfo
		dstRouter falconroute.RouteInfo
		router    *falconroute.Router
		ok        bool
		clusterId = task.SourceCluster
		srcPath   = task.Source
		dstPath   = task.Target
		logger    = cli.Logger
		err       error
	)
	virSrcPath := falconroute.GetVirtualPathFromAbsDir(srcPath)
	virDstPath := falconroute.GetVirtualPathFromAbsDir(dstPath)
	//server做了路由检测，这里可以省略大部分步骤
	if router, ok = cli.routerMap[clusterId]; !ok {
		logger.Error("route not found in config", zap.String("clusterId", clusterId))
		return errors.New(fmt.Sprintf("route not found in config %s", clusterId)), 0
	}

	srcRouter, err = router.GetRoute(virSrcPath, logger)
	if err != nil {
		logger.Error("get route failed", zap.Any("virSrcPath", virSrcPath), zap.Error(err))
		return errors.New(fmt.Sprintf("get route %s failed:%s ", virSrcPath, err.Error())), 0
	}

	dstRouter, err = router.GetRoute(virDstPath, logger)
	if err != nil {
		logger.Error("get router failed", zap.Any("virDstPath", virDstPath), zap.Error(err))
		return errors.New(fmt.Sprintf("get router %s failed:%s ", virDstPath, err.Error())), 0
	}
	cfg := &liblog.Config{
		ScreenOutput: false,
		LogFile:      gopath.Join(buildTaskCachePath(task.TaskId, gopath.Dir(cli.logCfg.LogFile)), fmt.Sprintf("%v.list", task.TaskId)),
		LogLevel:     "debug",
		MaxSizeMB:    1024,
		MaxBackups:   5, //保留的日志文件个数
		MaxAge:       7,
		Compress:     false,
	}
	copyLogger, _ := liblog.NewZapLoggerWithLevel(cfg)

	if err, totalSize := execCopyDirCommand(cli.sdkManager, srcPath, dstPath, srcRouter.Pool, srcRouter.Endpoint, dstRouter.Pool, dstRouter.Endpoint,
		logger, cli.copyGoroutineLimit, cli.copyQueueLimit, task.TaskId, cli.tinyFactor,
		cli.CheckDebugEnable, copyLogger, task.OverWrite, cli.localAddr); err == nil {
		return nil, totalSize
	} else {
		return errors.New(fmt.Sprintf("Execute copy single file operation failed: %s", err.Error())), totalSize
	}
}

const TinyCopyFileLimit = 1000

// 增加耗时
func execCopyDirCommand(manager *cubefssdk.SdkManager, source, target, srcVol, srcEndpoint, dstVol, dstEndpoint string,
	logger *zap.Logger, goroutineLimit, taskLimit int, taskId string,
	tinyFactor int, debugFunc cubefssdk.EnableDebugMsg, copyLogger *zap.Logger, overWrite bool, addr string) (err error, copySuccess uint64) {
	//无论是否为文件或者文件夹，执行的mv操作一致
	logger.Debug("execCopyDirCommand", zap.Any("src", source), zap.Any("from vol", srcVol),
		zap.Any("dst", target), zap.Any("to vol", dstVol), zap.Any("TaskId", taskId))
	copySuccess = 0
	var (
		succCnt int32
	)
	//start := time.Now()
	//defer logger.Debug("execCopyDirCommand completed", zap.Any("TaskId", taskId),
	//	zap.Any("consumeTime", time.Now().Sub(start).String()))
	//logger.Debug("execCopyDirCommand get src SDK", zap.Any("TaskId", taskId))
	logger.Debug("execCopyDirCommand get src sdk", zap.Any("TaskId", taskId))
	srcCli, err := manager.GetCubeFSSdk(srcVol, srcEndpoint)
	if err != nil {
		return err, 0
	}
	//logger.Debug("execCopyDirCommand get dst SDK", zap.Any("TaskId", taskId))
	logger.Debug("execCopyDirCommand  get dst sdk", zap.Any("TaskId", taskId))
	dstCli, err := manager.GetCubeFSSdk(dstVol, dstEndpoint)
	if err != nil {
		return err, 0
	}
	//这里可以判断文件数目了
	logger.Debug("execCopyDirCommand ReadDir start", zap.Any("TaskId", taskId))
	children, err := srcCli.ReadDir(source)
	if err != nil {
		logger.Debug("ReadDir failed", zap.Any("TaskId", taskId), zap.Any("source", source), zap.Any("err", err))
		return errors.New(fmt.Sprintf("ReadDir failed, dir %s[%s]", source, err.Error())), 0
	}
	logger.Debug("execCopyDirCommand ReadDir finish", zap.Any("TaskId", taskId))
	errCh := make(chan error, 10)
	taskCh := make(chan CopySubTask, taskLimit)
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var errMsg string
	var modifyTimeErrCached int32
	atomic.StoreInt32(&modifyTimeErrCached, 0)

	copyTaskFunc := func() {

		//logger.Debug("execCopyDirCommand new goroutine", zap.Any("TaskId", taskId))
		defer func() {
			wg.Done()
			//logger.Debug("execCopyDirCommand  goroutine close", zap.Any("TaskId", taskId))
		}()
		for task := range taskCh {
			select {
			case <-ctx.Done():
				return // Error somewhere, terminate
			default: // Default is must to avoid blocking
			}
			var taskErr error
			//logger.Warn("Try copy", zap.Any("source", task.source))
			taskErr = srcCli.CopyFileToDir(task.source, task.target, dstCli, taskId, debugFunc,
				copyLogger, overWrite, addr)
			if taskErr != nil {
				if strings.Contains(taskErr.Error(), "modify time") {
					if atomic.LoadInt32(&modifyTimeErrCached) == 0 {
						atomic.StoreInt32(&modifyTimeErrCached, 1)
						select {
						case errCh <- taskErr:
							logger.Warn("Copy failed", zap.Any("TaskId", taskId), zap.Any("err", taskErr))
						default:
							//logger.Warn("to many errors", zap.Any("TaskId", taskId), zap.Any("err", taskErr))
						}
						//logger.Warn("Copy failed", zap.Any("TaskId", taskId), zap.Any("err", taskErr))
					}
				} else {
					select {
					case errCh <- taskErr:
						logger.Warn("Copy failed", zap.Any("TaskId", taskId), zap.Any("err", taskErr))
					default:
						//logger.Warn("to many errors", zap.Any("TaskId", taskId), zap.Any("err", taskErr))
					}
				}
			} else {
				atomic.AddInt32(&succCnt, 1)
				atomic.AddUint64(&copySuccess, task.size)
			}
		}
	}
	//目前无法启用
	if len(children) >= TinyCopyFileLimit {
		goroutineLimit = tinyFactor * goroutineLimit
		//发布前删除
		logger.Warn("work as tiny", zap.Any("TaskId", taskId))
	}
	for i := 0; i < goroutineLimit; i++ {
		wg.Add(1)
		go copyTaskFunc()
	}
	for _, child := range children {
		if srcCli.IsDirByDentry(child) {
			continue
		}
		childSize, err := srcCli.GetFileSize(gopath.Join(source, child.Name))
		if err != nil {
			sizeErr := errors.New(fmt.Sprintf("Failed to get %v size:%v", gopath.Join(source, child.Name), err.Error()))
			select {
			case errCh <- sizeErr:
				logger.Warn("Failed to get file size", zap.Any("child", gopath.Join(source, child.Name)), zap.Any("err", err))
			default:
				//logger.Warn("to many errors", zap.Any("TaskId", taskId), zap.Any("err", taskErr))
			}
			continue
		}
		//logger.Debug("execCopyDirCommand  new copy task ", zap.Any("TaskId", taskId),
		//	zap.Any("source", gopath.Join(source, child.Name)), zap.Any("target", target))
		taskCh <- CopySubTask{source: gopath.Join(source, child.Name), target: target, size: childSize}
	}
	logger.Debug("execCopyDirCommand all file is send", zap.Any("TaskId", taskId))
	close(taskCh)
	wg.Wait()
	close(errCh)
	logger.Debug("execCopyDirCommand all groutine is closed", zap.Any("TaskId", taskId))
	//这里遍历error chan即可
	count := 0
	for subErr := range errCh {
		errMsg += fmt.Sprintf("%v;", subErr.Error())
		count++
	}
	if count == 10 {
		errMsg = fmt.Sprintf("[to many errors]%v", errMsg)
	}
	logger.Debug("execCopyDirCommand  copy  ", zap.Any("succCnt", succCnt), zap.Any("copySuccess", copySuccess), zap.Any("TaskId", taskId))
	if len(errMsg) == 0 {
		return nil, copySuccess
	} else {
		return errors.New(fmt.Sprintf("%v", errMsg)), copySuccess
	}
}
func RemoveDuplicateSubstrings(input string) string {
	substrings := strings.Split(input, ";")
	uniqueSubstrings := make(map[string]bool)
	var result string

	for _, substr := range substrings {
		if !uniqueSubstrings[substr] {
			uniqueSubstrings[substr] = true
			result += substr
			result += ";"
		}
	}

	return result
}

type CopySubTask struct {
	source string
	target string
	size   uint64
}
