package server

import (
	"errors"
	"fmt"
	"github.com/cubefs/cubefs/util/migrate/falconroute"
	"github.com/cubefs/cubefs/util/migrate/proto"
	"go.uber.org/zap"
	"strings"
)

func (svr *MigrateServer) copyFilesInCluster(srcPath, dstPath, clusterId string, overwrite bool) (err error, id string) {
	var (
		srcRouter falconroute.RouteInfo
		dstRouter falconroute.RouteInfo
		router    *falconroute.Router
		ok        bool
		logger    = svr.Logger
	)
	//虚拟路径只是用来查路由的，并不是完整路劲
	virSrcPath := falconroute.GetVirtualPathFromAbsDir(srcPath)
	if virSrcPath == "" {
		logger.Error("Path is not legal", zap.String("srcPath", srcPath))
		return errors.New(fmt.Sprintf("srcPath %s is not legal", srcPath)), ""
	}
	virDstPath := falconroute.GetVirtualPathFromAbsDir(dstPath)
	if virDstPath == "" {
		logger.Error("Path is not legal", zap.String("dstPath", dstPath))
		return errors.New(fmt.Sprintf("dstPath %s is not legal", clusterId)), ""
	}
	//获取路由
	if router, ok = svr.routerMap[clusterId]; !ok {
		logger.Error("route not found in config", zap.String("clusterId", clusterId))
		return errors.New(fmt.Sprintf("route not found in config %s", clusterId)), ""
	}
	srcRouter, err = router.GetRoute(virSrcPath, logger)
	if err != nil {
		logger.Error("get route failed", zap.Any("virSrcPath", virSrcPath), zap.Error(err))
		return errors.New(fmt.Sprintf("get route %s failed:%s ", virSrcPath, err.Error())), ""
	}
	if srcRouter.GroupType != falconroute.GroupTypeCfs {
		logger.Error("src route info not in cfs", zap.Any("src", srcRouter))
		return errors.New(fmt.Sprintf("src route info not in cfs: %s", srcRouter.VirtualPath)), ""
	}
	dstRouter, err = router.GetRoute(virDstPath, logger)
	if err != nil {
		logger.Error("get router failed", zap.Any("virDstPath", virDstPath), zap.Error(err))
		return errors.New(fmt.Sprintf("get router %s failed:%s ", virDstPath, err.Error())), ""
	}
	if dstRouter.GroupType != falconroute.GroupTypeCfs {
		logger.Error("dst route info not in cfs", zap.Any("dst", dstRouter))
		return errors.New(fmt.Sprintf("dst route info not in cfs: %s", dstRouter.VirtualPath)), ""
	}

	cli, err := svr.sdkManager.GetCubeFSSdk(srcRouter.Pool, srcRouter.Endpoint)
	if err != nil {
		return err, ""
	}
	cliDst, err := svr.sdkManager.GetCubeFSSdk(dstRouter.Pool, dstRouter.Endpoint)
	if err != nil {
		return err, ""
	}
	var mode = proto.JobInvalid
	//判断是否为拷贝文件(包括软连接)
	ret, err := cli.IsFileOrSymlink(srcPath)
	if err != nil {
		return err, ""
	}

	if ret {
		mode = proto.JobCopyFile
	}
	ret, _ = cli.IsDir(srcPath)
	//判断是否为拷贝文件
	if ret {
		mode = proto.JobCopyDir
	}
	if mode == proto.JobInvalid {
		return errors.New(fmt.Sprintf("unsupport file type:%s ", srcPath)), ""
	}
	//不支持重命名，所以只能拷贝在某个目录下。包含检测父目录在不在
	ret, err = cliDst.IsDir(dstPath)
	if err != nil {
		return err, ""
	}
	if !ret {
		return errors.New(fmt.Sprintf("only support copy file to folder:%s is not dir", dstPath)), ""
	}
	// 防止重复拷贝
	if svr.checkMigratingJobConflict(srcPath, dstPath, clusterId, clusterId, mode) {
		logger.Error("Migrate job is already exist")
		return errors.New(fmt.Sprintf("Migrate job is already exist")), ""
	}
	//logger.Error("=============", zap.Any("srcPath", srcPath),
	//	zap.Any("dstPath", dstPath), zap.Any("condition", strings.HasSuffix(dstPath, srcPath)), zap.Any("mode", mode))
	if mode == proto.JobCopyDir && strings.HasPrefix(dstPath, srcPath) {
		logger.Error("Cannot cp dir to its sub dir src %v dst %v", zap.Any("srcPath", srcPath),
			zap.Any("dstPath", dstPath))
		return errors.New(fmt.Sprintf("Cannot cp dir to its sub dir")), ""
	}
	job := NewMigrateJob(srcPath, clusterId, dstPath, clusterId, mode, svr.SummaryGoroutineLimit, svr.Logger, overwrite)
	svr.addMigratingJob(job)
	job.SetSourceSDK(cli)
	job.SetTargetSDK(cliDst)
	go job.execute(svr)
	return nil, job.JobId
}
