package client

import (
	"errors"
	"fmt"
	"github.com/cubefs/cubefs/util/migrate/cubefssdk"
	"github.com/cubefs/cubefs/util/migrate/falconroute"
	"github.com/cubefs/cubefs/util/migrate/proto"
	"go.uber.org/zap"
)

func (cli *MigrateClient) doMoveOperation(task *proto.Task) error {
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

	if router, ok = cli.routerMap[clusterId]; !ok {
		logger.Error("route not found in config", zap.String("clusterId", clusterId))
		return errors.New(fmt.Sprintf("route not found in config %s", clusterId))
	}

	srcRouter, err = router.GetRoute(virSrcPath, logger)
	if err != nil {
		logger.Error("get route failed", zap.Any("virSrcPath", virSrcPath), zap.Error(err))
		return errors.New(fmt.Sprintf("get route %s failed:%s ", virSrcPath, err.Error()))
	}
	if srcRouter.GroupType != falconroute.GroupTypeCfs {
		logger.Error("src route info not in cfs", zap.Any("src", srcRouter))
		return errors.New(fmt.Sprintf("src route info not in cfs: %s", srcRouter.VirtualPath))
	}

	dstRouter, err = router.GetRoute(virDstPath, logger)
	if err != nil {
		logger.Error("get router failed", zap.Any("virDstPath", virDstPath), zap.Error(err))
		return errors.New(fmt.Sprintf("get router %s failed:%s ", virDstPath, err.Error()))
	}
	if dstRouter.GroupType != falconroute.GroupTypeCfs {
		logger.Error("dst route info not in cfs", zap.Any("dst", dstRouter))
		return errors.New(fmt.Sprintf("dst route info not in cfs: %s", dstRouter.VirtualPath))
	}

	if srcRouter.Pool != dstRouter.Pool {
		logger.Error("src and dst not in the same vol", zap.Any("srcPath", srcRouter.VirtualPath),
			zap.Any("srcVol", srcRouter.Pool), zap.Any("dstVol", dstRouter.VirtualPath), zap.Any("dstVol", dstRouter.Pool))
		return errors.New(fmt.Sprintf("src and dst not in the same vol: src vol %s dst vol %s",
			srcRouter.Pool, dstRouter.Pool))
	}
	if err := execMvCommand(cli.sdkManager, srcPath, dstPath, srcRouter.Pool, srcRouter.Endpoint, logger); err == nil {
		return nil
	} else {
		return errors.New(fmt.Sprintf("Execute move operation failed: %s", err.Error()))
	}
}

func execMvCommand(manager *cubefssdk.SdkManager, source, target, vol, endpoint string, logger *zap.Logger) error {
	//无论是否为文件或者文件夹，执行的mv操作一致
	logger.Debug("move files", zap.Any("src", source), zap.Any("dst", target), zap.Any("vol", vol))
	cli, err := manager.GetCubeFSSdk(vol, endpoint)
	if err != nil {
		return err
	}
	return cli.Move(source, target)
}
