package client

import (
	"errors"
	"fmt"
	"github.com/cubefs/cubefs/util/migrate/falconroute"
	"github.com/cubefs/cubefs/util/migrate/proto"
	"go.uber.org/zap"
)

func (cli *MigrateClient) doMigrateDirOperation(task proto.Task) (error, uint64) {
	var (
		ok            bool
		srcClusterId  = task.SourceCluster
		dstClusterId  = task.TargetCluster
		srcRouterInfo falconroute.RouteInfo
		dstRouterInfo falconroute.RouteInfo
		srcRouter     *falconroute.Router
		dstRouter     *falconroute.Router

		srcPath = task.Source
		dstPath = task.Target
		logger  = cli.Logger
		err     error
	)
	virSrcPath := falconroute.GetVirtualPathFromAbsDir(srcPath)
	virDstPath := falconroute.GetVirtualPathFromAbsDir(dstPath)
	//server做了路由检测，这里可以省略大部分步骤
	if srcRouter, ok = cli.routerMap[srcClusterId]; !ok {
		logger.Error("route not found in config", zap.String("clusterId", srcClusterId))
		return errors.New(fmt.Sprintf("route not found in config %s", srcClusterId)), 0
	}

	srcRouterInfo, err = srcRouter.GetRoute(virSrcPath, logger)
	if err != nil {
		logger.Error("get route failed", zap.Any("virSrcPath", virSrcPath), zap.Error(err))
		return errors.New(fmt.Sprintf("get route %s failed:%s ", virSrcPath, err.Error())), 0
	}

	if dstRouter, ok = cli.routerMap[dstClusterId]; !ok {
		logger.Error("route not found in config", zap.String("clusterId", dstClusterId))
		return errors.New(fmt.Sprintf("route not found in config %s", dstClusterId)), 0
	}

	dstRouterInfo, err = dstRouter.GetRoute(virDstPath, logger)
	if err != nil {
		logger.Error("get route failed", zap.Any("virDstPath", virDstPath), zap.Error(err))
		return errors.New(fmt.Sprintf("get route %s failed:%s ", virDstPath, err.Error())), 0
	}

	if err, totalSize := execCopyDirCommand(cli.sdkManager, srcPath, dstPath, srcRouterInfo.Pool, srcRouterInfo.Endpoint, dstRouterInfo.Pool, dstRouterInfo.Endpoint,
		logger, cli.copyGoroutineLimit, cli.copyQueueLimit); err == nil {
		return nil, totalSize
	} else {
		return errors.New(fmt.Sprintf("Execute copy single file operation failed: %s", err.Error())), 0
	}
}
