package server

import (
	"errors"
	"fmt"
	"github.com/cubefs/cubefs/util/migrate/falconroute"
	"github.com/cubefs/cubefs/util/migrate/proto"
	"go.uber.org/zap"
)

func (svr *MigrateServer) moveFilesInCluster(srcPath, dstPath, clusterId string) (err error, id string) {
	logger := svr.Logger.With()
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
	if svr.checkMigratingJobConflict(srcPath, dstPath, clusterId, clusterId, proto.JobMove) {
		logger.Error("Migrate job is already exist")
		return errors.New(fmt.Sprintf("Migrate job is already exist")), ""
	}
	job := NewMigrateJob(srcPath, clusterId, dstPath, clusterId, proto.JobMove, svr.SummaryGoroutineLimit, svr.Logger)
	svr.addMigratingJob(job)
	go job.execute(svr)
	return nil, job.JobId
}
