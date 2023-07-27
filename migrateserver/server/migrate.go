package server

import (
	"errors"
	"fmt"
	"github.com/cubefs/cubefs/util/migrate/falconroute"
	"github.com/cubefs/cubefs/util/migrate/proto"
	"go.uber.org/zap"
)

func (svr *MigrateServer) migrateTargetDir(dir, srcClusterId, dstClusterId string) (err error, id string) {
	var (
		srcRouterInfo falconroute.RouteInfo
		dstRouterInfo falconroute.RouteInfo
		srcRouter     *falconroute.Router
		dstRouter     *falconroute.Router
		ok            bool
		logger        = svr.Logger
	)
	//虚拟路径只是用来查路由的，并不是完整路劲
	virSrcPath := falconroute.GetVirtualPathFromAbsDir(dir)
	if virSrcPath == "" {
		logger.Error("Path is not legal", zap.String("srcPath", dir))
		return errors.New(fmt.Sprintf("srcPath %s is not legal", dir)), ""
	}
	//获取源路由
	if srcRouter, ok = svr.routerMap[srcClusterId]; !ok {
		logger.Error("route not found in config", zap.String("clusterId", srcClusterId))
		return errors.New(fmt.Sprintf("route not found in config %s", srcClusterId)), ""
	}
	srcRouterInfo, err = srcRouter.GetRoute(virSrcPath, logger)
	if err != nil {
		logger.Error("get route failed", zap.Any("virSrcPath", virSrcPath), zap.Error(err))
		return errors.New(fmt.Sprintf("get route %s failed:%s ", virSrcPath, err.Error())), ""
	}
	if srcRouterInfo.GroupType != falconroute.GroupTypeCfs {
		logger.Error("src route info not in cfs", zap.Any("src", srcRouterInfo))
		return errors.New(fmt.Sprintf("src route info not in cfs: %s", srcRouterInfo.VirtualPath)), ""
	}
	//获取目的路由
	if dstRouter, ok = svr.routerMap[dstClusterId]; !ok {
		logger.Error("route not found in config", zap.String("clusterId", dstClusterId))
		return errors.New(fmt.Sprintf("route not found in config %s", dstClusterId)), ""
	}

	dstRouterInfo, err = dstRouter.GetRoute(virSrcPath, logger)
	if err != nil {
		logger.Error("get router failed", zap.Any("virDstPath", virSrcPath), zap.Error(err))
		return errors.New(fmt.Sprintf("get router %s failed:%s ", virSrcPath, err.Error())), ""
	}
	if dstRouterInfo.GroupType != falconroute.GroupTypeCfs {
		logger.Error("dst route info not in cfs", zap.Any("dst", dstRouterInfo))
		return errors.New(fmt.Sprintf("dst route info not in cfs: %s", dstRouterInfo.VirtualPath)), ""
	}

	srcCli, err := svr.sdkManager.GetCubeFSSdk(srcRouterInfo.Pool, srcRouterInfo.Endpoint)
	if err != nil {
		return err, ""
	}
	dstCli, err := svr.sdkManager.GetCubeFSSdk(dstRouterInfo.Pool, dstRouterInfo.Endpoint)
	if err != nil {
		return err, ""
	}

	var mode = proto.JobMigrateDir

	if svr.checkMigratingJobConflict(dir, dir, srcClusterId, dstClusterId, mode) {
		logger.Error("Migrate job is already exist")
		return errors.New(fmt.Sprintf("Migrate job is already exist")), ""
	}
	job := NewMigrateJob(dir, srcClusterId, dir, dstClusterId, mode, svr.SummaryGoroutineLimit, svr.Logger)
	svr.addMigratingJob(job)
	job.SetSourceSDK(srcCli)
	job.SetTargetSDK(dstCli)
	go job.execute(svr)
	return nil, job.JobId
}

func (svr *MigrateServer) migrateResourceGroupDir(resourceGroup, srcClusterId, dstClusterId string) (err error, id string) {
	var (
		logger        = svr.Logger
		mode          = proto.JobMigrateResourceGroupDir
		subId         string
		srcRouterInfo falconroute.RouteInfo
		dstRouterInfo falconroute.RouteInfo
		srcRouter     *falconroute.Router
		dstRouter     *falconroute.Router
		ok            bool
	)
	//只是为了获取vol来构建job的路径
	virSrcPath := getVirtualPath(CodeRooTDir, GroupDir, resourceGroup)
	//获取源路由
	if srcRouter, ok = svr.routerMap[srcClusterId]; !ok {
		logger.Error("route not found in config", zap.String("clusterId", srcClusterId))
		return errors.New(fmt.Sprintf("route not found in config %s", srcClusterId)), ""
	}
	srcRouterInfo, err = srcRouter.GetRoute(virSrcPath, logger)
	if err != nil {
		logger.Error("get route failed", zap.Any("virSrcPath", virSrcPath), zap.Error(err))
		return errors.New(fmt.Sprintf("get route %s failed:%s ", virSrcPath, err.Error())), ""
	}
	if srcRouterInfo.GroupType != falconroute.GroupTypeCfs {
		logger.Error("src route info not in cfs", zap.Any("src", srcRouterInfo))
		return errors.New(fmt.Sprintf("src route info not in cfs: %s", srcRouterInfo.VirtualPath)), ""
	}
	//获取目的路由
	if dstRouter, ok = svr.routerMap[dstClusterId]; !ok {
		logger.Error("route not found in config", zap.String("clusterId", dstClusterId))
		return errors.New(fmt.Sprintf("route not found in config %s", dstClusterId)), ""
	}

	dstRouterInfo, err = dstRouter.GetRoute(virSrcPath, logger)
	if err != nil {
		logger.Error("get router failed", zap.Any("virDstPath", virSrcPath), zap.Error(err))
		return errors.New(fmt.Sprintf("get router %s failed:%s ", virSrcPath, err.Error())), ""
	}
	if dstRouterInfo.GroupType != falconroute.GroupTypeCfs {
		logger.Error("dst route info not in cfs", zap.Any("dst", dstRouterInfo))
		return errors.New(fmt.Sprintf("dst route info not in cfs: %s", dstRouterInfo.VirtualPath)), ""
	}
	//复合任何判断重复
	jobSrc := fmt.Sprintf("%v-%v", srcRouterInfo.Pool, resourceGroup)
	jobDst := fmt.Sprintf("%v-%v", dstRouterInfo.Pool, resourceGroup)
	if svr.checkMigratingJobConflict(jobSrc, jobDst, srcClusterId, dstClusterId, mode) {
		logger.Error("Migrate job is already exist")
		return errors.New(fmt.Sprintf("Migrate job is already exist")), ""
	}
	//生成一个job只是为了去封装子job
	job := NewMigrateJob(jobSrc, srcClusterId, jobDst, dstClusterId, mode, svr.SummaryGoroutineLimit, svr.Logger)
	svr.addMigratingJob(job)
	err, subId = svr.migrateTargetDir(getVirtualPath(CodeRooTDir, GroupDir, resourceGroup), srcClusterId, dstClusterId)
	if err != nil {
		job.addMissMigrateJob(proto.MissMigrateJob{SrcClusterId: srcClusterId, DstClusterId: dstClusterId, SrcVol: srcRouterInfo.Pool,
			DstVol: dstRouterInfo.Pool, VirtualPath: getVirtualPath(CodeRooTDir, GroupDir, resourceGroup)})
	} else {
		subJob := svr.getMigratingJob(subId)
		job.addSubMigrateJob(subJob)
	}
	err, subId = svr.migrateTargetDir(getVirtualPath(DataRooTDir, GroupDir, resourceGroup), srcClusterId, dstClusterId)
	if err != nil {
		job.addMissMigrateJob(proto.MissMigrateJob{SrcClusterId: srcClusterId, DstClusterId: dstClusterId, SrcVol: srcRouterInfo.Pool,
			DstVol: dstRouterInfo.Pool, VirtualPath: getVirtualPath(DataRooTDir, GroupDir, resourceGroup)})
	} else {
		subJob := svr.getMigratingJob(subId)
		job.addSubMigrateJob(subJob)
	}
	job.SetJobStatus(proto.JobRunning)
	return nil, job.JobId
}

func (svr *MigrateServer) migrateResourceGroup(resourceGroup, srcClusterId, dstClusterId string) (err error, id string) {
	var (
		logger = svr.Logger
		mode   = proto.JobMigrateResourceGroup
		//subId         string
		srcRouterInfo falconroute.RouteInfo
		dstRouterInfo falconroute.RouteInfo
		srcRouter     *falconroute.Router
		dstRouter     *falconroute.Router
		ok            bool
	)
	//只是为了获取vol来构建job的路径
	virSrcPath := getVirtualPath(CodeRooTDir, GroupDir, resourceGroup)
	//获取源路由
	if srcRouter, ok = svr.routerMap[srcClusterId]; !ok {
		logger.Error("route not found in config", zap.String("clusterId", srcClusterId))
		return errors.New(fmt.Sprintf("route not found in config %s", srcClusterId)), ""
	}
	srcRouterInfo, err = srcRouter.GetRoute(virSrcPath, logger)
	if err != nil {
		logger.Error("get route failed", zap.Any("virSrcPath", virSrcPath), zap.Error(err))
		return errors.New(fmt.Sprintf("get route %s failed:%s ", virSrcPath, err.Error())), ""
	}
	if srcRouterInfo.GroupType != falconroute.GroupTypeCfs {
		logger.Error("src route info not in cfs", zap.Any("src", srcRouterInfo))
		return errors.New(fmt.Sprintf("src route info not in cfs: %s", srcRouterInfo.VirtualPath)), ""
	}
	//获取目的路由
	if dstRouter, ok = svr.routerMap[dstClusterId]; !ok {
		logger.Error("route not found in config", zap.String("clusterId", dstClusterId))
		return errors.New(fmt.Sprintf("route not found in config %s", dstClusterId)), ""
	}

	dstRouterInfo, err = dstRouter.GetRoute(virSrcPath, logger)
	if err != nil {
		logger.Error("get router failed", zap.Any("virDstPath", virSrcPath), zap.Error(err))
		return errors.New(fmt.Sprintf("get router %s failed:%s ", virSrcPath, err.Error())), ""
	}
	if dstRouterInfo.GroupType != falconroute.GroupTypeCfs {
		logger.Error("dst route info not in cfs", zap.Any("dst", dstRouterInfo))
		return errors.New(fmt.Sprintf("dst route info not in cfs: %s", dstRouterInfo.VirtualPath)), ""
	}
	//复合任何判断重复
	jobSrc := fmt.Sprintf("%v-%v", srcRouterInfo.Pool, resourceGroup)
	jobDst := fmt.Sprintf("%v-%v", dstRouterInfo.Pool, resourceGroup)
	if svr.checkMigratingJobConflict(jobSrc, jobDst, srcClusterId, dstClusterId, mode) {
		logger.Error("Migrate job is already exist")
		return errors.New(fmt.Sprintf("Migrate job is already exist")), ""
	}
	users, err := srcRouter.GetGroupUsers(resourceGroup, logger)
	if err != nil {
		logger.Error("src route get users failed", zap.Any("virSrcPath", virSrcPath), zap.Any("err", err))
		return errors.New(fmt.Sprintf("src route %s get users failed: %s", virSrcPath, err.Error())), ""

	}
	for _, user := range users {
		logger.Debug("Migrate user", zap.Any("resourceGroup", resourceGroup), zap.Any("user", user.UserID))
	}
	////生成一个job只是为了去封装子job
	//job := NewMigrateJob(jobSrc, srcClusterId, jobDst, dstClusterId, mode, svr.SummaryGoroutineLimit, svr.Logger)
	//svr.addMigratingJob(job)
	//err, subId = svr.migrateTargetDir(getVirtualPath(CodeRooTDir, GroupDir, resourceGroup), srcClusterId, dstClusterId)
	//if err != nil {
	//	job.addMissMigrateJob(proto.MissMigrateJob{SrcClusterId: srcClusterId, DstClusterId: dstClusterId, SrcVol: srcRouterInfo.Pool,
	//		DstVol: dstRouterInfo.Pool, VirtualPath: getVirtualPath(CodeRooTDir, GroupDir, resourceGroup)})
	//} else {
	//	subJob := svr.getMigratingJob(subId)
	//	job.addSubMigrateJob(subJob)
	//}
	//err, subId = svr.migrateTargetDir(getVirtualPath(DataRooTDir, GroupDir, resourceGroup), srcClusterId, dstClusterId)
	//if err != nil {
	//	job.addMissMigrateJob(proto.MissMigrateJob{SrcClusterId: srcClusterId, DstClusterId: dstClusterId, SrcVol: srcRouterInfo.Pool,
	//		DstVol: dstRouterInfo.Pool, VirtualPath: getVirtualPath(DataRooTDir, GroupDir, resourceGroup)})
	//} else {
	//	subJob := svr.getMigratingJob(subId)
	//	job.addSubMigrateJob(subJob)
	//}
	//job.SetJobStatus(proto.JobRunning)
	//return nil, job.JobId
	return nil, "invalid"
}

func (svr *MigrateServer) migrateUser(user, srcClusterId, dstClusterId string) (err error, id string) {
	var (
		logger        = svr.Logger
		mode          = proto.JobMigrateUser
		subId         string
		srcRouterInfo falconroute.RouteInfo
		dstRouterInfo falconroute.RouteInfo
		srcRouter     *falconroute.Router
		dstRouter     *falconroute.Router
		ok            bool
	)
	//只是为了获取vol来构建job的路径
	virSrcPath := getVirtualPath(CodeRooTDir, UserDir, user)
	//获取源路由
	if srcRouter, ok = svr.routerMap[srcClusterId]; !ok {
		logger.Error("route not found in config", zap.String("clusterId", srcClusterId))
		return errors.New(fmt.Sprintf("route not found in config %s", srcClusterId)), ""
	}
	srcRouterInfo, err = srcRouter.GetRoute(virSrcPath, logger)
	if err != nil {
		logger.Error("get route failed", zap.Any("virSrcPath", virSrcPath), zap.Error(err))
		return errors.New(fmt.Sprintf("get route %s failed:%s ", virSrcPath, err.Error())), ""
	}
	if srcRouterInfo.GroupType != falconroute.GroupTypeCfs {
		logger.Error("src route info not in cfs", zap.Any("src", srcRouterInfo))
		return errors.New(fmt.Sprintf("src route info not in cfs: %s", srcRouterInfo.VirtualPath)), ""
	}
	//获取目的路由
	if dstRouter, ok = svr.routerMap[dstClusterId]; !ok {
		logger.Error("route not found in config", zap.String("clusterId", dstClusterId))
		return errors.New(fmt.Sprintf("route not found in config %s", dstClusterId)), ""
	}

	dstRouterInfo, err = dstRouter.GetRoute(virSrcPath, logger)
	if err != nil {
		logger.Error("get router failed", zap.Any("virDstPath", virSrcPath), zap.Error(err))
		return errors.New(fmt.Sprintf("get router %s failed:%s ", virSrcPath, err.Error())), ""
	}
	if dstRouterInfo.GroupType != falconroute.GroupTypeCfs {
		logger.Error("dst route info not in cfs", zap.Any("dst", dstRouterInfo))
		return errors.New(fmt.Sprintf("dst route info not in cfs: %s", dstRouterInfo.VirtualPath)), ""
	}
	//复合任何判断重复
	jobSrc := fmt.Sprintf("%v-%v", srcRouterInfo.Pool, user)
	jobDst := fmt.Sprintf("%v-%v", dstRouterInfo.Pool, user)
	if svr.checkMigratingJobConflict(jobSrc, jobDst, srcClusterId, dstClusterId, mode) {
		logger.Error("Migrate job is already exist")
		return errors.New(fmt.Sprintf("Migrate job is already exist")), ""
	}
	//生成一个job只是为了去封装子job
	job := NewMigrateJob(jobSrc, srcClusterId, jobDst, dstClusterId, mode, svr.SummaryGoroutineLimit, svr.Logger)
	svr.addMigratingJob(job)
	err, subId = svr.migrateTargetDir(getVirtualPath(CodeRooTDir, UserDir, user), srcClusterId, dstClusterId)
	if err != nil {
		job.addMissMigrateJob(proto.MissMigrateJob{SrcClusterId: srcClusterId, DstClusterId: dstClusterId, SrcVol: srcRouterInfo.Pool,
			DstVol: dstRouterInfo.Pool, VirtualPath: getVirtualPath(CodeRooTDir, UserDir, user)})
	} else {
		subJob := svr.getMigratingJob(subId)
		job.addSubMigrateJob(subJob)
	}
	err, subId = svr.migrateTargetDir(getVirtualPath(DataRooTDir, UserDir, user), srcClusterId, dstClusterId)
	if err != nil {
		job.addMissMigrateJob(proto.MissMigrateJob{SrcClusterId: srcClusterId, DstClusterId: dstClusterId, SrcVol: srcRouterInfo.Pool,
			DstVol: dstRouterInfo.Pool, VirtualPath: getVirtualPath(DataRooTDir, UserDir, user)})
	} else {
		subJob := svr.getMigratingJob(subId)
		job.addSubMigrateJob(subJob)
	}
	job.SetJobStatus(proto.JobRunning)
	return nil, job.JobId
}

type MissMigrateJob struct {
	SrcClusterId string `json:"srcClusterId"`
	DstClusterId string `json:"dstClusterId"`
	VirtualPath  string `json:"virtualPath"`
	SrcVol       string `json:"srcVol"`
	DstVol       string `json:"dstVol"`
}

func getVirtualPath(rootDirType, subDirType, name string) string {
	return fmt.Sprintf("/volumes/mlp/%s/%s/%s", rootDirType, subDirType, name)
}

const (
	CodeRooTDir = "code"
	DataRooTDir = "data"
	UserDir     = "personal"
	GroupDir    = "group"
)
