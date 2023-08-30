package server

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/migrate/falconroute"
	"github.com/cubefs/cubefs/util/migrate/proto"
	"go.uber.org/zap"
	"math"
	"net/http"
	gopath "path"
	"strconv"
	"strings"
	"time"
)

func (svr *MigrateServer) registerRouter() {
	//注册worker
	http.HandleFunc(proto.RegisterUrl, svr.registerHandler)
	//迁移平台详细信息
	http.HandleFunc(proto.MigrateDetails, svr.migrateDetailsHandler)
	//worker拉取任务
	http.HandleFunc(proto.FetchTasksUrl, svr.fetchTasksHandler)
	//移动本集群文件/文件夹
	http.HandleFunc(proto.MoveLocalUrl, svr.moveLocalFilesHandler)
	//拷贝本集群文件/文件夹
	http.HandleFunc(proto.CopyLocalUrl, svr.copyLocalFilesHandler)
	//查询拷贝/移动的进度
	http.HandleFunc(proto.QueryProgressUrl, svr.queryJobProgressHandler)
	//批量查询拷贝/移动的进度
	http.HandleFunc(proto.QueryProgressesUrl, svr.queryJobsProgressHandler)
	//迁移目录
	http.HandleFunc(proto.MigrateDirUrl, svr.migrateDirHandler)
	//迁移资源组目录
	http.HandleFunc(proto.MigrateResourceGroupDirUrl, svr.migrateResourceGroupDirHandler)
	//迁移资源组目录
	http.HandleFunc(proto.MigrateResourceGroupUrl, svr.migrateResourceGroupHandler)
	//迁移用户
	http.HandleFunc(proto.MigrateUserUrl, svr.migrateUserHandler)
	//迁移任务的task列表
	http.HandleFunc(proto.MigratingTasksByJobUrl, svr.queryMigratingTasksByJobHandler)
	//停止迁移
	http.HandleFunc(proto.StopMigratingJobUrl, svr.stopMigratingJobHandler)
	//重试迁移（暂时没定位出为啥有的任务没分配）
	http.HandleFunc(proto.RetryMigratingJobUrl, svr.retryMigratingJobHandler)
}

func (svr *MigrateServer) migrateDetailsHandler(w http.ResponseWriter, r *http.Request) {
	logger := svr.Logger.With()
	jobCnt, jobInfos := svr.getMigratingJobsInfo()
	var taskTotal int64 = 0
	for _, info := range jobInfos {
		taskTotal += info.MigratingTaskCnt
	}
	detail := &proto.MigrateDetailsResp{
		FailedTasks:       svr.getFailedTasks(),
		MigratingJobCnt:   jobCnt,
		MigratingJobs:     jobInfos,
		MigrateClients:    svr.getAllMigrateClientInfo(),
		MigratingTasksNum: taskTotal, //单独接口获取
	}
	writeResp(w, detail, logger)
}

func (svr *MigrateServer) registerHandler(w http.ResponseWriter, r *http.Request) {
	logger := svr.Logger.With()
	req := &proto.RegisterReq{}
	err := decodeReq(r, req, logger)
	if err != nil {
		writeErr(w, proto.ParmErr, err.Error(), logger)
		return
	}
	if req.Addr == "" {
		req.Addr = strings.Split(r.RemoteAddr, ":")[0]
	}
	//分配nodeid
	nodeId := int32(time.Now().Unix())
	cli := newMigrateClient(req.Addr, req.JobCnt, nodeId, svr)
	svr.addMigrateClient(cli)
	resp := &proto.RegisterResp{
		NodeId: nodeId,
	}
	writeResp(w, resp, logger)
	logger.Info("register success", zap.String("resp", resp.String()))
}

func (svr *MigrateServer) fetchTasksHandler(w http.ResponseWriter, r *http.Request) {
	logger := svr.Logger
	req := &proto.FetchTasksReq{}
	err := decodeReq(r, req, logger)
	if err != nil {
		writeErr(w, proto.ParmErr, err.Error(), logger)
		return
	}
	//获取注册的work信息
	cli := svr.getMigrateClient(req.NodeId)
	if cli == nil || time.Now().Sub(cli.ReporterTime) > 3*proto.FetchTaskInterval {
		logger.Error("MigrateClient maybe already deleted", zap.Any("client", req.NodeId))
		writeErr(w, proto.ParmErr, "MigrateClient not exist", logger)
		return
	}
	//更新worker的时间以及空闲个数
	cli.updateStatics(req.IdleCnt)
	//更新失败task的列表
	svr.updateFailedTask(req.FailTasks, req.SuccTasks)

	resp := &proto.FetchTasksResp{}

	returnTasks := make([]proto.Task, 0)
	//ExtraTasks为忙不过来的map,分配其他空闲client
	if len(req.ExtraTasks) > 0 {
		returnTasks = svr.allocateExtraTask(req.ExtraTasks, cli)
	}
	//处理不了，给其他空闲worker
	if req.IdleCnt < 2 {
		cli.updateRunningTasksStatus(req.SuccTasks, req.FailTasks, make([]proto.Task, 0))
		logger.Debug("worker is busy, no new tasks, only update task map", zap.Any("extra", cli.String()))
		resp.Tasks = returnTasks
		writeResp(w, resp, logger)
		return
	}
	//master待处理的任务。并更新worker的状态
	tasks := cli.fetchTasks(req.SuccTasks, req.FailTasks)
	returnTasks = append(tasks, returnTasks...)
	//返回要做的任务
	resp.Tasks = returnTasks
	writeResp(w, resp, logger)
}

func (svr *MigrateServer) moveLocalFilesHandler(w http.ResponseWriter, r *http.Request) {
	logger := svr.Logger
	req := &proto.MoveLocalFilesReq{}
	err := decodeReq(r, req, logger)
	if err != nil {
		writeErr(w, proto.ParmErr, err.Error(), logger)
		return
	}
	if len(req.SrcPath) == 0 {
		writeErr(w, proto.ParmErr, "SrcPath can't be empty ", logger)
		return
	}
	if len(req.DstPath) == 0 {
		writeErr(w, proto.ParmErr, "DstPath can't be empty", logger)
		return
	}

	if req.SrcPath == req.DstPath {
		writeErr(w, proto.ParmErr, "DstPath can't equal to SrcPath", logger)
		return
	}

	if len(req.ClusterId) == 0 {
		writeErr(w, proto.ParmErr, "ClusterId can't be empty", logger)
		return
	}

	if req.ClusterId != falconroute.ClusterHT && req.ClusterId != falconroute.ClusterBHW {
		writeErr(w, proto.ParmErr, "ClusterId must be bhw or ht", logger)
		return
	}
	_, req.SrcPath = validateDirPath(req.SrcPath)
	_, req.DstPath = validateDirPath(req.DstPath)
	err, id := svr.moveFilesInCluster(req.SrcPath, req.DstPath, req.ClusterId, req.Overwrite)

	if err != nil {
		writeErr(w, proto.Fail, err.Error(), logger)
		return
	}

	writeResp(w, id, logger)
}

func (svr *MigrateServer) copyLocalFilesHandler(w http.ResponseWriter, r *http.Request) {
	logger := svr.Logger
	req := &proto.CopyLocalFilesReq{}
	err := decodeReq(r, req, logger)
	if err != nil {
		writeErr(w, proto.ParmErr, err.Error(), logger)
		return
	}
	if len(req.SrcPath) == 0 {
		writeErr(w, proto.ParmErr, "SrcPath can't be empty ", logger)
		return
	}
	if len(req.DstPath) == 0 {
		writeErr(w, proto.ParmErr, "DstPath can't be empty", logger)
		return
	}

	if req.SrcPath == req.DstPath {
		writeErr(w, proto.ParmErr, "DstPath can't equal to SrcPath", logger)
		return
	}
	if len(req.ClusterId) == 0 {
		writeErr(w, proto.ParmErr, "ClusterId can't be empty", logger)
		return
	}

	if req.ClusterId != falconroute.ClusterHT && req.ClusterId != falconroute.ClusterBHW {
		writeErr(w, proto.ParmErr, "ClusterId must be bhw or ht", logger)
		return
	}
	_, req.SrcPath = validateDirPath(req.SrcPath)
	_, req.DstPath = validateDirPath(req.DstPath)
	err, id := svr.copyFilesInCluster(req.SrcPath, req.DstPath, req.ClusterId, req.Overwrite)

	if err != nil {
		writeErr(w, proto.Fail, err.Error(), logger)
		return
	}
	writeResp(w, id, logger)
}

func (svr *MigrateServer) migrateDirHandler(w http.ResponseWriter, r *http.Request) {
	logger := svr.Logger
	req := &proto.MigrateDirReq{}
	err := decodeReq(r, req, logger)
	if err != nil {
		writeErr(w, proto.ParmErr, err.Error(), logger)
		return
	}
	if len(req.Dir) == 0 {
		writeErr(w, proto.ParmErr, "Dir can't be empty ", logger)
		return
	}

	if len(req.SrcClusterId) == 0 {
		writeErr(w, proto.ParmErr, "SrcClusterId can't be empty", logger)
		return
	}

	if len(req.DstClusterId) == 0 {
		writeErr(w, proto.ParmErr, "DstClusterId can't be empty", logger)
		return
	}

	if req.SrcClusterId == req.DstClusterId {
		writeErr(w, proto.ParmErr, "Cannot migrate in cluster,srcClusterId equal to dstClusterId", logger)
		return
	}

	if req.SrcClusterId != falconroute.ClusterHT && req.SrcClusterId != falconroute.ClusterBHW {
		writeErr(w, proto.ParmErr, "SrcClusterId must be bhw or ht", logger)
		return
	}

	if req.DstClusterId != falconroute.ClusterHT && req.DstClusterId != falconroute.ClusterBHW {
		writeErr(w, proto.ParmErr, "DstClusterId must be bhw or ht", logger)
		return
	}
	_, req.Dir = validateDirPath(req.Dir)
	err, id := svr.migrateTargetDir(req.Dir, req.SrcClusterId, req.DstClusterId, req.Overwrite)

	if err != nil {
		writeErr(w, proto.Fail, err.Error(), logger)
		return
	}
	writeResp(w, id, logger)
}

func (svr *MigrateServer) migrateResourceGroupDirHandler(w http.ResponseWriter, r *http.Request) {
	logger := svr.Logger
	req := &proto.MigrateResourceGroupDirReq{}
	err := decodeReq(r, req, logger)
	if err != nil {
		writeErr(w, proto.ParmErr, err.Error(), logger)
		return
	}
	if len(req.ResourceGroup) == 0 {
		writeErr(w, proto.ParmErr, "ResourceGroup can't be empty ", logger)
		return
	}

	if len(req.SrcClusterId) == 0 {
		writeErr(w, proto.ParmErr, "SrcClusterId can't be empty", logger)
		return
	}

	if len(req.DstClusterId) == 0 {
		writeErr(w, proto.ParmErr, "DstClusterId can't be empty", logger)
		return
	}

	if req.SrcClusterId == req.DstClusterId {
		writeErr(w, proto.ParmErr, "Cannot migrate in cluster,srcClusterId equal to dstClusterId", logger)
		return
	}

	if req.SrcClusterId != falconroute.ClusterHT && req.SrcClusterId != falconroute.ClusterBHW {
		writeErr(w, proto.ParmErr, "SrcClusterId must be bhw or ht", logger)
		return
	}

	if req.DstClusterId != falconroute.ClusterHT && req.DstClusterId != falconroute.ClusterBHW {
		writeErr(w, proto.ParmErr, "DstClusterId must be bhw or ht", logger)
		return
	}

	err, id := svr.migrateResourceGroupDir(req.ResourceGroup, req.SrcClusterId, req.DstClusterId, req.Overwrite)

	if err != nil {
		writeErr(w, proto.Fail, err.Error(), logger)
		return
	}
	writeResp(w, id, logger)
}

func (svr *MigrateServer) migrateResourceGroupHandler(w http.ResponseWriter, r *http.Request) {
	logger := svr.Logger
	req := &proto.MigrateResourceGroupDirReq{}
	err := decodeReq(r, req, logger)
	if err != nil {
		writeErr(w, proto.ParmErr, err.Error(), logger)
		return
	}
	if len(req.ResourceGroup) == 0 {
		writeErr(w, proto.ParmErr, "ResourceGroup can't be empty ", logger)
		return
	}

	if len(req.SrcClusterId) == 0 {
		writeErr(w, proto.ParmErr, "SrcClusterId can't be empty", logger)
		return
	}

	if len(req.DstClusterId) == 0 {
		writeErr(w, proto.ParmErr, "DstClusterId can't be empty", logger)
		return
	}

	if req.SrcClusterId == req.DstClusterId {
		writeErr(w, proto.ParmErr, "Cannot migrate in cluster,srcClusterId equal to dstClusterId", logger)
		return
	}

	if req.SrcClusterId != falconroute.ClusterHT && req.SrcClusterId != falconroute.ClusterBHW {
		writeErr(w, proto.ParmErr, "SrcClusterId must be bhw or ht", logger)
		return
	}

	if req.DstClusterId != falconroute.ClusterHT && req.DstClusterId != falconroute.ClusterBHW {
		writeErr(w, proto.ParmErr, "DstClusterId must be bhw or ht", logger)
		return
	}

	err, id := svr.migrateResourceGroup(req.ResourceGroup, req.SrcClusterId, req.DstClusterId)

	if err != nil {
		writeErr(w, proto.Fail, err.Error(), logger)
		return
	}
	writeResp(w, id, logger)
}

func (svr *MigrateServer) migrateUserHandler(w http.ResponseWriter, r *http.Request) {
	logger := svr.Logger
	req := &proto.MigrateUserReq{}
	err := decodeReq(r, req, logger)
	if err != nil {
		writeErr(w, proto.ParmErr, err.Error(), logger)
		return
	}
	if len(req.User) == 0 {
		writeErr(w, proto.ParmErr, "User can't be empty ", logger)
		return
	}

	if len(req.SrcClusterId) == 0 {
		writeErr(w, proto.ParmErr, "SrcClusterId can't be empty", logger)
		return
	}

	if len(req.DstClusterId) == 0 {
		writeErr(w, proto.ParmErr, "DstClusterId can't be empty", logger)
		return
	}

	if req.SrcClusterId == req.DstClusterId {
		writeErr(w, proto.ParmErr, "Cannot migrate in cluster,srcClusterId equal to dstClusterId", logger)
		return
	}

	if req.SrcClusterId != falconroute.ClusterHT && req.SrcClusterId != falconroute.ClusterBHW {
		writeErr(w, proto.ParmErr, "SrcClusterId must be bhw or ht", logger)
		return
	}

	if req.DstClusterId != falconroute.ClusterHT && req.DstClusterId != falconroute.ClusterBHW {
		writeErr(w, proto.ParmErr, "DstClusterId must be bhw or ht", logger)
		return
	}

	err, id := svr.migrateUser(req.User, req.SrcClusterId, req.DstClusterId, req.Overwrite)

	if err != nil {
		writeErr(w, proto.Fail, err.Error(), logger)
		return
	}
	writeResp(w, id, logger)
	//writeErr(w, proto.ParmErr, "not support", logger)
}

func (svr *MigrateServer) queryJobProgressHandler(w http.ResponseWriter, r *http.Request) {
	logger := svr.Logger
	req := &proto.QueryJobProgressReq{}
	err := decodeReq(r, req, logger)
	if err != nil {
		return
	}
	logger = logger.With(zap.String("JobId", req.JobId))
	//logger.Debug("queryJobProcessHandler is called")
	if len(req.JobId) == 0 {
		writeErr(w, proto.ParmErr, "JobId can't be empty ", logger)
		return
	}

	job := svr.findMigrateJob(req.JobId)
	if job == nil {
		writeErr(w, proto.ParmErr, "JobId is invalid ", logger)
		return
	}
	rsp := &proto.QueryJobProgressRsp{}
	process, status := job.getProgress()
	if status == proto.JobFailed {
		rsp.ErrorMsg = job.getErrorMsg(svr.FailTaskReportLimit)
	}
	process, _ = FormatFloatFloor(process, 4)
	rsp.Status = int(status)
	rsp.Progress = process
	//提供子任务进行查询
	if job.hasSubMigrateJobs() {
		rsp.SubJobsIdTotal, rsp.SubJobsIdRunning, rsp.SubJobsIdCompleted = job.getSubJobsId()
	}
	if status == proto.JobSuccess || status == proto.JobFailed {
		rsp.ConsumeTime = job.completeTime.Sub(time.Unix(job.CreateTime, 0)).String()
		rsp.SizeGB = float64(job.GetCompleteSize()) / float64(1024*1024*1024)
	} else {
		rsp.ConsumeTime = "Invalid"
		rsp.SizeGB = 0
	}

	writeResp(w, rsp, logger)
}

func (svr *MigrateServer) queryJobsProgressHandler(w http.ResponseWriter, r *http.Request) {
	logger := svr.Logger
	req := &proto.QueryJobsProgressReq{}
	err := decodeReq(r, req, logger)
	if err != nil {
		return
	}
	logger = logger.With(zap.Any("JobId", req.JobsId))
	//logger.Debug("queryJobsProgressHandler is called")
	if len(req.JobsId) == 0 {
		writeErr(w, proto.ParmErr, "JobId array can't be empty ", logger)
		return
	}
	jobsId := strings.Split(req.JobsId, ",")
	resp := &proto.QueryJobsProgressRsp{}
	for _, jobId := range jobsId {
		res := proto.JobProgressRsp{JobId: jobId}
		if len(jobId) == 0 {
			res.ErrorMsg = fmt.Sprintf("JobId can't be empty")
			resp.Resp = append(resp.Resp, res)
			continue
		}
		job := svr.findMigrateJob(jobId)
		if job == nil {
			writeErr(w, proto.ParmErr, "JobId is invalid ", logger)
			return
		}
		process, status := job.getProgress()
		if status == proto.JobFailed {
			res.ErrorMsg = job.getErrorMsg(svr.FailTaskReportLimit)
		}
		process, _ = FormatFloatFloor(process, 4)
		res.Status = int(status)
		res.Progress = process
		resp.Resp = append(resp.Resp, res)
	}
	writeResp(w, resp, logger)
}

func (svr *MigrateServer) queryMigratingTasksByJobHandler(w http.ResponseWriter, r *http.Request) {
	logger := svr.Logger
	req := &proto.QueryJobProgressReq{}
	err := decodeReq(r, req, logger)
	if err != nil {
		return
	}
	logger = logger.With(zap.String("JobId", req.JobId))
	logger.Debug("queryJobProcessHandler is called")
	if len(req.JobId) == 0 {
		writeErr(w, proto.ParmErr, "JobId can't be empty ", logger)
		return
	}
	svr.mapMigratingJobLk.Lock()
	job := svr.migratingJobMap[req.JobId]
	svr.mapMigratingJobLk.Unlock()

	if job == nil {
		writeErr(w, proto.ParmErr, "JobId is invalid ", logger)
		return
	}
	tasks := job.GetMigratingTasks()
	rsp := &proto.MigratingTasksResp{}
	rsp.MigratingTaskCnt = int(job.GetMigratingTaskCnt())
	rsp.MigratingTasks = tasks
	writeResp(w, rsp, logger)
}

func (svr *MigrateServer) stopMigratingJobHandler(w http.ResponseWriter, r *http.Request) {
	logger := svr.Logger
	req := &proto.StopMigratingJobReq{}
	err := decodeReq(r, req, logger)
	if err != nil {
		return
	}
	logger = logger.With(zap.String("JobId", req.JobId))
	logger.Debug("stopMigratingJobHandler is called")
	if len(req.JobId) == 0 {
		writeErr(w, proto.ParmErr, "JobId can't be empty ", logger)
		return
	}
	job := svr.findMigrateJob(req.JobId)
	if job == nil {
		writeErr(w, proto.ParmErr, "JobId is invalid ", logger)
		return
	}
	if job == nil {
		writeErr(w, proto.ParmErr, "JobId is invalid ", logger)
		return
	}
	svr.mapMigratingJobLk.Lock()
	delete(svr.migratingJobMap, job.JobId)
	svr.mapMigratingJobLk.Unlock()

	rsp := fmt.Sprintf("Stop job %v success", req.JobId)
	writeResp(w, rsp, logger)
}

func (svr *MigrateServer) retryMigratingJobHandler(w http.ResponseWriter, r *http.Request) {
	logger := svr.Logger
	req := &proto.RetryMigratingJobReq{}
	err := decodeReq(r, req, logger)
	if err != nil {
		return
	}
	logger = logger.With(zap.String("JobId", req.JobId))
	logger.Debug("retryMigratingJobHandler is called")
	if len(req.JobId) == 0 {
		writeErr(w, proto.ParmErr, "JobId can't be empty ", logger)
		return
	}
	job := svr.findMigrateJob(req.JobId)
	if job == nil {
		writeErr(w, proto.ParmErr, "JobId is invalid ", logger)
		return
	}

	if job == nil {
		writeErr(w, proto.ParmErr, "JobId is invalid ", logger)
		return
	}
	//将job下没有分配的task重新分配。
	tasks := job.GetMigratingTasks()
	for _, task := range tasks {
		if task.Owner != "" {
			continue
		}
		svr.taskCh <- task
	}
	rsp := fmt.Sprintf("Retry job %v success", req.JobId)
	writeResp(w, rsp, logger)
}

func FormatFloatFloor(num float64, decimal int) (float64, error) {
	d := float64(1)
	if decimal > 0 {
		d = math.Pow10(decimal)
	}

	res := strconv.FormatFloat(math.Floor(num*d)/d, 'f', -1, 64)
	return strconv.ParseFloat(res, 64)
}

func writeResp(w http.ResponseWriter, data interface{}, logger *zap.Logger) {
	var err error
	defer func() {
		if err != nil {
			logger.Error("write err failed", zap.Error(err))
		}
	}()

	w.Header().Set("Content-Type", "application/json")
	reply := &proto.HttpReply{
		Code: proto.Succ,
		Msg:  "success",
		Data: data,
	}

	body, err := json.Marshal(reply)
	if err != nil {
		w.WriteHeader(http.StatusUnavailableForLegalReasons)
		return
	}

	_, err = w.Write(body)
}

func writeErr(w http.ResponseWriter, code int, msg string, logger *zap.Logger) {
	var err error
	defer func() {
		if err != nil {
			logger.Error("write err failed", zap.Error(err))
		}
	}()

	w.Header().Set("Content-Type", "application/json")
	reply := &proto.HttpReply{
		Code: code,
		Msg:  msg,
	}

	body, err := json.Marshal(reply)
	if err != nil {
		w.WriteHeader(http.StatusUnavailableForLegalReasons)
		return
	}

	_, err = w.Write(body)
}

func decodeReq(r *http.Request, req interface{}, logger *zap.Logger) error {
	err := json.NewDecoder(r.Body).Decode(req)
	if err != nil {
		logger.Error("decode req failed", zap.Error(err))
		return err
	}
	return nil
}

// TODO:是否需要合法性校验，比如前缀
func validateDirPath(dirPath string) (error, string) {
	dirPath = gopath.Clean(dirPath)
	if dirPath[len(dirPath)-1] == '/' {
		dirPath = dirPath[:len(dirPath)-1]
	}
	return nil, dirPath
}
