package server

import (
	"fmt"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/mail"
	"github.com/cubefs/cubefs/util/migrate/cubefssdk"
	"github.com/cubefs/cubefs/util/migrate/proto"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"path"
	"strings"
	"sync"
	"time"
)

type MigrateJob struct {
	SrcPath               string
	DstPath               string
	JobId                 string
	logger                *zap.Logger
	CreateTime            int64
	failedTask            map[string]proto.Task //失败的任务，用户定位
	migratingTaskCnt      atomic.Int64
	migratingTask         map[string]proto.Task //job的正在进行的迁移任务缓存
	mapMigratingLk        sync.RWMutex
	mapFailedLk           sync.RWMutex
	completeSize          atomic.Uint64
	TotalSize             uint64
	retryCh               chan proto.Task
	WorkMode              int
	SrcCluster            string
	DstCluster            string
	Status                atomic.Int32
	completeTime          time.Time
	srcSDK                *cubefssdk.CubeFSSdk
	dstSDK                *cubefssdk.CubeFSSdk
	SummaryGoroutineLimit int
	stopCh                chan bool
	subMigratingJob       map[string]*MigrateJob //如果是迁移资源组等包含多个迁移目录的任务，则记录正在迁移的子任务
	mapSubMigratingJobLk  sync.RWMutex
	subCompleteMigrateJob map[string]*MigrateJob //如果是迁移资源组等包含多个迁移目录的任务，则记录完成迁移的子任务
	mapSubCompleteJobLk   sync.RWMutex
	missMigrateJob        []proto.MissMigrateJob //如果是迁移资源组等包含多个迁移目录的任务，有可能没生成任务
	owner                 *MigrateJob            //子迁移任务所属任务
	overWrite             bool
	ErrorMsg              string //仅用于master重启后使用
}

func NewMigrateJob(srcPath, srcCluster, dstPath, dstCluster string, workMode,
	SummaryGoroutineLimit int, logger *zap.Logger, overWrite bool) *MigrateJob {
	job := &MigrateJob{
		SrcPath:               path.Clean(srcPath),
		DstPath:               path.Clean(dstPath),
		SrcCluster:            srcCluster,
		DstCluster:            dstCluster,
		JobId:                 GenerateUUID(),
		CreateTime:            time.Now().Unix(),
		failedTask:            make(map[string]proto.Task),
		migratingTask:         make(map[string]proto.Task),
		retryCh:               make(chan proto.Task, 1024000),
		WorkMode:              workMode,
		TotalSize:             0,
		SummaryGoroutineLimit: SummaryGoroutineLimit,
		stopCh:                make(chan bool),
		subMigratingJob:       make(map[string]*MigrateJob),
		subCompleteMigrateJob: make(map[string]*MigrateJob),
		missMigrateJob:        make([]proto.MissMigrateJob, 0),
		overWrite:             overWrite,
	}
	job.migratingTaskCnt.Store(0)
	job.completeSize.Store(0)
	job.SetJobStatus(proto.JobInitial)
	job.logger = logger.With(zap.String("job", job.JobId), zap.String("srcDir", srcPath),
		zap.String("srcCluster", srcCluster), zap.String("dstDir", dstPath), zap.String("dstCluster", dstCluster),
		zap.Any("mode", job.WorkMode), zap.Any("CreateTime", time.Unix(job.CreateTime, 0).Format("2006-01-02 15:04:05")),
		zap.Any("TotalSize", job.TotalSize))
	job.logger.Debug("Create new job ")
	return job
}

func (job *MigrateJob) GetMigratingTasks() (tasks []proto.Task) {
	if job.hasSubMigrateJobs() {
		return job.getMigratingTasksBySubJobs()
	}

	job.mapMigratingLk.Lock()
	cache := job.migratingTask
	job.mapMigratingLk.Unlock()
	for _, task := range cache {
		tasks = append(tasks, task)
	}
	return
}

func (job *MigrateJob) ResetCompleteSize(size uint64) {
	job.completeSize.Add(size)
}

func (job *MigrateJob) getMigratingTasksBySubJobs() (tasks []proto.Task) {
	job.mapSubMigratingJobLk.Lock()
	cache := job.subMigratingJob
	job.mapSubMigratingJobLk.Unlock()
	for _, sub := range cache {
		tasks = append(tasks, sub.GetMigratingTasks()...)
	}
	return
}

func (job *MigrateJob) addMigratingTask(task proto.Task) {
	job.mapMigratingLk.Lock()
	defer job.mapMigratingLk.Unlock()

	key := task.Key()
	job.migratingTask[key] = task
}

func (job *MigrateJob) delMigratingTask(task proto.Task) {
	//重试的task不减少次数，不然任务可能提前结束，但是需要从migratingTask删除，不然
	//再次分配到这个client可能就无法处理
	if task.IsRetrying == false {
		if job.migratingTaskCnt.Load() > 0 {
			job.migratingTaskCnt.Dec()
		} else {
			job.logger.Warn("cannot dec job cnt if cnt == 0 ", zap.Any("task", task.String()))
		}
	}
	job.mapMigratingLk.Lock()
	defer job.mapMigratingLk.Unlock()

	key := task.Key()
	delete(job.migratingTask, key)
}

func (job *MigrateJob) saveFailedMigratingTask(task proto.Task) {
	job.mapFailedLk.Lock()
	defer job.mapFailedLk.Unlock()
	//可能重试还是失败，从而更新owner
	job.failedTask[task.Key()] = task
	job.logger.Debug("saveFailedMigratingTask", zap.Any("task", task.String()))
}

func (job *MigrateJob) delFailedMigratingTask(task proto.Task) {
	job.mapFailedLk.Lock()
	defer job.mapFailedLk.Unlock()
	if _, ok := job.failedTask[task.Key()]; ok {
		delete(job.failedTask, task.Key())
		job.logger.Debug("delFailedMigratingTask", zap.Any("task", task.String()))
	}
}

func (job *MigrateJob) GetFailedMigratingTask() (tasks []proto.Task) {
	job.mapFailedLk.Lock()
	defer job.mapFailedLk.Unlock()
	for _, task := range job.failedTask {
		tasks = append(tasks, task)
	}
	return
}
func (job *MigrateJob) SetJobStatus(status int32) {
	job.Status.Store(status)
}
func (job *MigrateJob) GetJobStatus() int32 {
	return job.Status.Load()
}

func (job *MigrateJob) updateCompleteSize(task proto.Task) {
	job.completeSize.Add(task.MigrateSize)
	if job.owner != nil {
		job.owner.updateCompleteSize(task)
	}
}

func (job *MigrateJob) execute(svr *MigrateServer) {
	if job.WorkMode == proto.JobMove {
		job.executeInMoveMode(svr)
	}
	if job.WorkMode == proto.JobCopyFile {
		job.executeInCopySingleFileMode(svr)
	}

	if job.WorkMode == proto.JobCopyDir {
		job.executeInCopyDirMode(svr)
	}

	if job.WorkMode == proto.JobMigrateDir {
		job.executeInMigrateDirMode(svr)
	}
}
func (job *MigrateJob) executeInMoveMode(svr *MigrateServer) {
	logger := job.logger
	logger.Debug("start executeInMoveMode")
	defer job.close(svr)
	task := job.newTask(job.SrcPath, job.DstPath, 0)
	job.sendTask(task, svr)
	job.SetJobStatus(proto.JobRunning)
	job.waitUtilTaskDone(svr)
	job.mapFailedLk.Lock()
	if len(job.failedTask) == 0 {
		job.SetJobStatus(proto.JobSuccess)
	} else {
		job.SetJobStatus(proto.JobFailed)
	}
	job.mapFailedLk.Unlock()
}

func (job *MigrateJob) executeInCopySingleFileMode(svr *MigrateServer) {
	logger := job.logger
	logger.Debug("start executeInCopySingleFileMode")
	defer job.close(svr)
	if job.srcSDK != nil {
		job.TotalSize, _ = job.srcSDK.GetFileSize(job.SrcPath)
	}
	task := job.newTask(job.SrcPath, job.DstPath, job.TotalSize)
	job.sendTask(task, svr)
	job.SetJobStatus(proto.JobRunning)
	job.waitUtilTaskDone(svr)
	job.mapFailedLk.Lock()
	if len(job.failedTask) == 0 {
		job.SetJobStatus(proto.JobSuccess)
	} else {
		job.SetJobStatus(proto.JobFailed)
	}
	job.mapFailedLk.Unlock()
}

func (job *MigrateJob) executeInCopyDirMode(svr *MigrateServer) {
	logger := job.logger
	logger.Debug("start executeInCopyDirMode")
	defer job.close(svr)
	if job.srcSDK == nil {
		panic(fmt.Sprintf("srcSDK should not be none"))
	}
	if err := job.mkParentDir(job.DstPath); err != nil {
		job.SetJobStatus(proto.JobFailed)
		job.saveWalkFailedTask(job.SrcPath, job.DstPath, err)
		return
	}
	job.TotalSize, _ = job.srcSDK.GetDirSize(job.SrcPath, job.SummaryGoroutineLimit)
	//拷贝到目标目录下，那么需要对目标路径进行修正
	_, srcRoot := path.Split(job.SrcPath)
	dstPath := path.Join(job.DstPath, srcRoot)
	job.walkDir(job.SrcPath, dstPath, svr)
	job.SetJobStatus(proto.JobRunning)
	logger.Debug("walk done")
	job.waitUtilTaskDone(svr)
	job.mapFailedLk.Lock()
	if len(job.failedTask) == 0 {
		job.SetJobStatus(proto.JobSuccess)
	} else {
		job.SetJobStatus(proto.JobFailed)
	}
	job.mapFailedLk.Unlock()
}

func (job *MigrateJob) executeInMigrateDirMode(svr *MigrateServer) {
	logger := job.logger
	logger.Debug("start executeInMigrateDirMode")
	defer job.close(svr)
	if job.srcSDK == nil {
		panic(fmt.Sprintf("srcSDK should not be none"))
	}
	if err := job.mkParentDir(job.DstPath); err != nil {
		job.SetJobStatus(proto.JobFailed)
		job.saveWalkFailedTask(job.SrcPath, job.DstPath, err)
		return
	}
	job.TotalSize, _ = job.srcSDK.GetDirSize(job.SrcPath, job.SummaryGoroutineLimit)

	job.walkDir(job.SrcPath, job.DstPath, svr)
	job.SetJobStatus(proto.JobRunning)
	logger.Debug("walk done")
	job.waitUtilTaskDone(svr)
	job.mapFailedLk.Lock()
	if len(job.failedTask) == 0 {
		job.SetJobStatus(proto.JobSuccess)
	} else {
		job.SetJobStatus(proto.JobFailed)
	}
	job.mapFailedLk.Unlock()
}
func (job *MigrateJob) close(svr *MigrateServer) {
	job.completeTime = time.Now()
	job.logger = job.logger.With(zap.Any("bytes", job.TotalSize),
		zap.Any("cost", job.completeTime.Sub(time.Unix(job.CreateTime, 0)).String()))
	//从svr记录的正在执行的迁移任务中删除
	svr.closeJob(job)
	svr.saveJobProcess(job)
	//如果是子任务，更新父任务状态
	if job.owner != nil {
		job.owner.saveSubMigrateJob(svr, job)
	} else { //不是子任务可以邮件通知
		//job.sendEmail(svr)
	}
}

func (job *MigrateJob) sendEmail(svr *MigrateServer) {
	progress, status := job.getProgress()
	progress, _ = FormatFloatFloor(progress, 4)
	var errMsg string
	if status == proto.JobFailed {
		errMsg = job.getErrorMsg(svr.FailTaskReportLimit)
	}

	content := fmt.Sprintf("Job [%v] completed [%v],status[%v],errorMsg[%v]", job.JobId, progress, status, errMsg)
	mail.SendMail(content)
}

func (job *MigrateJob) waitUtilTaskDone(svr *MigrateServer) {
	ticker := time.NewTicker(5 * time.Second)
	logger := job.logger
	for {
		select {
		case <-ticker.C:
			if job.idle(svr) {
				return
			}
		case <-svr.stopCh:
			logger.Warn("receive stop signal")
			return
		}
	}
}

func (job *MigrateJob) idle(svr *MigrateServer) bool {
	//有重试任务需要重试
	if len(job.retryCh) > 0 {
		for {
			select {
			case task := <-job.retryCh:
				job.sendTask(task, svr)
			default:
				return false
			}
		}
	}
	return job.migratingTaskCnt.Load() == 0
}

func (job *MigrateJob) sendTask(task proto.Task, svr *MigrateServer) {
	if job.taskIsMigrating(task) {
		return
	}
	job.logger.Debug("sendTask", zap.String("task", task.String()))
	svr.taskCh <- task
	//重传的不参与计数
	if task.IsRetrying == false {
		job.migratingTaskCnt.Add(1)
	}
	job.addMigratingTask(task)
}

func (job *MigrateJob) taskIsMigrating(task proto.Task) bool {
	job.mapMigratingLk.Lock()
	defer job.mapMigratingLk.Unlock()

	if _, ok := job.migratingTask[task.Key()]; ok {
		job.logger.Debug("task is already migrating, no need send again", zap.String("task", task.String()))
		return true
	}

	return false
}

func (job *MigrateJob) GetMigratingTaskCnt() int64 {
	return job.migratingTaskCnt.Load()
}

func (job *MigrateJob) newTask(source, target string, migrateSize uint64) proto.Task {
	t := proto.Task{
		Source:        path.Clean(source),
		Target:        path.Clean(target),
		JobId:         job.JobId,
		Retry:         0,
		JobStartTime:  job.CreateTime,
		WorkMode:      job.WorkMode,
		MigrateSize:   migrateSize,
		TaskId:        GenerateUUID(),
		SourceCluster: job.SrcCluster,
		TargetCluster: job.DstCluster,
		IsRetrying:    false,
	}
	return t
}

func (job *MigrateJob) getSubJobsId() (total, running, completed []string) {
	if !job.hasSubMigrateJobs() {
		return
	}
	job.mapSubCompleteJobLk.Lock()
	cache := job.subCompleteMigrateJob
	job.mapSubCompleteJobLk.Unlock()

	for _, sub := range cache {
		total = append(total, sub.JobId)
		completed = append(completed, sub.JobId)
	}

	job.mapSubMigratingJobLk.Lock()
	cache = job.subMigratingJob
	job.mapSubMigratingJobLk.Unlock()
	for _, sub := range cache {
		total = append(total, sub.JobId)
		running = append(running, sub.JobId)
	}
	return
}
func (job *MigrateJob) getProgress() (float64, int32) {
	if job.GetJobStatus() == proto.JobSuccess {
		return float64(1), proto.JobSuccess
	}
	//复合任务
	if job.hasSubMigrateJobs() {
		return job.getProgressBySubJobs()
	}
	//有可能是空文件夹，或者是没有统计到容量的新目录，还是可以返回job的状态

	if job.TotalSize == 0 {
		job.logger.Debug("totalMigrate cannot be zero")
		//任务完成时，返回1，其他时候为无效值0
		progress := float64(0)
		if job.GetJobStatus() == proto.JobSuccess {
			progress = float64(1)
		}
		return progress, job.GetJobStatus()
	}
	progress := float64(job.completeSize.Load()) / float64(job.TotalSize)
	//job.logger.Debug("getCopyProgress is called", zap.Any("progress", progress), zap.Any("status", job.GetJobStatus()),
	//	zap.Any("completeSize", job.completeSize.Load()))
	return progress, job.GetJobStatus()
}

func (job *MigrateJob) GetCompleteSize() uint64 {
	return job.completeSize.Load()
}
func (job *MigrateJob) SetSourceSDK(sdk *cubefssdk.CubeFSSdk) {
	job.srcSDK = sdk
}

func (job *MigrateJob) SetTargetSDK(sdk *cubefssdk.CubeFSSdk) {
	job.dstSDK = sdk
}

func (job *MigrateJob) hasSubMigrateJobs() bool {
	return job.WorkMode >= proto.JobMigrateResourceGroupDir && job.WorkMode <= proto.JobMigrateUser
}

func (job *MigrateJob) canBeRemovedFromCompleteCache() bool {
	return job.owner == nil
}

func (job *MigrateJob) addSubMigrateJob(sub *MigrateJob) {
	if !job.hasSubMigrateJobs() {
		return
	}
	job.mapSubMigratingJobLk.Lock()
	defer job.mapSubMigratingJobLk.Unlock()
	job.subMigratingJob[sub.JobId] = sub
	sub.setOwner(job)
}

func (job *MigrateJob) saveSubMigrateJob(svr *MigrateServer, sub *MigrateJob) {
	if !job.hasSubMigrateJobs() {
		return
	}
	job.mapSubMigratingJobLk.Lock()
	delete(job.subMigratingJob, sub.JobId)
	subJobNum := len(job.subMigratingJob)
	job.mapSubMigratingJobLk.Unlock()

	job.mapSubCompleteJobLk.Lock()
	job.subCompleteMigrateJob[sub.JobId] = sub
	job.mapSubCompleteJobLk.Unlock()
	//如果任务完成
	if subJobNum == 0 {
		if !job.hasFailSubMigrateStatus() {
			job.SetJobStatus(proto.JobSuccess)
		} else {
			job.SetJobStatus(proto.JobFailed)
		}
		job.close(svr)
	}
}

func (job *MigrateJob) clearSubCompleteMigrateJob(svr *MigrateServer) {
	if !job.hasSubMigrateJobs() {
		return
	}
	job.mapSubCompleteJobLk.Lock()
	for _, sub := range job.subCompleteMigrateJob {
		delete(job.subCompleteMigrateJob, sub.JobId)
		svr.removeCompleteMigrateJob(sub)
		tasks := sub.GetFailedMigratingTask()
		svr.removeFailedTask(tasks)
		svr.removeSuccessTask(sub)
		svr.removeOldJobRelationship(sub.JobId)
	}
	job.mapSubCompleteJobLk.Unlock()
}

func (job *MigrateJob) addMissMigrateJob(sub proto.MissMigrateJob) {
	if !job.hasSubMigrateJobs() {
		return
	}
	job.missMigrateJob = append(job.missMigrateJob, sub)
}

func (job *MigrateJob) setOwner(owner *MigrateJob) {
	if job.hasSubMigrateJobs() {
		return
	}
	job.owner = owner
}

func (job *MigrateJob) hasFailSubMigrateStatus() bool {
	if !job.hasSubMigrateJobs() {
		return false
	}
	var failNum = 0
	job.mapSubCompleteJobLk.Lock()
	cache := job.subCompleteMigrateJob
	job.mapSubCompleteJobLk.Unlock()
	for _, sub := range cache {
		if sub.GetJobStatus() == proto.JobFailed {
			failNum += 1
		}
	}
	return failNum > 0
}

func (job *MigrateJob) getProgressBySubJobs() (float64, int32) {

	var (
		progress   float64
		successNum int
		failNum    int
		runningNum int
	)
	//先看完成的
	job.mapSubCompleteJobLk.Lock()
	cache := job.subCompleteMigrateJob
	job.mapSubCompleteJobLk.Unlock()
	for _, sub := range cache {
		subProgress, _ := sub.getProgress()
		progress += subProgress
		if sub.GetJobStatus() == proto.JobSuccess {
			successNum += 1
		} else if sub.GetJobStatus() == proto.JobFailed {
			failNum += 1
		}
		//发布前注释
		//job.logger.Debug("getProgressBySubJobs is called", zap.Any("progress", subProgress), zap.Any("sub", sub.JobId),
		//	zap.Any("SrcPath", sub.SrcPath), zap.Any("DstPath", sub.DstPath))
	}
	//再看正在进行迁移的
	job.mapSubMigratingJobLk.Lock()
	cache = job.subMigratingJob
	job.mapSubMigratingJobLk.Unlock()
	runningNum = len(cache)
	total := float64(successNum + failNum + runningNum)
	for _, sub := range cache {
		subProgress, _ := sub.getProgress()
		//发布前注释
		//job.logger.Debug("getProgressBySubJobs is called", zap.Any("progress", subProgress), zap.Any("sub", sub.JobId),
		//	zap.Any("SrcPath", sub.SrcPath), zap.Any("DstPath", sub.DstPath))
		progress += subProgress
	}
	//发布前注释
	//job.logger.Debug("getProgressBySubJobs is called", zap.Any("progress", progress), zap.Any("status", job.GetJobStatus()),
	//	zap.Any("total", total), zap.Any("runningNum", runningNum), zap.Any("failNum", failNum), zap.Any("successNum", successNum), zap.Any("result", progress/total))
	//还有任务在跑就是说明进行中
	if runningNum != 0 {
		return progress / total, proto.JobRunning
	}
	if failNum == 0 {
		return float64(1), proto.JobSuccess
	}
	//没有任务在跑
	return progress / total, proto.JobFailed
}

func (job *MigrateJob) getErrorMsg(failTaskReportLimit int) (errorMsg string) {
	if job.ErrorMsg != "" {
		return job.ErrorMsg
	}
	if job.hasSubMigrateJobs() {
		job.mapSubCompleteJobLk.Lock()
		cache := job.subCompleteMigrateJob
		job.mapSubCompleteJobLk.Unlock()
		var count = 0
		for _, sub := range cache {
			failTasks := sub.GetFailedMigratingTask()
			for index, task := range failTasks {
				errorMsg += fmt.Sprintf("[%v]%v;", index, task.StringToReport())
				count++
				if count > failTaskReportLimit {
					errorMsg += "too many errors!"
					return
				}
			}
		}

	}
	failTasks := job.GetFailedMigratingTask()
	if len(failTasks) > failTaskReportLimit {
		failTasks = failTasks[:failTaskReportLimit]
		errorMsg = "too many errors:"
	}
	for index, task := range failTasks {
		errorMsg += fmt.Sprintf("[%v]%v;", index, task.StringToReport())
	}
	return
}
func (job *MigrateJob) GetConsumeTime() string {
	return job.completeTime.Sub(time.Unix(job.CreateTime, 0)).String()
}

// 对于已完成的job，则持久化进度以及task等状态，
func (job *MigrateJob) dump(svr *MigrateServer) proto.MigrateJobMeta {
	//子任务就不参与dump
	if !job.hasSubMigrateJobs() && job.owner != nil {
		return proto.MigrateJobMeta{}
	}
	//如果本身就是之前的任务，要用之前的jobID
	jobId := svr.findOldJobId(job.JobId)
	if jobId == "" {
		jobId = job.JobId
	}
	var meta = proto.MigrateJobMeta{
		JobId:      jobId,
		SrcPath:    job.SrcPath,
		DstPath:    job.DstPath,
		WorkMode:   job.WorkMode,
		SrcCluster: job.SrcCluster,
		DstCluster: job.DstCluster,
		Overwrite:  job.overWrite,
		Status:     job.GetJobStatus(),
		CreateTime: job.CreateTime,
	}
	if meta.Status == proto.JobSuccess || meta.Status == proto.JobFailed {
		meta.CompleteTime = job.completeTime.Unix()
		meta.CompleteSize = job.GetCompleteSize()
		meta.TotalSize = job.TotalSize
	}
	if meta.Status == proto.JobFailed {
		meta.ErrorMsg = job.getErrorMsg(svr.FailTaskReportLimit)
	}
	return meta
}

func (svr *MigrateServer) restoreMigrateJob(jobMeta proto.MigrateJobMeta) (err error) {
	if jobMeta.Status == proto.JobSuccess || jobMeta.Status == proto.JobFailed {
		job := &MigrateJob{
			SrcPath:               jobMeta.SrcPath,
			SrcCluster:            jobMeta.SrcCluster,
			DstPath:               jobMeta.DstPath,
			DstCluster:            jobMeta.DstCluster,
			WorkMode:              jobMeta.WorkMode,
			SummaryGoroutineLimit: svr.SummaryGoroutineLimit,
			logger:                svr.Logger,
			overWrite:             jobMeta.Overwrite,
			JobId:                 jobMeta.JobId,
			CreateTime:            jobMeta.CreateTime,
			completeTime:          time.Unix(jobMeta.CompleteTime, 0),
			TotalSize:             jobMeta.TotalSize,
		}
		job.SetJobStatus(jobMeta.Status)
		job.ResetCompleteSize(jobMeta.CompleteSize)
		if jobMeta.Status == proto.JobFailed {
			job.ErrorMsg = jobMeta.ErrorMsg
		}
		svr.saveJobProcess(job)
		return
	}

	//重新提交job
	var newJobId string
	if jobMeta.WorkMode == proto.JobCopyFile || jobMeta.WorkMode == proto.JobCopyDir {
		err, newJobId = svr.copyFilesInCluster(jobMeta.SrcPath, jobMeta.DstPath, jobMeta.SrcCluster, jobMeta.Overwrite)
		if err != nil {
			return err
		}
	} else if jobMeta.WorkMode == proto.JobMove {
		err, newJobId = svr.moveFilesInCluster(jobMeta.SrcPath, jobMeta.DstPath, jobMeta.SrcCluster, jobMeta.Overwrite)
		if err != nil {
			return err
		}
	} else if jobMeta.WorkMode == proto.JobMigrateDir {
		err, newJobId = svr.migrateTargetDir(jobMeta.SrcPath, jobMeta.SrcCluster, jobMeta.DstCluster, jobMeta.Overwrite)
		if err != nil {
			return err
		}
	} else if jobMeta.WorkMode == proto.JobMigrateResourceGroupDir {
		//TODO
		resourceGroup := jobMeta.SrcPath[strings.Index(jobMeta.SrcPath, "-")+1:]
		err, newJobId = svr.migrateResourceGroupDir(resourceGroup, jobMeta.SrcCluster, jobMeta.DstCluster, jobMeta.Overwrite)
		if err != nil {
			return err
		}
	} else if jobMeta.WorkMode == proto.JobMigrateResourceGroup {
		resourceGroup := jobMeta.SrcPath[strings.Index(jobMeta.SrcPath, "-")+1:]
		err, newJobId = svr.migrateResourceGroup(resourceGroup, jobMeta.SrcCluster, jobMeta.DstCluster)
		if err != nil {
			return err
		}
	} else if jobMeta.WorkMode == proto.JobMigrateUser {
		user := jobMeta.SrcPath[strings.Index(jobMeta.SrcPath, "-")+1:]
		err, newJobId = svr.migrateUser(user, jobMeta.SrcCluster, jobMeta.DstCluster, jobMeta.Overwrite)
		if err != nil {
			return err
		}
	} else {
		return errors.NewErrorf(fmt.Sprintf("Unexpected work mode %v", jobMeta.WorkMode))
	}
	svr.Logger.Info("Create job relationship", zap.Any("old", jobMeta.JobId), zap.Any("new", newJobId))
	svr.addOldJobRelationship(jobMeta.JobId, newJobId)
	return
}

func (svr *MigrateServer) addOldJobRelationship(oldJob, newJob string) {
	svr.mapOldJobsLk.Lock()
	defer svr.mapOldJobsLk.Unlock()
	svr.oldJobs[oldJob] = newJob
}

func (svr *MigrateServer) removeOldJobRelationship(newJob string) {
	svr.mapOldJobsLk.Lock()
	defer svr.mapOldJobsLk.Unlock()
	for key, value := range svr.oldJobs {
		if value == newJob {
			//发布前删除
			//svr.Logger.Debug("Delete job relationship", zap.Any("old", key), zap.Any("new", newJob))
			delete(svr.oldJobs, key)
		}
	}
}
