package server

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/migrateserver/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/liblog"
	"github.com/cubefs/cubefs/util/migrate/cubefssdk"
	"github.com/cubefs/cubefs/util/migrate/falconroute"
	"github.com/cubefs/cubefs/util/migrate/proto"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"net"
	"net/http"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"
)

const (
	TaskMeta   = "task_meta"
	JobMeta    = "job_meta"
	WorkerMeta = "worker_meta"
)

type MigrateServer struct {
	Logger          *zap.Logger
	port            int
	stopCh          chan bool
	cliMap          sync.Map
	taskCh          chan proto.Task
	reSendTaskCh    chan []proto.Task
	migratingJobMap map[string]*MigrateJob
	completeJobMap  map[string]*MigrateJob
	failTasks       map[string]proto.Task
	//successTasks              map[string]proto.Task
	routerMap map[string]*falconroute.Router
	//mapCliLk                  sync.RWMutex
	mapMigratingJobLk         sync.RWMutex
	mapCompleteJobLk          sync.RWMutex
	mapTaskCacheLk            sync.RWMutex
	taskRetryLimit            int
	sdkLogDir                 string
	sdkLogLevel               string
	TraverseDirGoroutineLimit chan bool
	SummaryGoroutineLimit     int
	FailTaskReportLimit       int
	CompleteJobTimeout        time.Duration
	sdkManager                *cubefssdk.SdkManager
	metaDir                   string
	oldJobs                   map[string]string
	mapOldJobsLk              sync.RWMutex
}

func NewMigrateServer(cfg *config.Config) *MigrateServer {
	svr := &MigrateServer{
		port:   cfg.Port,
		stopCh: make(chan bool),
		//cliMap:          make(map[int32]*MigrateClient),
		taskCh:          make(chan proto.Task, 4096),
		reSendTaskCh:    make(chan []proto.Task, 1024),
		migratingJobMap: make(map[string]*MigrateJob),
		completeJobMap:  make(map[string]*MigrateJob),
		failTasks:       make(map[string]proto.Task),
		//successTasks:              make(map[string]proto.Task),
		routerMap:                 make(map[string]*falconroute.Router),
		taskRetryLimit:            cfg.TaskRetryLimit,
		sdkLogDir:                 cfg.SdkLogCfg.LogDir,
		sdkLogLevel:               cfg.SdkLogCfg.LogLevel,
		TraverseDirGoroutineLimit: make(chan bool, cfg.TraverseDirGoroutineLimit),
		SummaryGoroutineLimit:     cfg.SummaryGoroutineLimit,
		FailTaskReportLimit:       cfg.FailTaskReportLimit,
		CompleteJobTimeout:        time.Duration(cfg.CompleteJobTimeout),
		metaDir:                   path.Join(path.Dir(cfg.LogCfg.LogFile), "META"),
		oldJobs:                   make(map[string]string),
	}
	svr.Logger, _ = liblog.NewZapLoggerWithLevel(cfg.LogCfg)
	for _, route := range cfg.FalconRoute {
		svr.Logger.Debug("Add Router", zap.Any("cluster", route.Name),
			zap.Any("Host", route.Addr), zap.Any("FalconAddr", cfg.FalconAddr))
		svr.routerMap[route.Name] = &falconroute.Router{Host: route.Addr, FalconAddr: cfg.FalconAddr}
	}
	svr.sdkManager = cubefssdk.NewCubeFSSdkManager(svr.Logger)
	go svr.clearCompleteMigrateJob()
	go func() {
		if cfg.PprofPort != "" {
			svr.Logger.Info("Start pprof with port:", zap.Any("port", cfg.PprofPort))
			http.ListenAndServe(":"+cfg.PprofPort, nil)
		} else {
			pprofListener, err := net.Listen("tcp", ":0")
			if err != nil {
				svr.Logger.Error("Listen pprof failed", zap.Any("err", err))
				os.Exit(1)
			}
			svr.Logger.Info("Start pprof with port:", zap.Any("port", pprofListener.Addr().(*net.TCPAddr).Port))
			http.Serve(pprofListener, nil)
		}
	}()
	err := svr.loadMetadata()
	if err != nil {
		svr.Logger.Error("load meta dir failed", zap.Any("err", err))
		os.Exit(1)
	}
	return svr
}

func (svr *MigrateServer) Close() {
	svr.Logger.Warn("MigrateServer close")
	close(svr.stopCh)
}

func (svr *MigrateServer) StartHttpServer() {
	svr.registerRouter()
	var server = &http.Server{
		Addr: fmt.Sprintf(":%d", svr.port),
	}

	go func() {
		if err := server.ListenAndServe(); err != nil {
			svr.Logger.Fatal("start http failed", zap.Int("port", svr.port), zap.Error(err))
		}
	}()
}

func (svr *MigrateServer) Run() {
	go svr.scheduleToCheckMigrateClients()
	go svr.persistMeta()
	svr.Logger.Info("start master svr success", zap.Int("pid", os.Getpid()), zap.Int("listen port", svr.port))
	<-svr.stopCh
}

func (svr *MigrateServer) scheduleToCheckMigrateClients() {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-svr.stopCh:
			svr.stopAllMigrateClients()
			svr.Logger.Warn("receive svr stop ch, stop all worker")
			return
		case <-ticker.C:
			//svr.Logger.Info("start check and remove inactive worker")
			svr.removeInactiveMigrateClient()
		}
	}
}

func (svr *MigrateServer) getMigrateClient(nodeId string) *MigrateClient {
	//svr.mapCliLk.RLock()
	//defer svr.mapCliLk.RUnlock()
	value, ok := svr.cliMap.Load(nodeId)
	if ok {
		return value.(*MigrateClient)
	} else {
		return nil
	}
}

func (svr *MigrateServer) getAllMigrateClientInfo() []proto.MigrateClientInfo {
	//svr.mapCliLk.RLock()
	//defer svr.mapCliLk.RUnlock()
	clients := make([]proto.MigrateClientInfo, 0)
	svr.cliMap.Range(func(key, value interface{}) bool {
		cli := value.(*MigrateClient)
		clients = append(clients, proto.MigrateClientInfo{
			NodeId:       cli.NodeId,
			Addr:         cli.Addr,
			JobCnt:       int(atomic.LoadInt32(&cli.jobCnt)),
			IdleCnt:      int(atomic.LoadInt32(&cli.idleCnt)),
			ReporterTime: cli.ReporterTime.Format("2006-01-02 15:04:05"),
		})
		return true
	})
	//for _, cli := range svr.cliMap {
	//	clients = append(clients, proto.MigrateClientInfo{
	//		NodeId:       cli.NodeId,
	//		Addr:         cli.Addr,
	//		JobCnt:       int(atomic.LoadInt32(&cli.jobCnt)),
	//		IdleCnt:      int(atomic.LoadInt32(&cli.idleCnt)),
	//		ReporterTime: cli.ReporterTime.Format("2006-01-02 15:04:05"),
	//	})
	//}
	return clients
}

func (svr *MigrateServer) addMigrateClient(mc *MigrateClient) {
	logger := svr.Logger
	logger.Debug("addMigrateClient start", zap.Any("worker", mc.NodeId))
	//svr.mapCliLk.Lock()
	//defer svr.mapCliLk.Unlock()
	//svr.cliMap[mc.NodeId] = mc
	svr.cliMap.Store(mc.Addr, mc)
	logger.Debug("addMigrateClient end", zap.Any("worker", mc.NodeId))
}

const MigrateClientTimeout = proto.FetchTaskInterval * 240

func (svr *MigrateServer) removeInactiveMigrateClient() {
	logger := svr.Logger
	logger.Debug("removeInactiveMigrateClient start")
	var workersToDelete []string
	svr.cliMap.Range(func(key, value interface{}) bool {
		cli := value.(*MigrateClient)
		//更新上报时间
		logger.Debug("removeInactiveMigrateClient check", zap.String("client", cli.String()),
			zap.String("report time", cli.ReporterTime.String()))
		if cli.isHandling() {
			cli.ReporterTime = time.Now()
		} else {
			if time.Now().Sub(cli.ReporterTime) > MigrateClientTimeout {
				//将client正在处理的任务重新分配
				cli.close()
				workersToDelete = append(workersToDelete, cli.Addr)
			}
		}
		return true
	})

	for _, worker := range workersToDelete {
		logger.Error("MigrateClient is inactive, remove", zap.String("client", worker))
		svr.cliMap.Delete(worker)
	}

	//for _, cli := range svr.cliMap {
	//	//更新上报时间
	//	logger.Debug("removeInactiveMigrateClient check", zap.String("client", cli.String()))
	//	if cli.isHandling() {
	//		cli.ReporterTime = time.Now()
	//	} else {
	//		if time.Now().Sub(cli.ReporterTime) > MigrateClientTimeout {
	//			//将client正在处理的任务重新分配
	//			cli.close()
	//			delete(svr.cliMap, cli.NodeId)
	//			logger.Error("MigrateClient is inactive, remove", zap.String("client", cli.String()))
	//		}
	//	}
	//
	//}
	logger.Debug("removeInactiveMigrateClient complete")
}

func (svr *MigrateServer) stopAllMigrateClients() {
	svr.cliMap.Range(func(key, value interface{}) bool {
		cli := value.(*MigrateClient)
		cli.close()
		return true
	})
}

func (svr *MigrateServer) getMigratingTasks() (tasks []proto.Task) {
	svr.mapMigratingJobLk.RLock()
	for _, job := range svr.migratingJobMap {
		//避免重复统计，复合任务统计了，子任务就不应该被统计
		if !job.hasSubMigrateJobs() && job.owner != nil {
			continue
		}
		tasks = append(tasks, job.GetMigratingTasks()...)
	}
	svr.mapMigratingJobLk.RUnlock()
	return
}

func (svr *MigrateServer) addMigratingJob(job *MigrateJob) {
	svr.mapMigratingJobLk.Lock()
	defer svr.mapMigratingJobLk.Unlock()
	svr.migratingJobMap[job.JobId] = job
}
func (svr *MigrateServer) closeJob(job *MigrateJob) {
	svr.mapMigratingJobLk.Lock()
	defer svr.mapMigratingJobLk.Unlock()
	delete(svr.migratingJobMap, job.JobId)
}

func (svr *MigrateServer) saveJobProcess(job *MigrateJob) {
	svr.mapCompleteJobLk.Lock()
	defer svr.mapCompleteJobLk.Unlock()
	svr.completeJobMap[job.JobId] = job
}

func (svr *MigrateServer) getMigratingJob(id string) *MigrateJob {
	svr.mapMigratingJobLk.RLock()
	defer svr.mapMigratingJobLk.RUnlock()
	return svr.migratingJobMap[id]
}

func (svr *MigrateServer) checkMigratingJobConflict(srcPath, dstPath, srcCluster, dstCluster string, workMode int) bool {
	svr.mapMigratingJobLk.RLock()
	defer svr.mapMigratingJobLk.RUnlock()
	logger := svr.Logger
	logger.Debug("checkMigratingJobConflict", zap.String("srcPath", srcPath), zap.String("dstPath", dstPath),
		zap.String("srcCluster", srcCluster), zap.String("dstCluster", dstCluster), zap.Any("workMode", workMode))
	for _, job := range svr.migratingJobMap {
		logger.Debug("checkMigratingJobConflict job", zap.String("srcPath", job.SrcPath), zap.String("dstPath", job.DstPath),
			zap.String("srcCluster", job.SrcCluster), zap.String("dstCluster", job.DstCluster), zap.Any("workMode", job.WorkMode))
		if job.SrcPath == srcPath && job.DstPath == dstPath && job.SrcCluster == srcCluster &&
			job.DstCluster == dstCluster && job.WorkMode == workMode {
			return true
		}
	}
	return false
}
func (svr *MigrateServer) getMigratingJobsInfo() (int, []proto.MigratingJobInfo) {
	svr.mapMigratingJobLk.RLock()
	defer svr.mapMigratingJobLk.RUnlock()
	jobs := make([]proto.MigratingJobInfo, 0)
	for _, job := range svr.migratingJobMap {
		jobs = append(jobs, proto.MigratingJobInfo{
			JobId:            job.JobId,
			CreateTime:       time.Unix(job.CreateTime, 0).Format("2006-01-02 15:04:05"),
			SrcPath:          job.SrcPath,
			DstPath:          job.DstPath,
			SrcCluster:       job.SrcCluster,
			DstCluster:       job.DstCluster,
			WorkMode:         job.WorkMode,
			TotalSize:        job.TotalSize.Load(),
			MigratingTaskCnt: job.GetMigratingTaskCnt(),
			JobStatus:        job.GetJobStatus(),
		})
	}

	return len(svr.migratingJobMap), jobs
}

func (svr *MigrateServer) getFailedTasks() []proto.Task {
	svr.mapTaskCacheLk.RLock()
	defer svr.mapTaskCacheLk.RUnlock()

	tasks := make([]proto.Task, 0, len(svr.failTasks))
	for _, task := range svr.failTasks {
		tasks = append(tasks, task)
	}
	return tasks
}

func (svr *MigrateServer) updateFailedTask(failTasks []proto.Task, succTasks []proto.Task) {
	logger := svr.Logger
	defer svr.mapTaskCacheLk.Unlock()
	svr.mapTaskCacheLk.Lock()
	//重试成功，则从失败缓存中删除
	for _, t := range succTasks {
		if _, ok := svr.failTasks[t.Key()]; ok {
			logger.Debug("task success after retry", zap.String("task", t.String()))
			delete(svr.failTasks, t.Key())
		}
		//		svr.successTasks[t.Key()] = t
	}
	//如果达到最大重试次数还是败了，则保存
	for _, t := range failTasks {
		if t.Retry == svr.taskRetryLimit {
			svr.failTasks[t.Key()] = t
			logger.Warn("task reach retry limit,failed", zap.String("task", t.String()))
			//if _, ok := svr.successTasks[t.Key()]; ok {
			//	logger.Debug("remove failed task from success cache", zap.String("task", t.String()))
			//	delete(svr.successTasks, t.Key())
			//}
		}
	}
}

func (svr *MigrateServer) allocateExtraTask(extraTasks []proto.Task, mc *MigrateClient) []proto.Task {
	sendCh := svr.taskCh
	newTasks := make([]proto.Task, 0)
	for _, t := range extraTasks {
		select {
		case sendCh <- t:
			mc.removeTaskMap(t)
			mc.logger.Debug("Handout extra task", zap.String("task", t.String()))
		default:
			//处理不过来就返回给worker
			newTasks = append(newTasks, t)
			mc.logger.Warn("No resource to handle extra task", zap.String("task", t.String()))
		}
	}
	return newTasks
}

func (svr *MigrateServer) ReleaseTraverseToken() {
	select {
	case <-svr.TraverseDirGoroutineLimit:
		return
	default:
		return
	}
}

func GenerateUUID() string {
	uuid := uuid.New()
	return uuid.String()
}
func (svr *MigrateServer) clearCompleteMigrateJob() {
	logger := svr.Logger.With(zap.String("timer", "clearCompleteMigrateJob"))
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			//从job缓存中删除超过最大保存周期的job信息
			svr.mapCompleteJobLk.Lock()
			mapCache := svr.completeJobMap
			svr.mapCompleteJobLk.Unlock()
			for _, job := range mapCache {
				//发布前修改
				if job.canBeRemovedFromCompleteCache() && time.Now().Sub(job.completeTime) > (svr.CompleteJobTimeout*time.Minute) {
					//if job.canBeRemovedFromCompleteCache() && time.Now().Sub(job.completeTime) > (1*time.Minute) {
					svr.removeCompleteMigrateJob(job)
					if job.hasSubMigrateJobs() {
						job.clearSubCompleteMigrateJob(svr)
					}
				}
			}

		case <-svr.stopCh:
			logger.Warn("receive stop signal")
			return
		}
	}
}

func (svr *MigrateServer) removeCompleteMigrateJob(job *MigrateJob) {
	svr.mapCompleteJobLk.Lock()
	defer svr.mapCompleteJobLk.Unlock()
	delete(svr.completeJobMap, job.JobId)
	svr.Logger.Info("Delete job process from cache", zap.Any("job", job))
	tasks := job.GetFailedMigratingTask()
	svr.removeFailedTask(tasks)
	//	svr.removeSuccessTask(job)
	svr.removeOldJobRelationship(job.JobId)
}

func (svr *MigrateServer) removeFailedTask(tasks []proto.Task) {
	svr.mapTaskCacheLk.Lock()
	defer svr.mapTaskCacheLk.Unlock()
	for _, task := range tasks {
		delete(svr.failTasks, task.Key())
	}
}

//	func (svr *MigrateServer) removeSuccessTask(job *MigrateJob) {
//		svr.mapTaskCacheLk.RLock()
//		defer svr.mapTaskCacheLk.RUnlock()
//		oldId := svr.findOldJobId(job.JobId)
//		for _, task := range svr.successTasks {
//			//MigrateServer 当前启动后的task删除
//			if task.JobId == job.JobId {
//				svr.Logger.Warn("task is new migrate success", zap.Any("task", task), zap.Any("cacheTask", task))
//				delete(svr.successTasks, task.Key())
//			}
//			//MigrateServer 当前启动前的task删除
//			if task.JobId == oldId {
//				svr.Logger.Warn("task is migrate success before", zap.Any("task", task), zap.Any("cacheTask", task))
//				delete(svr.successTasks, task.Key())
//			}
//
//		}
//	}
func (svr *MigrateServer) findOldJobId(newId string) string {
	svr.mapOldJobsLk.Lock()
	defer svr.mapOldJobsLk.Unlock()
	for key, value := range svr.oldJobs {
		if value == newId {
			return key
		}
	}
	return ""
}

//func (svr *MigrateServer) alreadySuccess(taskKey string) (bool, proto.Task) {
//	svr.mapTaskCacheLk.RLock()
//	defer svr.mapTaskCacheLk.RUnlock()
//	if cacheTask, ok := svr.successTasks[taskKey]; ok {
//		return true, cacheTask
//	} else {
//		return false, proto.Task{}
//	}
//}

func (svr *MigrateServer) persistMeta() {
	//发布前修改：请调整大小
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-svr.stopCh:
			svr.Logger.Warn("receive svr stop ch, exit")
			return
		case <-ticker.C:
			//			svr.Logger.Warn("persist.....")
			svr.persistWorkerMap()
			svr.persistMigratingJobMap()
			//svr.persistSuccessTask()
		}
	}
}

func (svr *MigrateServer) persistMigratingJobMap() {
	logger := svr.Logger.With()
	jobMetas := make([]proto.MigrateJobMeta, 0)
	svr.mapMigratingJobLk.RLock()
	//进行中的job
	for _, job := range svr.migratingJobMap {
		jobMeta := job.dump(svr)
		if jobMeta.JobId == "" {
			continue
		}
		jobMetas = append(jobMetas, jobMeta)
	}
	svr.mapMigratingJobLk.RUnlock()
	//已经完成的job
	svr.mapCompleteJobLk.RLock()
	for _, job := range svr.completeJobMap {
		jobMeta := job.dump(svr)
		if jobMeta.JobId == "" {
			continue
		}
		jobMetas = append(jobMetas, jobMeta)
	}
	svr.mapCompleteJobLk.RUnlock()
	//persist to meta file
	metaFile := path.Join(svr.metaDir, JobMeta)
	tmpFile := path.Join(svr.metaDir, fmt.Sprintf("%s.tmp", JobMeta))
	data, err := json.Marshal(jobMetas)
	if err != nil {
		logger.Error("Marshal jobs error", zap.Any("err", err))
		return
	}

	err = os.WriteFile(tmpFile, data, 0777)
	if err != nil {
		logger.Error("Persist temp jobs meta failed", zap.Any("err", err))
		return
	}

	err = os.Rename(tmpFile, metaFile)
	if err != nil {
		logger.Error("Rename temp jobs meta failed", zap.Any("err", err))
		return
	}
	//svr.Logger.Warn("persistMigratingJobMap success")
}

func (svr *MigrateServer) persistWorkerMap() {
	//start := time.Now()
	logger := svr.Logger.With()
	clients := make([]proto.WorkerMeta, 0)
	svr.cliMap.Range(func(key, value interface{}) bool {
		cli := value.(*MigrateClient)
		clients = append(clients, cli.dump())
		return true
	})
	//persist to meta file
	metaFile := path.Join(svr.metaDir, WorkerMeta)
	tmpFile := path.Join(svr.metaDir, fmt.Sprintf("%s.tmp", WorkerMeta))
	data, err := json.Marshal(clients)
	if err != nil {
		logger.Error("Marshal worker error", zap.Any("err", err))
		return
	}

	err = os.WriteFile(tmpFile, data, 0777)
	if err != nil {
		logger.Error("Persist temp worker meta failed", zap.Any("err", err))
		return
	}

	err = os.Rename(tmpFile, metaFile)
	if err != nil {
		logger.Error("Rename temp worker meta failed", zap.Any("err", err))
		return
	}

	//logger.Debug("Persist worker meta success", zap.String("cost", time.Since(start).String()),
	//	zap.Int("workerCnt", len(clients)))
}

func (svr *MigrateServer) loadWorkerMeta() (err error) {
	logger := svr.Logger.With()
	metaFile := path.Join(svr.metaDir, WorkerMeta)
	data, err := os.ReadFile(metaFile)
	if err != nil {
		//可能之前不存在
		if os.IsNotExist(err) {
			logger.Warn("Worker meta not exist")
			return nil
		}
	}
	if len(data) == 0 {
		logger.Warn("Worker meta is empty")
		return errors.NewErrorf("Worker meta is empty")
	}
	workers := make([]proto.WorkerMeta, 0)
	err = json.Unmarshal(data, &workers)
	if err != nil {
		logger.Warn("Unmarshal Worker meta failed", zap.Any("err", err))
		return errors.NewErrorf(fmt.Sprintf("Unmarshal Worker meta failed %v", err.Error()))
	}
	for _, worker := range workers {
		svr.restoreMigrateClient(worker)
	}
	logger.Info("Read worker meta success")
	return nil
}

func (svr *MigrateServer) loadMigrateJobMeta() (err error) {
	logger := svr.Logger.With()
	metaFile := path.Join(svr.metaDir, JobMeta)
	data, err := os.ReadFile(metaFile)
	if err != nil {
		//可能之前不存在
		if os.IsNotExist(err) {
			logger.Warn("MigrateJob meta not exist")
			return nil
		}
	}
	if len(data) == 0 {
		logger.Warn("MigrateJob meta is empty")
		return errors.NewErrorf("MigrateJob meta is empty")
	}
	jobs := make([]proto.MigrateJobMeta, 0)
	err = json.Unmarshal(data, &jobs)
	if err != nil {
		logger.Warn("Unmarshal MigrateJob meta failed", zap.Any("err", err))
		return errors.NewErrorf(fmt.Sprintf("Unmarshal Worker meta failed %v", err.Error()))
	}
	for _, job := range jobs {
		err = svr.restoreMigrateJob(job)
		if err != nil {
			logger.Warn("Restore migrateJob  failed", zap.Any("err", err), zap.Any("job", job))
			return errors.NewErrorf(fmt.Sprintf("Restore migrateJob %v failed %v", job, err.Error()))
		}
	}
	logger.Info("Read MigrateJob meta success")
	return nil
}

func (svr *MigrateServer) loadMetadata() (err error) {
	logger := svr.Logger
	_, err = os.Open(svr.metaDir)
	if os.IsNotExist(err) {
		logger.Info("Meta dir not exist, crate it", zap.String("dir", svr.metaDir))
		err = os.MkdirAll(svr.metaDir, 0777)
		if err != nil {
			return
		}
	}
	//读取worker元文件
	err = svr.loadWorkerMeta()
	if err != nil {
		return
	}
	//err = svr.loadSuccessTasksMeta()
	//if err != nil {
	//	return
	//}
	err = svr.loadMigrateJobMeta()
	if err != nil {
		return
	}
	return nil
}

//func (svr *MigrateServer) persistSuccessTask() {
//	//start := time.Now()
//	logger := svr.Logger.With()
//	tasks := make([]proto.Task, 0)
//	svr.mapTaskCacheLk.RLock()
//	for _, task := range svr.successTasks {
//		tasks = append(tasks, task)
//	}
//	svr.mapTaskCacheLk.RUnlock()
//	//persist to meta file
//	metaFile := path.Join(svr.metaDir, TaskMeta)
//	tmpFile := path.Join(svr.metaDir, fmt.Sprintf("%s.tmp", TaskMeta))
//	data, err := json.Marshal(tasks)
//	if err != nil {
//		logger.Error("Marshal success tasks error", zap.Any("err", err))
//		return
//	}
//
//	err = os.WriteFile(tmpFile, data, 0777)
//	if err != nil {
//		logger.Error("Persist success tasks meta failed", zap.Any("err", err))
//		return
//	}
//
//	err = os.Rename(tmpFile, metaFile)
//	if err != nil {
//		logger.Error("Rename success tasks meta failed", zap.Any("err", err))
//		return
//	}
//
//	//logger.Debug("Persist tasks meta success", zap.String("cost", time.Since(start).String()),
//	//	zap.Int("tasksCnt", len(tasks)))
//}
//
//func (svr *MigrateServer) loadSuccessTasksMeta() (err error) {
//	logger := svr.Logger.With()
//	metaFile := path.Join(svr.metaDir, TaskMeta)
//	data, err := os.ReadFile(metaFile)
//	if err != nil {
//		//可能之前不存在
//		if os.IsNotExist(err) {
//			logger.Warn("Success tasks meta not exist")
//			return nil
//		}
//	}
//	if len(data) == 0 {
//		logger.Warn("Success tasks is empty")
//		return errors.NewErrorf("Worker meta is empty")
//	}
//	tasks := make([]proto.Task, 0)
//	err = json.Unmarshal(data, &tasks)
//	if err != nil {
//		logger.Warn("Unmarshal success tasks meta failed", zap.Any("err", err))
//		return errors.NewErrorf(fmt.Sprintf("Unmarshal success tasks meta failed %v", err.Error()))
//	}
//	for _, task := range tasks {
//		svr.successTasks[task.Key()] = task
//	}
//	logger.Info("Read task meta success")
//	return nil
//}

func (svr *MigrateServer) findMigrateJob(jobId string) *MigrateJob {
	//	logger := svr.Logger.With()
	//发布前删除
	//logger.Info("findMigrateJob", zap.Any("migratingJobMap", svr.migratingJobMap),
	//	zap.Any("completeJobMap", svr.completeJobMap), zap.Any("oldJobs", svr.oldJobs))
	svr.mapMigratingJobLk.RLock()
	job := svr.migratingJobMap[jobId]
	svr.mapMigratingJobLk.RUnlock()

	if job == nil {
		svr.mapCompleteJobLk.RLock()
		job = svr.completeJobMap[jobId]
		svr.mapCompleteJobLk.RUnlock()
		if job == nil {
			svr.mapOldJobsLk.RLock()
			newJobId := svr.oldJobs[jobId]
			svr.mapOldJobsLk.RUnlock()
			if newJobId == "" {
				//发布前删除
				//logger.Warn("cannot find migrate job", zap.Any("id", jobId))
				return nil
			} else {
				return svr.findMigrateJob(newJobId)
			}
		} else {
			return job
		}
	}
	return job
}
