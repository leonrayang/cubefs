package server

import (
	"fmt"
	"github.com/cubefs/cubefs/migrateserver/config"
	"github.com/cubefs/cubefs/util/liblog"
	"github.com/cubefs/cubefs/util/migrate/cubefssdk"
	"github.com/cubefs/cubefs/util/migrate/falconroute"
	"github.com/cubefs/cubefs/util/migrate/proto"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"net/http"
	"os"
	"sync"
	"time"
)

type MigrateServer struct {
	Logger                    *zap.Logger
	port                      int
	stopCh                    chan bool
	cliMap                    map[int32]*MigrateClient
	taskCh                    chan proto.Task
	reSendTaskCh              chan []proto.Task
	migratingJobMap           map[string]*MigrateJob
	completeJobMap            map[string]*MigrateJob
	failTasks                 map[string]proto.Task
	routerMap                 map[string]*falconroute.Router
	mapCliLk                  sync.RWMutex
	mapMigratingJobLk         sync.RWMutex
	mapCompleteJobLk          sync.RWMutex
	mapFailTaskLk             sync.RWMutex
	taskRetryLimit            int
	sdkLogDir                 string
	sdkLogLevel               string
	TraverseDirGoroutineLimit chan bool
	SummaryGoroutineLimit     int
	FailTaskReportLimit       int
	CompleteJobTimeout        time.Duration
	sdkManager                *cubefssdk.SdkManager
}

func NewMigrateServer(cfg *config.Config) *MigrateServer {
	svr := &MigrateServer{
		port:                      cfg.Port,
		stopCh:                    make(chan bool),
		cliMap:                    make(map[int32]*MigrateClient),
		taskCh:                    make(chan proto.Task, 4096),
		reSendTaskCh:              make(chan []proto.Task, 128),
		migratingJobMap:           make(map[string]*MigrateJob),
		completeJobMap:            make(map[string]*MigrateJob),
		failTasks:                 make(map[string]proto.Task),
		routerMap:                 make(map[string]*falconroute.Router),
		taskRetryLimit:            cfg.TaskRetryLimit,
		sdkLogDir:                 cfg.SdkLogCfg.LogDir,
		sdkLogLevel:               cfg.SdkLogCfg.LogLevel,
		TraverseDirGoroutineLimit: make(chan bool, cfg.TraverseDirGoroutineLimit),
		SummaryGoroutineLimit:     cfg.SummaryGoroutineLimit,
		FailTaskReportLimit:       cfg.FailTaskReportLimit,
		CompleteJobTimeout:        time.Duration(cfg.CompleteJobTimeout),
	}
	svr.Logger, _ = liblog.NewZapLoggerWithLevel(cfg.LogCfg)
	for _, route := range cfg.FalconRoute {
		svr.Logger.Debug("Add Router", zap.Any("cluster", route.Name),
			zap.Any("Host", route.Addr), zap.Any("FalconAddr", cfg.FalconAddr))
		svr.routerMap[route.Name] = &falconroute.Router{Host: route.Addr, FalconAddr: cfg.FalconAddr}
	}
	svr.sdkManager = cubefssdk.NewCubeFSSdkManager(svr.Logger)
	go svr.clearCompleteMigrateJob()
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
	svr.Logger.Info("start master svr success", zap.Int("pid", os.Getpid()), zap.Int("listen port", svr.port))
	<-svr.stopCh
}

func (svr *MigrateServer) scheduleToCheckMigrateClients() {
	ticker := time.NewTicker(time.Minute)
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

func (svr *MigrateServer) getMigrateClient(nodeId int32) *MigrateClient {
	svr.mapCliLk.RLock()
	defer svr.mapCliLk.RUnlock()
	return svr.cliMap[nodeId]
}

func (svr *MigrateServer) getAllMigrateClientInfo() []proto.MigrateClientInfo {
	svr.mapCliLk.RLock()
	defer svr.mapCliLk.RUnlock()
	clients := make([]proto.MigrateClientInfo, 0)
	for _, cli := range svr.cliMap {
		clients = append(clients, proto.MigrateClientInfo{
			NodeId:       cli.NodeId,
			Addr:         cli.Addr,
			JobCnt:       cli.jobCnt,
			IdleCnt:      cli.idleCnt,
			ReporterTime: cli.ReporterTime.Format("2006-01-02 15:04:05"),
		})
	}
	return clients
}

func (svr *MigrateServer) addMigrateClient(mc *MigrateClient) {
	svr.mapCliLk.Lock()
	defer svr.mapCliLk.Unlock()
	svr.cliMap[mc.NodeId] = mc
}

const MigrateClientTimeout = proto.FetchTaskInterval * 10

func (svr *MigrateServer) removeInactiveMigrateClient() {
	svr.mapCliLk.Lock()
	defer svr.mapCliLk.Unlock()
	logger := svr.Logger
	for _, cli := range svr.cliMap {
		if time.Now().Sub(cli.ReporterTime) > MigrateClientTimeout {
			cli.close()
			delete(svr.cliMap, cli.NodeId)
			logger.Error("MigrateClient is inactive, remove", zap.String("client", cli.String()))
		}
	}
}

func (svr *MigrateServer) stopAllMigrateClients() {
	svr.mapCliLk.RLock()
	defer svr.mapCliLk.RUnlock()

	for _, cli := range svr.cliMap {
		cli.close()
	}
}

func (svr *MigrateServer) getMigratingTasks() (tasks []proto.Task) {
	svr.mapMigratingJobLk.RLock()
	cache := svr.migratingJobMap
	svr.mapMigratingJobLk.RUnlock()
	for _, job := range cache {
		tasks = append(tasks, job.GetMigratingTasks()...)
	}
	return
}

func (svr *MigrateServer) addMigratingJob(job *MigrateJob) {
	svr.mapMigratingJobLk.RLock()
	defer svr.mapMigratingJobLk.RUnlock()
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
			TotalSize:        job.TotalSize,
			MigratingTaskCnt: job.GetMigratingTaskCnt(),
		})
	}

	return len(svr.migratingJobMap), jobs
}

func (svr *MigrateServer) getFailedTasks() []proto.Task {
	svr.mapFailTaskLk.RLock()
	defer svr.mapFailTaskLk.RUnlock()

	tasks := make([]proto.Task, 0, len(svr.failTasks))
	for _, task := range svr.failTasks {
		tasks = append(tasks, task)
	}
	return tasks
}

func (svr *MigrateServer) updateFailedTask(failTasks []proto.Task, succTasks []proto.Task) {
	logger := svr.Logger
	defer svr.mapFailTaskLk.Unlock()
	svr.mapFailTaskLk.Lock()
	//重试成功，则从失败缓存中删除
	for _, t := range succTasks {
		if _, ok := svr.failTasks[t.Key()]; ok {
			logger.Debug("task success after retry", zap.String("task", t.String()))
			delete(svr.failTasks, t.Key())
		}
	}
	//如果达到最大重试次数还是败了，则保存
	for _, task := range failTasks {
		if task.Retry == svr.taskRetryLimit {
			svr.failTasks[task.Key()] = task
			logger.Warn("task reach retry limit,failed", zap.String("task", task.String()))
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
	for {
		select {
		case <-ticker.C:
			//从job缓存中删除超过最大保存周期的job信息
			svr.mapCompleteJobLk.Lock()
			mapCache := svr.completeJobMap
			svr.mapCompleteJobLk.Unlock()
			for _, job := range mapCache {
				if job.canBeRemovedFromCompleteCache() && time.Now().Sub(job.completeTime) > (svr.CompleteJobTimeout*time.Minute) {
					svr.removeCompleteMigrateJob(job)
					if job.hasSubMigrateJobs() {
						job.clearSubCompleteMigrateJob(svr)
					}
					//clear failed task
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
}

func (svr *MigrateServer) removeFailedTask(tasks []proto.Task) {
	svr.mapFailTaskLk.RLock()
	defer svr.mapFailTaskLk.RUnlock()
	for _, task := range tasks {
		delete(svr.failTasks, task.Key())
	}

}
