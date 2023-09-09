package server

import (
	"fmt"
	"github.com/cubefs/cubefs/util/migrate/proto"
	"go.uber.org/zap"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type MigrateClient struct {
	ReporterTime time.Time //上报时间
	jobCnt       int32     //支持最大的任务数
	idleCnt      int32
	NodeId       int32  //server分配的id
	Addr         string //client地址
	svr          *MigrateServer
	stopCh       chan bool
	logger       *zap.Logger
	taskMap      sync.Map //正在处理的任务
	//rwLk         sync.RWMutex
	sendCh   chan []proto.Task //分配给client的任务
	handling int32
}

const (
	IsHandling  = 1
	NotHandling = 0
)

func newMigrateClient(addr string, jobCnt int32, nodeId int32, svr *MigrateServer) *MigrateClient {
	mc := &MigrateClient{
		ReporterTime: time.Now(),
		jobCnt:       jobCnt,
		Addr:         addr,
		svr:          svr,
		stopCh:       make(chan bool),
		NodeId:       nodeId,
		sendCh:       make(chan []proto.Task), //没有缓存，没人消费就无法放置。
		//taskMap:      make(map[string]proto.Task),
	}
	atomic.StoreInt32(&mc.handling, 0)
	mc.logger = svr.Logger.With(zap.String("work", mc.String()))
	go mc.start()
	return mc
}

func (mc *MigrateClient) String() string {
	return fmt.Sprintf("Addr(%s)", mc.Addr)
}

func (mc *MigrateClient) updateStatics(idleCnt int) {
	mc.ReporterTime = time.Now()
	atomic.StoreInt32(&mc.idleCnt, int32(idleCnt))
}

func (mc *MigrateClient) close() {
	mc.logger.Info("Migrate client exit")
	if !isClosed(mc.stopCh) {
		close(mc.stopCh)
	}
}

func isClosed(ch <-chan bool) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func (mc *MigrateClient) getTaskCh() chan proto.Task {
	return mc.svr.taskCh
}

func (mc *MigrateClient) getReSendCh() chan []proto.Task {
	return mc.svr.reSendTaskCh
}

func (mc *MigrateClient) start() {
	lastSendTime := time.Now()
	tasks := make([]proto.Task, 0) //client将获取的任务列表
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	taskCh := mc.getTaskCh()
	reSendCh := mc.getReSendCh()
	for {
		fetchLimit := mc.tasksFetchLimit()

		//长时间没法送或者当前Server已经堆积了很多任务
		if len(tasks) >= fetchLimit || time.Since(lastSendTime) > 2*time.Second {
			if len(tasks) > fetchLimit {
				//mc.logger.Warn("putReSendCh for fetch more", zap.Any("taskLen", len(tasks[fetchLimit:])))
				mc.putReSendCh(tasks[fetchLimit:])
				mc.putSendCh(tasks[0:fetchLimit])
			} else {
				mc.putSendCh(tasks)
			}
			lastSendTime = time.Now()
			tasks = make([]proto.Task, 0)
		}

		select {
		case <-mc.stopCh:
			//worker如果退出，则将work之前的任务发给master重新分配
			mc.logger.Warn("putReSendCh for already  get", zap.Any("taskLen", len(tasks)))
			mc.putReSendCh(tasks)
			backupTasks := mc.getRunningTasks()
			mc.logger.Warn("putReSendCh for running tasks", zap.Any("taskLen", len(backupTasks)))
			mc.putReSendCh(backupTasks)
			return
		default:
		}
		//重试的迁移任务
		select {
		case reTasks := <-reSendCh:
			tasks = append(tasks, reTasks...)
			continue
		default:
		}
		//新的迁移任务
		select {
		case task := <-taskCh:
			tasks = append(tasks, task)
		case <-ticker.C:
			continue
		}

	}
}

// 如果阻塞太多就重新发送
func (mc *MigrateClient) tasksFetchLimit() int {
	return int(atomic.LoadInt32(&mc.idleCnt)) * 10
}
func (mc *MigrateClient) putSendCh(tasks []proto.Task) {
	if len(tasks) == 0 {
		return
	}

	ticker := time.NewTicker(time.Second * 30)
	defer ticker.Stop()
	logger := mc.logger.With()

	select {
	case mc.sendCh <- tasks:
		return
	case <-ticker.C:
		logger.Warn("long time (30s) no consume, send to server again")
		mc.putReSendCh(tasks)
		time.Sleep(time.Second)
		return
	}
}

func (mc *MigrateClient) putReSendCh(tasks []proto.Task) {
	if len(tasks) == 0 {
		return
	}
	//mc.logger.Warn("putReSendCh", zap.Any("taskLen", len(tasks)))
	mc.svr.reSendTaskCh <- tasks
}

func (mc *MigrateClient) getRunningTasks() []proto.Task {
	//mc.rwLk.RLock()
	//defer mc.rwLk.RUnlock()

	tasks := make([]proto.Task, 0)
	mc.taskMap.Range(func(key, value interface{}) bool {
		task := value.(proto.Task)
		tasks = append(tasks, task)
		return true
	})
	//for _, task := range mc.taskMap {
	//	tasks = append(tasks, task)
	//}
	return tasks
}

func (mc *MigrateClient) updateRunningTasksStatus(succTasks, failTasks, newTasks []proto.Task, req proto.FetchTasksReq) {
	//mc.rwLk.Lock()
	//defer mc.rwLk.Unlock()

	logger := mc.logger
	//新任务记录到worker的task缓存中
	start := time.Now()
	for _, task := range newTasks {
		key := task.Key()
		mc.taskMap.Store(key, task)
		logger.Debug("add new task", zap.Any("RequestID", req.RequestID), zap.Any("client", req.NodeId), zap.String("task", task.String()))
	}
	logger.Debug("action[fetchTasksHandler] newTasks",
		zap.Any("RequestID", req.RequestID), zap.Any("client", req.NodeId), zap.Any("cost", time.Now().Sub(start).String()))
	//
	start = time.Now()
	for _, task := range succTasks {
		key := task.Key()
		if _, ok := mc.taskMap.Load(key); !ok {
			logger.Warn("receive success task, but not found", zap.Any("RequestID", req.RequestID), zap.Any("client", req.NodeId), zap.String("task", task.String()))
			//直接丢弃
			//continue
		} else {
			mc.taskMap.Delete(key)
		}
		//更新task所属dir的状态
		mc.updateMigratingDirState(task, true, req)
		logger.Debug("receive success task", zap.Any("RequestID", req.RequestID), zap.Any("client", req.NodeId), zap.String("task", task.String()))
	}
	logger.Debug("action[fetchTasksHandler] succTasks",
		zap.Any("RequestID", req.RequestID), zap.Any("client", req.NodeId), zap.Any("cost", time.Now().Sub(start).String()))
	start = time.Now()
	for _, task := range failTasks {
		key := task.Key()
		if _, ok := mc.taskMap.Load(key); !ok {
			logger.Info("receive fail task false, already been deleted", zap.Any("RequestID", req.RequestID), zap.Any("client", req.NodeId), zap.String("task", task.String()))
			//continue
		} else {
			//因为要换个worker，所以从当前worker删除
			mc.taskMap.Delete(key)
		}
		mc.updateMigratingDirState(task, false, req)
		logger.Debug("receive fail task", zap.Any("RequestID", req.RequestID), zap.Any("client", req.NodeId), zap.String("task", task.String()))
	}
	logger.Debug("action[fetchTasksHandler] failTasks",
		zap.Any("RequestID", req.RequestID), zap.Any("client", req.NodeId), zap.Any("cost", time.Now().Sub(start).String()))
}

func (mc *MigrateClient) updateMigratingDirState(task proto.Task, succ bool, req proto.FetchTasksReq) {
	job := mc.svr.getMigratingJob(task.JobId)
	if job == nil {
		//logger.Warn("updateJobState cannot find job", zap.Any("JobId", task.JobId))
		return
	}
	logger := job.logger
	if succ {
		//server重启可能会导致重复计算
		job.updateCompleteSize(task)
		//logger.Debug("CompleteCopy add ", zap.Any("size", task.MigrateSize))
		//如果重试成功,从失败列表中删除
		if task.Retry > 0 {
			job.delFailedMigratingTask(task)
			task.IsRetrying = false
		}
	} else {
		//更新重试次数
		task.Retry++
		//发布前修改
		if mc.taskNeedRetry(task) {
			//if task.Retry <= 1 {
			task.IsRetrying = true
			select {
			case job.retryCh <- task:
				logger.Debug("fail task retry again", zap.String("task", task.String()))
			default:
				logger.Warn("too many failed tasks, do not retry", zap.String("task", task.String()))
				task.ErrorMsg += "[too many failed tasks, do not retry!]"
				task.IsRetrying = false
			}
		} else {
			logger.Warn("task reach retry limit,failed", zap.String("task", task.String()))
			task.IsRetrying = false
		}
		job.saveFailedMigratingTask(task)
	}
	job.delMigratingTask(task)
	job.completeTaskNum.Add(1)
}
func (mc *MigrateClient) taskNeedRetry(task proto.Task) bool {
	//如果是只有修改时间不一致的错误，就不重传
	substrings := strings.Split(task.ErrorMsg, ";")
	var result []string

	for _, s := range substrings {
		trimmed := strings.TrimSpace(s)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	if len(result) == 1 && strings.Contains(result[0], "modify time not the same") {
		return false
	}

	if task.Retry <= mc.svr.taskRetryLimit {
		return true
	}
	return false
}

func (mc *MigrateClient) removeTaskMap(task proto.Task) {
	//mc.rwLk.Lock()
	//defer mc.rwLk.Unlock()

	key := task.Key()
	mc.taskMap.Delete(key)
}

func (mc *MigrateClient) fetchTasks(succTasks, failTasks []proto.Task, req proto.FetchTasksReq) (newTasks []proto.Task) {
	start := time.Now()
	tasks := make([]proto.Task, 0)
	select {
	case tasks = <-mc.sendCh:
	default:
		//发布前修改注释掉
		mc.logger.Debug("no new tasks", zap.Any("RequestID", req.RequestID))
	}
	//更改owner
	mc.svr.mapMigratingJobLk.RLock()
	for _, task := range tasks {
		//如果job被停止了，这个task也就不要了
		if job := mc.svr.migratingJobMap[task.JobId]; job == nil {
			continue
		}
		task.Owner = mc.Addr
		newTasks = append(newTasks, task)
	}
	mc.svr.mapMigratingJobLk.RUnlock()
	mc.logger.Debug("action[fetchTasksHandler] get new tasks", zap.Any("RequestID", req.RequestID), zap.Any("client", req.NodeId), zap.Any("cost", time.Now().Sub(start).String()),
		zap.Any("num", len(newTasks)))
	//更新client的task列表，追加新任务，移除失败和成功的任务
	go mc.updateRunningTasksStatus(succTasks, failTasks, newTasks, req)
	return
}

func (mc *MigrateClient) dump() proto.WorkerMeta {
	return proto.WorkerMeta{
		JobCnt: atomic.LoadInt32(&mc.jobCnt),
		NodeId: mc.NodeId,
		Addr:   mc.Addr,
	}
}
func (mc *MigrateClient) markHandling(flag int) {
	atomic.StoreInt32(&mc.handling, int32(flag))
}

func (mc *MigrateClient) isHandling() bool {
	return atomic.LoadInt32(&mc.handling) == IsHandling
}

// 正在迁移的任务会全部重新尝试
func (svr *MigrateServer) restoreMigrateClient(meta proto.WorkerMeta) {
	cli := newMigrateClient(meta.Addr, meta.JobCnt, meta.NodeId, svr)
	svr.addMigrateClient(cli)
}
