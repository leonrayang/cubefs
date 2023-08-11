package server

import (
	"fmt"
	"github.com/cubefs/cubefs/util/migrate/proto"
	"go.uber.org/zap"
	"sync"
	"time"
)

type MigrateClient struct {
	ReporterTime time.Time //上报时间
	jobCnt       int       //支持最大的任务数
	idleCnt      int
	NodeId       int32  //server分配的id
	Addr         string //client地址
	svr          *MigrateServer
	stopCh       chan bool
	logger       *zap.Logger
	taskMap      map[string]proto.Task //正在处理的任务
	rwLk         sync.RWMutex
	sendCh       chan []proto.Task //分配给client的任务
}

func newMigrateClient(addr string, jobCnt int, nodeId int32, svr *MigrateServer) *MigrateClient {
	mc := &MigrateClient{
		ReporterTime: time.Now(),
		jobCnt:       jobCnt,
		Addr:         addr,
		svr:          svr,
		stopCh:       make(chan bool),
		NodeId:       nodeId,
		sendCh:       make(chan []proto.Task), //没有缓存，没人消费就无法放置。
		taskMap:      make(map[string]proto.Task),
	}
	mc.logger = svr.Logger.With(zap.String("work", mc.String()))
	go mc.start()
	return mc
}

func (mc *MigrateClient) String() string {
	return fmt.Sprintf("NodeId(%d),Addr(%s)", mc.NodeId, mc.Addr)
}

func (mc *MigrateClient) updateStatics(idleCnt int) {
	mc.ReporterTime = time.Now()
	mc.idleCnt = idleCnt
}

func (mc *MigrateClient) close() {
	mc.logger.Info("Migrate client exit")
	close(mc.stopCh)
}

func (mc *MigrateClient) getTaskCh() chan proto.Task {
	return mc.svr.taskCh
}

func (mc *MigrateClient) getReSendCh() chan []proto.Task {
	return mc.svr.reSendTaskCh
}

func (mc *MigrateClient) start() {
	logger := mc.logger

	lastSendTime := time.Now()
	tasks := make([]proto.Task, 0) //client将获取的任务列表
	ticker := time.NewTicker(2 * time.Second)
	taskCh := mc.getTaskCh()
	reSendCh := mc.getReSendCh()
	for {
		//长时间没法送或者当前Server已经堆积了很多任务
		if len(tasks) >= mc.tasksFetchLimit() || time.Since(lastSendTime) > 2*time.Second {
			if len(tasks) > mc.tasksFetchLimit() {
				mc.putReSendCh(tasks[mc.tasksFetchLimit():])
				mc.putSendCh(tasks[0:mc.tasksFetchLimit()])
			} else {
				mc.putSendCh(tasks)
			}
			lastSendTime = time.Now()
			tasks = make([]proto.Task, 0)
		}

		select {
		case <-mc.stopCh:
			//worker如果退出，则将work之前的任务发给master重新分配
			mc.putReSendCh(tasks)
			backupTasks := mc.getRunningTasks()
			mc.putReSendCh(backupTasks)
			logger.Warn("receive worker stop signal, hand out task", zap.Any("task", tasks), zap.Any("backupTasks", backupTasks))
			return
		default:
		}
		//重试的迁移任务
		select {
		case reTasks := <-reSendCh: //数组?
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
	return mc.idleCnt * 1
}
func (mc *MigrateClient) putSendCh(tasks []proto.Task) {
	if len(tasks) == 0 {
		return
	}

	ticker := time.NewTicker(time.Second * 30)
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
	mc.svr.reSendTaskCh <- tasks
}

func (mc *MigrateClient) getRunningTasks() []proto.Task {
	mc.rwLk.RLock()
	defer mc.rwLk.RUnlock()

	tasks := make([]proto.Task, 0)
	for _, task := range mc.taskMap {
		tasks = append(tasks, task)
	}
	return tasks
}

func (mc *MigrateClient) updateRunningTasksStatus(succTasks, failTasks, newTasks []proto.Task) {
	mc.rwLk.Lock()
	defer mc.rwLk.Unlock()

	logger := mc.logger
	//新任务记录到worker的task缓存中
	for _, task := range newTasks {
		key := task.Key()
		mc.taskMap[key] = task
		logger.Debug("add new task", zap.String("task", task.String()))
	}
	//
	for _, task := range succTasks {
		key := task.Key()
		if _, ok := mc.taskMap[key]; !ok {
			logger.Warn("receive success task, but already been deleted", zap.String("task", task.String()))
		} else {
			delete(mc.taskMap, key)
		}
		//更新task所属dir的状态
		mc.updateMigratingDirState(task, true)
		logger.Debug("receive success task", zap.String("task", task.String()))
	}

	for _, task := range failTasks {
		key := task.Key()
		if _, ok := mc.taskMap[key]; !ok {
			logger.Info("receive fail task false, already been deleted", zap.String("task", task.String()))
		} else {
			//因为要换个worker，所以从当前worker删除
			delete(mc.taskMap, key)
		}
		mc.updateMigratingDirState(task, false)
		logger.Debug("receive fail task ok", zap.String("task", task.String()), zap.String("error", task.ErrorMsg))
	}
}

func (mc *MigrateClient) updateMigratingDirState(task proto.Task, succ bool) {
	job := mc.svr.getMigratingJob(task.JobId)
	logger := job.logger
	if job == nil {
		logger.Warn("updateJobState cannot find job", zap.Any("JobId", task.JobId))
		return
	}
	if succ {
		job.updateCompleteSize(task)
		logger.Debug("CompleteCopy add ", zap.Any("size", task.MigrateSize))
		//如果重试成功,从失败列表中删除
		if task.Retry > 0 {
			job.delFailedMigratingTask(task)
			task.IsRetrying = false
		}
	} else {
		//更新重试次数
		task.Retry++
		if task.Retry <= mc.svr.taskRetryLimit {
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
}

func (mc *MigrateClient) removeTaskMap(task proto.Task) {
	mc.rwLk.Lock()
	defer mc.rwLk.Unlock()

	key := task.Key()
	delete(mc.taskMap, key)
}

func (mc *MigrateClient) fetchTasks(succTasks, failTasks []proto.Task) (newTasks []proto.Task) {
	tasks := make([]proto.Task, 0)
	select {
	case tasks = <-mc.sendCh:
	default:
		mc.logger.Debug("no new tasks")
	}
	//更改owner
	mc.svr.mapMigratingJobLk.Lock()
	for _, task := range tasks {
		//如果job被停止了，这个task也就不要了
		if job := mc.svr.migratingJobMap[task.JobId]; job == nil {
			continue
		}
		task.Owner = mc.Addr
		newTasks = append(newTasks, task)
	}
	mc.logger.Debug("get new tasks", zap.Any("num", len(newTasks)))
	mc.svr.mapMigratingJobLk.Unlock()
	//更新client的task列表，追加新任务，移除失败和成功的任务
	mc.updateRunningTasksStatus(succTasks, failTasks, newTasks)
	return
}
