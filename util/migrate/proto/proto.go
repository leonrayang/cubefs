package proto

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"time"
)

const (
	RegisterUrl                 = "/register"
	FetchTasksUrl               = "/get/tasks"
	MoveLocalUrl                = "/moveLocalFiles"
	MigrateDetails              = "/MigrateDetails"
	QueryProgressUrl            = "/queryJobProgress"
	QueryProgressesUrl          = "/queryJobsProgress"
	CopyLocalUrl                = "/copyLocalFiles"
	MigrateDirUrl               = "/migrateDir"
	MigrateResourceGroupDirUrl  = "/migrateResourceGroupDir"
	MigrateUserUrl              = "/migrateUser"
	MigrateResourceGroupUrl     = "/migrateResourceGroup"
	MigratingTasksByJobUrl      = "/migratingTasksByJob"
	StopMigratingJobUrl         = "/stopMigratingJob"
	RetryMigratingJobUrl        = "/retryMigratingJob"
	QueryClientMigratingTaskUrl = "/queryClientMigratingTask"
	EnableClientDebugUrl        = "/enableClientDebug"
	DisableClientDebugUrl       = "/disableClientDebug"
)

type HttpReply struct {
	Code int
	Msg  string
	Data interface{}
}

const (
	Succ = iota
	ParmErr
	Fail
)

const (
	JobInitial = iota
	JobRunning
	JobSuccess
	JobFailed
	JobStopped
	JobIdInvalid
)

const (
	JobInvalid = iota
	JobCopyFile
	JobCopyDir
	JobMove
	JobMigrateDir
	JobMigrateResourceGroupDir
	JobMigrateResourceGroup
	JobMigrateUser
)

const (
	TinyFile   = 1024 * 1024
	TinyTask   = "tiny"
	NormalTask = "normal"
)

const FetchTaskInterval = 5 * time.Second

type Task struct {
	Source        string
	Target        string
	JobId         string //所属迁移目录的id
	Retry         int
	Owner         string
	JobStartTime  int64
	WorkMode      int
	MigrateSize   uint64
	TaskId        string
	ErrorMsg      string
	SourceCluster string
	TargetCluster string
	IsRetrying    bool //正在重试的任务不影响job计数
	ConsumeTime   string
	OverWrite     bool
}

func (t *Task) String() string {
	return fmt.Sprintf("source(%s)_target(%s)_jobID(%s)_taskID(%s)_retry(%d)_isRetrying(%v)_client(%s)_"+
		"start(%d)_mode(%v)_migrateSize(%v)_OverWrite(%v)_ErrorMsg(%v)",
		t.Source, t.Target, t.JobId, t.TaskId, t.Retry, t.IsRetrying, t.Owner, t.JobStartTime, t.WorkMode,
		t.MigrateSize, t.OverWrite, t.ErrorMsg)
}

func (t *Task) StringToReport() string {
	return fmt.Sprintf("source(%s)_target(%s)_jobID(%s)_taskID(%s)_retry(%d)_client(%s)_"+
		"start(%d)_mode(%v)_migrateSize(%v)_Err(%v)",
		t.Source, t.Target, t.JobId, t.TaskId, t.Retry, t.Owner, t.JobStartTime, t.WorkMode, t.MigrateSize, t.ErrorMsg)
}

func (t *Task) Key() string {
	return fmt.Sprintf("%s_%s", t.JobId, t.TaskId)
}

func (t *Task) GenerateTaskID() string {
	data := fmt.Sprintf("%s-%s-%s-%s-%d", t.Source, t.Target, t.SourceCluster, t.TargetCluster, t.WorkMode)
	hash := md5.Sum([]byte(data))
	return hex.EncodeToString(hash[:])
}

type RegisterReq struct {
	Addr   string
	JobCnt int
}

func (r *RegisterReq) String() string {
	return fmt.Sprintf("Addr:%s,JobCnt:%d", r.Addr, r.JobCnt)
}

type RegisterResp struct {
	Addr   string
	NodeId int32 // global uniq int
}

func (regResp *RegisterResp) String() string {
	return fmt.Sprintf("RegisterResp nodeId %d", regResp.NodeId)
}

type FetchTasksReq struct {
	NodeId     int32
	IdleCnt    int
	SuccTasks  []Task
	FailTasks  []Task
	ExtraTasks []Task
}

type FetchTasksResp struct {
	Tasks []Task
	//JobCnt int //todo:之后可以提供接口动态调整
}

type MoveLocalFilesReq struct {
	ClusterId string `json:"clusterId"`
	SrcPath   string `json:"srcPath"`
	DstPath   string `json:"dstPath"`
	Overwrite bool   `json:"overwrite"`
}

type MigrateDetailsResp struct {
	MigratingJobCnt   int
	FailedTasks       []Task
	MigratingJobs     []MigratingJobInfo
	MigrateClients    []MigrateClientInfo
	MigratingTasksNum int64
	TaskChanPending   int
}

type MigratingJobInfo struct {
	JobId            string
	CreateTime       string
	SrcPath          string
	DstPath          string
	SrcCluster       string
	DstCluster       string
	WorkMode         int
	TotalSize        uint64
	MigratingTaskCnt int64
	JobStatus        int32
}

type MigrateClientInfo struct {
	ReporterTime string //上报时间
	JobCnt       int    //支持最大的任务数
	IdleCnt      int
	NodeId       int32  //server分配的id
	Addr         string //client地址
}

type QueryJobProgressReq struct {
	JobId string `json:"jobId"`
}

type QueryJobsProgressReq struct {
	JobsId string `json:"jobId"`
}

type QueryJobProgressRsp struct {
	Status             int      `json:"status"`
	Progress           float64  `json:"progress"`
	ErrorMsg           string   `json:"errMsg"`
	ConsumeTime        string   `json:"consumeTime"`
	SizeGB             float64  `json:"sizeGB"`
	SubJobsIdTotal     []string `json:"subJobsIdTotal"`
	SubJobsIdRunning   []string `json:"subJobsIdRunning"`
	SubJobsIdCompleted []string `json:"subJobsIdCompleted"`
}

type JobProgressRsp struct {
	JobId    string  `json:"jobId"`
	Status   int     `json:"status"`
	Progress float64 `json:"progress"`
	ErrorMsg string  `json:"errMsg"`
}

type QueryJobsProgressRsp struct {
	Resp []JobProgressRsp `json:"resp"`
}

type CopyLocalFilesReq struct {
	ClusterId string `json:"clusterId"`
	SrcPath   string `json:"srcPath"`
	DstPath   string `json:"dstPath"`
	Overwrite bool   `json:"overwrite"`
}

type MigrateDirReq struct {
	SrcClusterId string `json:"srcClusterId"`
	DstClusterId string `json:"dstClusterId"`
	Dir          string `json:"dir"`
	Overwrite    bool   `json:"overwrite"`
}

type MigrateResourceGroupDirReq struct {
	SrcClusterId  string `json:"srcClusterId"`
	DstClusterId  string `json:"dstClusterId"`
	ResourceGroup string `json:"resourceGroup"`
	Overwrite     bool   `json:"overwrite"`
}

type MissMigrateJob struct {
	SrcClusterId string `json:"srcClusterId"`
	DstClusterId string `json:"dstClusterId"`
	VirtualPath  string `json:"virtualPath"`
	SrcVol       string `json:"srcVol"`
	DstVol       string `json:"dstVol"`
	Overwrite    bool   `json:"overwrite"`
}

type MigrateUserReq struct {
	SrcClusterId string
	DstClusterId string `json:"dstClusterId"`
	User         string `json:"user"`
	Overwrite    bool   `json:"overwrite"`
}

type MigratingTasksResp struct {
	MigratingTaskCnt int    `json:"taskCnt"`
	MigratingTasks   []Task `json:"migratingTasks"`
}

type StopMigratingJobReq struct {
	JobId string `json:"jobId"`
}

type RetryMigratingJobReq struct {
	JobId string `json:"jobId"`
}

type WorkerMeta struct {
	JobCnt  int //支持最大的任务数
	IdleCnt int
	NodeId  int32  //server分配的id
	Addr    string //client地址
}

type MigrateJobMeta struct {
	SrcPath      string
	DstPath      string
	JobId        string
	WorkMode     int
	SrcCluster   string
	DstCluster   string
	Overwrite    bool
	Status       int32
	CompleteSize uint64
	CreateTime   int64
	CompleteTime int64
	ErrorMsg     string
	TotalSize    uint64
}
