package client

import (
	"encoding/json"
	"github.com/cubefs/cubefs/util/migrate/proto"
	"go.uber.org/zap"
	"net/http"
	"sync/atomic"
)

func (cli *MigrateClient) registerRouter() {
	//查询worker当前有哪些迁移任务
	http.HandleFunc(proto.QueryClientMigratingTaskUrl, cli.queryClientMigratingTask)
	http.HandleFunc(proto.EnableClientDebugUrl, cli.enableClientDebug)
	http.HandleFunc(proto.DisableClientDebugUrl, cli.disableClientDebug)
	//http.HandleFunc(proto.QueryStreamerLenUrl, cli.queryStreamerLen)

}

func (cli *MigrateClient) disableClientDebug(w http.ResponseWriter, r *http.Request) {
	logger := cli.Logger.With()
	cli.enableDebug = false
	writeResp(w, "worker disable debug mode", logger)
}

func (cli *MigrateClient) enableClientDebug(w http.ResponseWriter, r *http.Request) {
	logger := cli.Logger.With()
	cli.enableDebug = true
	writeResp(w, "worker enable debug mode", logger)
}

//func (cli *MigrateClient) queryStreamerLen(w http.ResponseWriter, r *http.Request) {
//	logger := cli.Logger.With()
//	rsp := &proto.StreamerLenResp{}
//	rsp.Infos, rsp.Total = cli.getStreamerLen()
//	writeResp(w, rsp, logger)
//}

func (cli *MigrateClient) queryClientMigratingTask(w http.ResponseWriter, r *http.Request) {
	logger := cli.Logger.With()
	tasks := cli.getAllMigrateTask()
	rsp := &proto.MigratingTasksResp{}
	rsp.MigratingTaskCnt = int(atomic.LoadInt32(&cli.curJobCnt))
	rsp.MigratingTasks = tasks
	rsp.PendingTaskCnt = len(cli.pendingTaskCh)
	rsp.MaxJobCnt = int(atomic.LoadInt32(&cli.maxJobCnt))
	writeResp(w, rsp, logger)
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
