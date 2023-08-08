package client

import (
	"encoding/json"
	"github.com/cubefs/cubefs/util/migrate/proto"
	"go.uber.org/zap"
	"net/http"
)

func (cli *MigrateClient) registerRouter() {
	//查询worker当前有哪些迁移任务
	http.HandleFunc(proto.QueryClientMigratingTaskUrl, cli.queryClientMigratingTask)
}

func (cli *MigrateClient) queryClientMigratingTask(w http.ResponseWriter, r *http.Request) {
	logger := cli.Logger.With()
	tasks := cli.getAllMigrateTask()
	rsp := &proto.MigratingTasksResp{}
	rsp.MigratingTaskCnt = len(tasks)
	rsp.MigratingTasks = tasks
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
