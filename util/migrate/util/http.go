package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/migrate/proto"
	"go.uber.org/zap"
	"io"
	"net/http"
	"time"
)

func DoPostWithJson(url string, params, result interface{}, log *zap.Logger) (err error) {
	reply := &proto.HttpReply{}
	err = DoHttpWithResp(url, http.MethodPost, params, reply, log)
	if err != nil {
		return
	}

	if reply.Code != proto.Succ {
		return fmt.Errorf("reply not success, code %d, msg %s", reply.Code, reply.Msg)
	}

	data, err := json.Marshal(reply.Data)
	if err != nil {
		return
	}

	err = json.Unmarshal(data, result)
	if err != nil {
		return
	}

	return
}

func DoHttpWithResp(url, method string, params, result interface{}, log *zap.Logger) (err error) {
	var b io.Reader = http.NoBody
	if params != nil {
		msg, err := json.Marshal(params)
		if err != nil {
			log.Fatal("params is not legal", zap.Any("param", params), zap.Error(err))
		}
		b = bytes.NewReader(msg)
	}

	req, err := http.NewRequest(method, url, b)
	if err != nil {
		log.Error("new req failed", zap.String("url", url), zap.Any("param", params), zap.Error(err))
		return
	}

	req.Header.Set("Content-Type", "application/json")

	client := http.DefaultClient
	client.Timeout = 5 * time.Minute
	var resp *http.Response

	for idx := 0; idx < 3; idx++ {
		resp, err = client.Do(req)
		if err == nil {
			break
		}

		log.Warn("do http req err, retry", zap.String("url", url), zap.Any("param", params), zap.Error(err))
		time.Sleep(3 * time.Second)
	}

	if err != nil {
		log.Error("do http req failed after 3 times", zap.String("url", url), zap.Any("param", params), zap.Error(err))
		return err
	}

	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(result)
	if err != nil {
		log.Error("decode reply error", zap.String("url", url), zap.Error(err))
		return
	}

	return
}
