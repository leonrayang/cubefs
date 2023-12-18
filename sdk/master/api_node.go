// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package master

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/cubefs/cubefs/proto"
)

type NodeAPI struct {
	mc *MasterClient
}

func (api *NodeAPI) AddDataNode(serverAddr, zoneName string) (id uint64, err error) {
	request := newAPIRequest(http.MethodGet, proto.AddDataNode)
	request.addParam("addr", serverAddr)
	request.addParam("zoneName", zoneName)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	id, err = strconv.ParseUint(string(data), 10, 64)
	return
}

func (api *NodeAPI) AddDataNodeWithAuthNode(serverAddr, zoneName, clientIDKey string) (id uint64, err error) {
	request := newAPIRequest(http.MethodGet, proto.AddDataNode)
	request.addParam("addr", serverAddr)
	request.addParam("zoneName", zoneName)
	request.addParam("clientIDKey", clientIDKey)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	id, err = strconv.ParseUint(string(data), 10, 64)
	return
}

func (api *NodeAPI) AddMetaNode(serverAddr, zoneName string) (id uint64, err error) {
	request := newAPIRequest(http.MethodGet, proto.AddMetaNode)
	request.addParam("addr", serverAddr)
	request.addParam("zoneName", zoneName)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	id, err = strconv.ParseUint(string(data), 10, 64)
	return
}

func (api *NodeAPI) AddMetaNodeWithAuthNode(serverAddr, zoneName, clientIDKey string) (id uint64, err error) {
	request := newAPIRequest(http.MethodGet, proto.AddMetaNode)
	request.addParam("addr", serverAddr)
	request.addParam("zoneName", zoneName)
	request.addParam("clientIDKey", clientIDKey)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	id, err = strconv.ParseUint(string(data), 10, 64)
	return
}

func (api *NodeAPI) GetDataNode(serverHost string) (node *proto.DataNodeInfo, err error) {
	var buf []byte
	request := newAPIRequest(http.MethodGet, proto.GetDataNode)
	request.addParam("addr", serverHost)
	if buf, err = api.mc.serveRequest(request); err != nil {
		return
	}
	node = &proto.DataNodeInfo{}
	if err = json.Unmarshal(buf, &node); err != nil {
		return
	}
	return
}

func (api *NodeAPI) GetMetaNode(serverHost string) (node *proto.MetaNodeInfo, err error) {
	var buf []byte
	request := newAPIRequest(http.MethodGet, proto.GetMetaNode)
	request.addParam("addr", serverHost)
	if buf, err = api.mc.serveRequest(request); err != nil {
		return
	}
	node = &proto.MetaNodeInfo{}
	if err = json.Unmarshal(buf, &node); err != nil {
		return
	}
	return
}

func (api *NodeAPI) ResponseMetaNodeTask(task *proto.AdminTask) (err error) {
	var encoded []byte
	if encoded, err = json.Marshal(task); err != nil {
		return
	}
	request := newAPIRequest(http.MethodPost, proto.GetMetaNodeTaskResponse)
	request.addBody(encoded)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *NodeAPI) ResponseDataNodeTask(task *proto.AdminTask) (err error) {
	var encoded []byte
	if encoded, err = json.Marshal(task); err != nil {
		return
	}
	request := newAPIRequest(http.MethodPost, proto.GetDataNodeTaskResponse)
	request.addBody(encoded)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *NodeAPI) DataNodeDecommission(nodeAddr string, count int, clientIDKey string) (err error) {
	request := newAPIRequest(http.MethodGet, proto.DecommissionDataNode)
	request.addParam("addr", nodeAddr)
	request.addParam("count", strconv.Itoa(count))
	request.addParam("clientIDKey", clientIDKey)
	request.addHeader("isTimeOut", "false")
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *NodeAPI) MetaNodeDecommission(nodeAddr string, count int, clientIDKey string) (err error) {
	request := newAPIRequest(http.MethodGet, proto.DecommissionMetaNode)
	request.addParam("addr", nodeAddr)
	request.addParam("count", strconv.Itoa(count))
	request.addHeader("isTimeOut", "false")
	request.addParam("clientIDKey", clientIDKey)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *NodeAPI) MetaNodeMigrate(srcAddr, targetAddr string, count int, clientIDKey string) (err error) {
	request := newAPIRequest(http.MethodGet, proto.MigrateMetaNode)
	request.addParam("srcAddr", srcAddr)
	request.addParam("targetAddr", targetAddr)
	request.addParam("count", strconv.Itoa(count))
	request.addHeader("isTimeOut", "false")
	request.addParam("clientIDKey", clientIDKey)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *NodeAPI) DataNodeMigrate(srcAddr, targetAddr string, count int, clientIDKey string) (err error) {
	request := newAPIRequest(http.MethodGet, proto.MigrateDataNode)
	request.addParam("srcAddr", srcAddr)
	request.addParam("targetAddr", targetAddr)
	request.addParam("count", strconv.Itoa(count))
	request.addParam("clientIDKey", clientIDKey)
	request.addHeader("isTimeOut", "false")
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *NodeAPI) AddLcNode(serverAddr string) (id uint64, err error) {
	request := newAPIRequest(http.MethodGet, proto.AddLcNode)
	request.addParam("addr", serverAddr)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	id, err = strconv.ParseUint(string(data), 10, 64)
	return
}

func (api *NodeAPI) ResponseLcNodeTask(task *proto.AdminTask) (err error) {
	var encoded []byte
	if encoded, err = json.Marshal(task); err != nil {
		return
	}
	request := newAPIRequest(http.MethodPost, proto.GetLcNodeTaskResponse)
	request.addBody(encoded)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *NodeAPI) AddFlashNode(serverAddr, zoneName, version string) (id uint64, err error) {
	request := newAPIRequest(http.MethodPost, proto.FlashNodeAdd)
	request.addParam("addr", serverAddr)
	request.addParam("zoneName", zoneName)
	request.addParam("version", version)
	val, err := api.mc.serveRequest(request)
	if err != nil {
		return
	}
	id, err = strconv.ParseUint(string(val), 10, 64)
	return
}

func (api *NodeAPI) SetFlashNode(addr, state string) (err error) {
	request := newAPIRequest(http.MethodGet, proto.FlashNodeSet)
	request.addParam("addr", addr)
	request.addParam("state", state)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *NodeAPI) RemoveFlashNode(nodeAddr string) (result string, err error) {
	request := newAPIRequest(http.MethodPost, proto.FlashNodeRemove)
	request.addParam("addr", nodeAddr)
	request.addHeader("isTimeOut", "false")
	data, err := api.mc.serveRequest(request)
	return string(data), err
}

func (api *NodeAPI) GetFlashNode(addr string) (node proto.FlashNodeViewInfo, err error) {
	request := newAPIRequest(http.MethodGet, proto.FlashNodeGet)
	request.addParam("addr", addr)
	err = api.mc.sendRequest(request, &node)
	return
}

func (api *AdminAPI) ListFlashNodes(all bool) (zoneFlashNodes map[string][]*proto.FlashNodeViewInfo, err error) {
	request := newAPIRequest(http.MethodGet, proto.FlashNodeList)
	request.addParam("all", strconv.FormatBool(all))
	zoneFlashNodes = make(map[string][]*proto.FlashNodeViewInfo)
	err = api.mc.sendRequest(request, &zoneFlashNodes)
	return
}
