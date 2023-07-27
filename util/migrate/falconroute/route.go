package falconroute

import (
	"fmt"
	"github.com/cubefs/cubefs/util/migrate/util"
	"go.uber.org/zap"
	"net/http"
	"path"
	"strings"
)

var PathPrefix = []string{
	"/volumes/mlp/code/group/",
	"/volumes/mlp/data/group/",
	"/volumes/mlp/code/personal/",
	"/volumes/mlp/data/personal/",
	"/volumes/mlp/data/public/",
	"/volumes/mlp/code/public/",
}

type GroupType uint32

const (
	GroupTypeCeph GroupType = 1 // ceph
	GroupTypeHfs  GroupType = 2 // hfs
	GroupTypeS3fs GroupType = 3 // s3fs
	GroupTypeCfs  GroupType = 4 // cfs
)

const (
	ClusterHT  = "ht"
	ClusterBHW = "bhw"
)

type Router struct {
	Host       string
	FalconAddr string
}

type RouteInfo struct {
	VirtualPath string    `json:"virtualpath"`
	Ak          string    `json:"ak"`
	Sk          string    `json:"sk"`
	Endpoint    string    `json:"endpoint"`
	MountPath   string    `json:"mountpath"`
	GroupType   GroupType `json:"grouptype"`
	Pool        string    `json:"pool"`
	CreateTime  int64     `json:"createtime"`
	Uptime      int64     `json:"uptime"`
	UpdateLog   string    `json:"updatelog"`
	GroupId     string    `json:"groupid"`
	GroupAlias  string    `json:"groupalias"`
}

type User struct {
	ResourceGroupName string `json:"resourceGroupName"`
	FullName          string `json:"fullName"`
	Type              string `json:"type"`
	UserID            string `json:"userId"`
}

type GetGroupUsersResponse struct {
	Users   []User `json:"data"`
	Message string `json:"message"`
	RetCode int    `json:"retCode"`
	Success bool   `json:"success"`
}

type RouterResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type RouterRequest struct {
}

type GetRouteRequest struct {
	RouterRequest
	VirtualPath string `json:"virtualpath"`
}

type GetRouteResponse struct {
	RouterResponse
	RouteInfo RouteInfo `json:"routeinfo"`
}

func GetVirtualPathFromAbsDir(absDir string) string {
	s := ""
	for _, dir := range PathPrefix {
		if strings.HasPrefix(absDir, dir) {
			s = dir
			break
		}
	}

	if s == "" {
		return ""
	}

	arr := strings.Split(absDir, "/")
	if len(arr) < 6 || arr[5] == "" {
		return ""
	}
	if strings.HasPrefix(absDir, "/volumes/mlp/data/public/") ||
		strings.HasPrefix(absDir, "/volumes/mlp/code/public/") {
		return path.Clean(s)
	} else {
		tag := arr[5]
		return path.Clean(s + "/" + tag)
	}
}

type RouterApi interface {
	GetGroupUsers(groupName string, logger *zap.Logger) (users []User, err error)
	GetRoute(virtualPath string, logger *zap.Logger) (routeInfo RouteInfo, err error)
}

func getGroupUrl(group string) string {
	return fmt.Sprintf("/mercury/v1/rg/%s/users", strings.TrimSpace(group))
}

func (r *Router) GetGroupUsers(groupName string, logger *zap.Logger) (users []User, err error) {
	var (
		resp GetGroupUsersResponse
	)

	url := fmt.Sprintf("http://%s%s", r.FalconAddr, getGroupUrl(groupName))

	err = util.DoHttpWithResp(url, http.MethodGet, nil, &resp, logger)
	if err != nil {
		logger.Error("Get group's user list failed", zap.Error(err))
		return nil, err
	}

	if !resp.Success {
		logger.Error("Get group's user resp failed", zap.Any("resp", resp))
		return nil, fmt.Errorf("req group's user fail, msg %s", resp.Message)
	}

	//check if resp user id is upper or lower case
	for i := range resp.Users {
		u := &resp.Users[i]
		u.UserID = strings.ToUpper(u.UserID)
	}

	return resp.Users, nil
}

func GetVirtualPath(dirTyp, usrTyp, usr string) string {
	return fmt.Sprintf("/volumes/mlp/%s/%s/%s", dirTyp, usrTyp, usr)
}

func (g *Router) GetRoute(virtPath string, logger *zap.Logger) (routeInfo RouteInfo, err error) {
	var (
		resp GetRouteResponse
		req  GetRouteRequest
	)

	req.VirtualPath = virtPath

	url := fmt.Sprintf("http://%s/v1/grouter/getroute", g.Host)
	err = util.DoHttpWithResp(url, http.MethodPost, req, &resp, logger)
	if err != nil {
		return
	}

	if resp.Code != 0 {
		return routeInfo, fmt.Errorf("req route list failed, url %s msg %s, code %d", url, resp.Message, resp.Code)
	}

	return resp.RouteInfo, nil
}
