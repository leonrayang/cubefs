package cp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"path"
	"strings"

	"github.com/chubaofs/chubaofs/proto"
	clog "github.com/chubaofs/chubaofs/util/log"
)

var WorkerCnt int
var WalkCnt int
var QueueSize int
var CfgPath string

type clusterCfg struct {
	Volname string `json:"volname"`
	Addr    string `json:"addr"`
	Owner   string `json:"owner"`
	Ak      string `json:"ak"`
	Sk      string `json:"sk"`
	Cluster string `json:"cluster"`
}

type config struct {
	ClusterCfg []clusterCfg `json:"clusterCfg"`
	LogLevel   string       `json:"logLevel"`
	LogDir     string       `json:"logDir"`
	WalkerCnt  int          `json:"walkerCnt"`
	WorkerCnt  int          `json:"workerCnt"`
	BlkSize    int          `json:"blkSize"`
	QueueSize  int          `json:"queueSize"`
}

type pathCfg struct {
	tp     FsType
	option *proto.MountOptions
	dir    string
}

type Conf struct {
	TraverseJobCnt int
	workerCnt      int
	TaskCnt        int
	BlkSize        int
	SrcDir         *pathCfg
	DestDir        *pathCfg
	Op             opType
}

// cubefs: cfs:m1:ltptest://data1
func (cfg *config) buildPathCfg(dir string) *pathCfg {
	if !strings.HasPrefix(dir, "cfs") || !strings.Contains(dir, "://") {
		return &pathCfg{
			tp:  OsTyp,
			dir: path.Clean(dir),
		}
	}

	var clusterCfg *clusterCfg
	cluster, vol, newDir := parseCfsDir(dir)
	for _, cCfg := range cfg.ClusterCfg {
		if cCfg.Cluster == cluster && cCfg.Volname == vol {
			clusterCfg = &cCfg
			break
		}
	}

	if clusterCfg == nil {
		log.Fatalf("cfs path is illegal, can't find response cluster and volume, path %s", dir)
	}

	opt := &proto.MountOptions{}
	opt.Master = clusterCfg.Addr
	opt.Volname = clusterCfg.Volname
	opt.Owner = clusterCfg.Owner
	opt.AccessKey = clusterCfg.Ak
	opt.SecretKey = clusterCfg.Sk
	opt.NearRead = true

	pCfg := &pathCfg{
		tp:     CubeFsTyp,
		dir:    path.Clean(newDir),
		option: opt,
	}

	return pCfg
}

// cubefs: cfs:m1:ltptest://data1
func parseCfsDir(path string) (cluster, vol, dir string) {
	idx := strings.Index(path, "://")
	if idx <= 0 {
		log.Fatalf("cfs dir is not legal, path %s", path)
	}

	dir = path[idx+3:]

	path = path[:idx]
	arr := strings.Split(path, ":")
	if len(arr) != 3 {
		log.Fatalf("cfs dir is not legal, path %s", path)
	}

	return arr[1], arr[2], dir
}

func (cfg *config) setDefault() {
	if cfg.BlkSize < 1024*1024 {
		cfg.BlkSize = 1024 * 1024
	}

	if WalkCnt > 0 {
		cfg.WalkerCnt = WalkCnt
	}
	if cfg.WalkerCnt <= 0 {
		cfg.WalkerCnt = 10
	}

	if WorkerCnt > 0 {
		cfg.WorkerCnt = WorkerCnt
	}
	if cfg.WorkerCnt <= 0 {
		cfg.WorkerCnt = 10
	}

	if QueueSize > 1024 {
		cfg.QueueSize = QueueSize
	}
	if QueueSize <= 0 {
		cfg.QueueSize = 8192
	}
}

func parseLogLevel(loglvl string) clog.Level {
	switch strings.ToLower(loglvl) {
	case "debug":
		return clog.DebugLevel
	case "info":
		return clog.InfoLevel
	case "warn":
		return clog.WarnLevel
	case "error":
		return clog.ErrorLevel
	default:
		return clog.ErrorLevel
	}
}

func (cfg *config) initLogger() {
	level := parseLogLevel(cfg.LogLevel)
	_, err := clog.InitLog(cfg.LogDir, "client", level, nil)
	if err != nil {
		log.Fatalf("init log dir failed, logdir %s, err %s", cfg.LogDir, err.Error())
	}
}

var defaultCfgPath = "/home/cfs/cfs.json"

func ParseConfig(srcDir, destDir string, op opType) Conf {
	c := Conf{}

	cfg := &config{}
	if CfgPath == "" {
		CfgPath = defaultCfgPath
	}

	err := loadConfig(cfg, CfgPath)
	if err != nil {
		log.Fatalf("load cfg from %s failed, err %s", CfgPath, err.Error())
	}

	cfg.setDefault()
	cfg.initLogger()

	c.Op = op
	c.SrcDir = cfg.buildPathCfg(srcDir)
	c.DestDir = cfg.buildPathCfg(destDir)
	c.TraverseJobCnt = cfg.WalkerCnt
	c.workerCnt = cfg.WorkerCnt
	c.TaskCnt = cfg.QueueSize
	c.BlkSize = cfg.BlkSize

	return c
}

func loadConfig(conf interface{}, configPath string) error {
	if configPath == "" {
		return fmt.Errorf("path can't be empty")
	}

	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("read cfg path failed, path %s, err %s", configPath, err.Error())
	}

	if err := loadConfigBytes(conf, data); err != nil {
		return fmt.Errorf("load cfg failed, path %s , err %s", configPath, err.Error())
	}
	return nil
}

func loadConfigBytes(conf interface{}, data []byte) error {

	data = trimComments(data)

	if err := json.Unmarshal(data, conf); err != nil {
		return fmt.Errorf("unmarshal data from path failed, err %s", err.Error())
	}

	return nil
}

func trimComments(data []byte) []byte {
	lines := bytes.Split(data, []byte("\n"))
	for k, line := range lines {
		lines[k] = trimCommentsLine(line)
	}
	return bytes.Join(lines, []byte("\n"))
}

func trimCommentsLine(line []byte) []byte {

	var newLine []byte
	var i, quoteCount int
	lastIdx := len(line) - 1
	for i = 0; i <= lastIdx; i++ {
		if line[i] == '\\' {
			if i != lastIdx && (line[i+1] == '\\' || line[i+1] == '"') {
				newLine = append(newLine, line[i], line[i+1])
				i++
				continue
			}
		}
		if line[i] == '"' {
			quoteCount++
		}
		if line[i] == '#' {
			if quoteCount%2 == 0 {
				break
			}
		}
		if line[i] == '/' && i < lastIdx && line[i+1] == '/' {
			if quoteCount%2 == 0 {
				break
			}
		}
		newLine = append(newLine, line[i])
	}
	return newLine
}
