package cp

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"

	"github.com/chubaofs/chubaofs/proto"
	clog "github.com/chubaofs/chubaofs/util/log"
)

var (
	WorkerCnt = *flag.Int("workerCnt", 0, "thread count to copy or sync files")
	WalkCnt   = *flag.Int("walkCnt", 0, "thread count to walk dirs")
	QueueSize = *flag.Int("queueSize", 0, "use to store files, waiting to consume")
	CfgPath   = ""
)

type clusterCfg struct {
	Volname   string `json:"volname"`
	Addr      string `json:"addr"`
	Owner     string `json:"owner"`
	Ak        string `json:"ak"`
	Sk        string `json:"sk"`
	Cluster   string `json:"cluster"`
	ClusterId string `json:"cid"`
	Desc      string `json:"desc"`
	Idc       string `json:"idc"`
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

// cubefs: c1://data1
func (cfg *config) buildPathCfg(dir string) *pathCfg {
	if !strings.Contains(dir, "://") {
		return &pathCfg{
			tp:  OsTyp,
			dir: path.Clean(dir),
		}
	}

	var clusterCfg *clusterCfg
	clusterId, newDir := parseCfsDir(dir)
	for _, cCfg := range cfg.ClusterCfg {
		if cCfg.ClusterId == clusterId {
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

	// fmt.Printf("dir: %s, clusterId %s\n", newDir, clusterId)
	return pCfg
}

// cubefs: c1://data1
func parseCfsDir(path string) (clusterId, dir string) {
	idx := strings.Index(path, "://")
	if idx <= 0 {
		log.Fatalf("cfs dir is not legal, path %s", path)
	}

	dir = path[idx+2:]

	clusterId = path[:idx]

	return clusterId, dir
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

	logDir := fmt.Sprintf("/tmp/%s/cfs-logs", getUser().Username)
	if cfg.LogDir != "" {
		logDir = cfg.LogDir
	}

	log.Printf("log dir %s/client", logDir)

	_, err := os.Stat(logDir)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Fatalf("stat logDir %s err %s", logDir, err.Error())
		}

		err = os.MkdirAll(logDir, 0777)
		if err != nil && !os.IsExist(err) {
			log.Fatalf("mk logdir %s err %s", logDir, err.Error())
		}

	}

	_, err = clog.InitLog(logDir, "client", level, nil)
	if err != nil {
		log.Fatalf("init log dir failed, logdir %s, err %s", cfg.LogDir, err.Error())
	}
}

func getCfgPath() string {
	pathName, err := os.Executable()
	if err != nil {
		log.Fatal("get cfg path failed")
	}

	dir := path.Dir(path.Dir(pathName))
	return fmt.Sprintf("%s/conf/cfs-tool.json", dir)
}

func (cfg *config) check() {
	clusterCfgList := cfg.ClusterCfg
	if len(clusterCfgList) == 0 {
		return
	}

	for i := range clusterCfgList {
		for j := range clusterCfgList {
			if i == j {
				continue
			}
			if clusterCfgList[i].ClusterId == clusterCfgList[j].ClusterId {
				log.Fatalf("cluster Id can't be same, id %s", clusterCfgList[i].ClusterId)
			}
		}
	}
}

func ParseConfig(srcDir, destDir string, op opType) Conf {
	c := Conf{}

	cfg := &config{}
	if CfgPath == "" {
		CfgPath = getCfgPath()
	}
	// log.Println("cfg path, ", CfgPath)

	err := loadConfig(cfg, CfgPath)
	if err != nil {
		log.Fatalf("load cfg from %s failed, err %s", CfgPath, err.Error())
	}

	cfg.check()
	cfg.setDefault()
	cfg.initLogger()

	c.Op = op
	c.SrcDir = cfg.buildPathCfg(srcDir)
	c.DestDir = cfg.buildPathCfg(destDir)

	// fileName := getLastDir(c.SrcDir.dir)
	_, fileName := path.Split(c.SrcDir.dir)
	clog.LogDebugf("get fileName %s destDir %s", fileName, c.DestDir.dir)
	c.DestDir.dir = path.Join(c.DestDir.dir, fileName)

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
