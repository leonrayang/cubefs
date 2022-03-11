package cp

import (
	"os"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	data := []byte(`
	{
		"clusterCfg":[
			{
				"volname":"ltptest",
				"addr":"localhost",
				"owner":"owner",
				"ak":"ak",
				"sk":"sk",
				"cluster":"m1"
			}
		],
		"logLevel":"warn",
		"logDir":"/home/service",
		"walkerCnt":10,
		"workerCnt":10,
		"blkSize":4096,
		"queueSize":4096
	}`)

	cfgPath := "./cfgPath"
	fd, err := os.Create(cfgPath)
	defer os.Remove(cfgPath)
	if err != nil {
		t.Error(err)
	}

	fd.Write(data)

	cfg := &config{}
	err = loadConfig(cfg, cfgPath)
	if err != nil {
		t.Error(err)
	}

	assertTrue(t, len(cfg.ClusterCfg) == 1)

	assertTrue(t, cfg.WorkerCnt == 10 && cfg.WalkerCnt == 10 && cfg.BlkSize == 4096)
	assertTrue(t, cfg.QueueSize == 4096 && cfg.LogLevel == "warn" && cfg.LogDir == "/home/service")
	clusterCfg := cfg.ClusterCfg[0]
	assertTrue(t, clusterCfg.Volname == "ltptest" && clusterCfg.Addr == "localhost")
	assertTrue(t, clusterCfg.Owner == "owner" && clusterCfg.Ak == "ak")
	assertTrue(t, clusterCfg.Sk == "sk" && clusterCfg.Cluster == "m1")
}

func TestParseCfg(t *testing.T) {
	data := []byte(`
	{
		"clusterCfg":[
			{
				"volname":"ltptest",
				"addr":"localhost",
				"owner":"owner",
				"ak":"ak",
				"sk":"sk",
				"cluster":"m1"
			},
			{
				"volname":"ltptest",
				"addr":"localhost",
				"owner":"owner",
				"ak":"ak",
				"sk":"sk",
				"cluster":"m2"
			}
		],
		"logLevel":"warn",
		"logDir":"/home/service",
		"walkerCnt":10,
		"workerCnt":10,
		"blkSize":4096,
		"queueSize":4096
	}`)

	CfgPath = "/root/cfs/cfs.json"
	cfgPath := CfgPath
	fd, err := os.Create(cfgPath)
	defer os.Remove(cfgPath)
	if err != nil {
		t.Error(err)
	}

	fd.Write(data)

	cfg := ParseConfig("/d1", "/d2", CopyOp)
	assertTrue(t, cfg.SrcDir.tp == OsTyp)
	assertTrue(t, cfg.DestDir.tp == OsTyp)

	cfgCubefs := ParseConfig("cfs:m1:ltptest://d1", "cfs:m1:ltptest://d2", CopyOp)
	assertTrue(t, cfgCubefs.SrcDir.tp == CubeFsTyp)
	assertTrue(t, cfgCubefs.DestDir.tp == CubeFsTyp)

}

func assertTrue(t *testing.T, con bool) {
	if con {
		return
	}
	t.Fail()
}
