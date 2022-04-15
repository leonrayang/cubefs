package cp

import (
	"fmt"
	"os"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	data := []byte(`
	{
		"clusterCfg":[
			# different cfg
			{
				"volname":"ltptest",
				"addr":"localhost",
				"owner":"owner",
				"ak":"ak",
				"sk":"sk",
				"cluster":"marina",
				"desc": "bjht marina volume1",
				"idc":"bjht",
				"cid":"c1"
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

	if !(cfg.WorkerCnt == 10 && cfg.WalkerCnt == 10 && cfg.BlkSize == 4096) {
		t.Fail()
	}

	assertTrue(t, cfg.QueueSize == 4096 && cfg.LogLevel == "warn" && cfg.LogDir == "/home/service")
	clusterCfg := cfg.ClusterCfg[0]
	assertTrue(t, clusterCfg.Volname == "ltptest" && clusterCfg.Addr == "localhost")
	assertTrue(t, clusterCfg.Owner == "owner" && clusterCfg.Ak == "ak" && clusterCfg.Desc == "bjht marina volume1")
	assertTrue(t, clusterCfg.Sk == "sk" && clusterCfg.Cluster == "marina" && clusterCfg.ClusterId == "c1")
	t.Log("cfg", clusterCfg)
	if clusterCfg.Idc != "bjht" {
		t.Fail()
	}

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
				"cluster":"m1",
				"cid":"c1"
			},
			{
				"volname":"ltptest",
				"addr":"localhost",
				"owner":"owner",
				"ak":"ak",
				"sk":"sk",
				"cluster":"m2",
				"cid":"c2"
			}
		],
		"logLevel":"warn",
		"logDir":"/home/service",
		"walkerCnt":10,
		"workerCnt":10,
		"blkSize":4096,
		"queueSize":4096
	}`)

	CfgPath = "/root/cfs.json"
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

	cfgCubefs := ParseConfig("c1://d1", "c2://d2", CopyOp)
	fmt.Printf("src tp %d, dest tp %d \n", cfgCubefs.SrcDir.tp, cfgCubefs.DestDir.tp)
	if cfgCubefs.SrcDir.tp != CubeFsTyp {
		t.Fail()
	}

	if cfgCubefs.DestDir.tp != CubeFsTyp {
		t.Fail()
	}
}

func assertTrue(t *testing.T, con bool) {
	if con {
		return
	}
	t.Fail()
}
