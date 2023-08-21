package config

import (
	"github.com/cubefs/cubefs/util/liblog"
	"github.com/cubefs/cubefs/util/migrate/config"
)

type Config struct {
	Server             string           `json:"server"`
	Port               int              `json:"port"`
	LogCfg             *liblog.Config   `json:"log"`
	FalconRoute        []FalconRouteCfg `json:"falconRoute"`
	FalconAddr         string           `json:"falconAddr"`
	SdkLogCfg          sdkLog           `json:"sdklog"`
	JobCnt             int              `json:"jobCnt"`
	CopyGoroutineLimit int              `json:"copyLimit"`
	CopyQueueLimit     int              `json:"copyQueueLimit"`
	PprofPort          string           `json:"pprof"`
	TinyFactor         int              `json:"TinyFactor"`
}

type sdkLog struct {
	LogDir   string `json:"dir"`
	LogLevel string `json:"level"`
}
type FalconRouteCfg struct {
	Name string `json:"name"`
	Addr string `json:"addr"`
}

func ParseConfig(path string) (*Config, error) {
	cfg := &Config{}
	err := config.LoadConfig(cfg, path)
	if err != nil {
		return nil, err
	}
	if cfg.CopyGoroutineLimit <= 0 {
		cfg.CopyGoroutineLimit = 10
	}
	if cfg.CopyQueueLimit <= 0 {
		cfg.CopyQueueLimit = 4096
	}
	if cfg.JobCnt <= 0 {
		cfg.JobCnt = 30
	}
	if cfg.TinyFactor <= 0 {
		cfg.TinyFactor = 80
	}
	return cfg, nil
}
