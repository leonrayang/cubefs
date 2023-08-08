package config

import (
	"github.com/cubefs/cubefs/util/liblog"
	"github.com/cubefs/cubefs/util/mail"
	"github.com/cubefs/cubefs/util/migrate/config"
)

type Config struct {
	Port                      int              `json:"port"`
	LogCfg                    *liblog.Config   `json:"log"`
	MailCfg                   *mail.Config     `json:"mail"`
	FalconRoute               []FalconRouteCfg `json:"falconRoute"`
	FalconAddr                string           `json:"falconAddr"`
	SdkLogCfg                 sdkLog           `json:"sdklog"`
	TaskRetryLimit            int              `json:"retry"`
	TraverseDirGoroutineLimit int              `json:"traverseDirGoroutine"`
	SummaryGoroutineLimit     int              `json:"summaryGoroutine"`
	FailTaskReportLimit       int              `json:"failTaskReportLimit"`
	CompleteJobTimeout        int              `json:"completeJobTimeout"`
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
	if cfg.TraverseDirGoroutineLimit <= 0 {
		cfg.TraverseDirGoroutineLimit = 10
	}
	if cfg.SummaryGoroutineLimit <= 0 {
		cfg.SummaryGoroutineLimit = 8
	}
	//失败的时候最多30个失败任务信息，避免过长
	if cfg.FailTaskReportLimit <= 0 {
		cfg.FailTaskReportLimit = 30
	}
	if cfg.CompleteJobTimeout <= 0 {
		cfg.CompleteJobTimeout = 60 * 24 * 5
	}
	return cfg, nil
}
