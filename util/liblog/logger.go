package liblog

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Config struct {
	ScreenOutput bool   `json:"screenOutput"` // 是否输出到stdout
	LogFile      string `json:"logfile"`      //日志文件名
	LogLevel     string `json:"level"`        //日志级别
	MaxSizeMB    int    `json:"maxsizeMB"`    //单个日志文件的大小
	MaxBackups   int    `json:"maxbackups"`   //保留的日志文件个数
	MaxAge       int    `json:"maxage"`       //日志保留的最长时间: 天
	Compress     bool   `json:"compress"`     //日志是否压缩
}

type Logger struct {
	*zap.Logger
	hook *lumberjack.Logger
}

type Field = zap.Field

func strToLevel(s string) (level zapcore.Level) {
	switch strings.ToLower(s) {
	case "debug":
		level = zap.DebugLevel
	case "info":
		level = zap.InfoLevel
	case "warn":
		level = zap.WarnLevel
	case "error":
		level = zap.ErrorLevel
	case "dpanic":
		level = zap.DPanicLevel
	case "panic":
		level = zap.PanicLevel
	case "fatal":
		level = zap.FatalLevel
	default:
		level = zap.InfoLevel
	}
	return
}

func NewZapLoggerWithLevel(cfg *Config) (*zap.Logger, zap.AtomicLevel) {
	var (
		s    zapcore.WriteSyncer
		core zapcore.Core
	)
	if cfg.LogFile != "" {
		os.MkdirAll(filepath.Dir(cfg.LogFile), 0755)
		hook := &lumberjack.Logger{
			Filename:   cfg.LogFile,
			MaxSize:    cfg.MaxSizeMB,
			MaxBackups: cfg.MaxBackups,
			MaxAge:     cfg.MaxAge,
			LocalTime:  true,
			Compress:   cfg.Compress,
		}
		s = zapcore.AddSync(hook)
	}

	encoderConfig := zapcore.EncoderConfig{
		TimeKey:       "time",
		LevelKey:      "level",
		NameKey:       "logger",
		CallerKey:     "file",
		MessageKey:    "msg",
		StacktraceKey: "stack",
		LineEnding:    zapcore.DefaultLineEnding,
		EncodeLevel:   zapcore.CapitalLevelEncoder,
		EncodeTime: func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(t.Format("2006-01-02 15:04:05.000"))
		}, // 时间格式
		EncodeDuration: zapcore.SecondsDurationEncoder, //
		EncodeCaller:   zapcore.ShortCallerEncoder,     // 路径编码器
		EncodeName:     zapcore.FullNameEncoder,
	}

	// 设置日志级别
	atomicLevel := zap.NewAtomicLevel()
	atomicLevel.SetLevel(strToLevel(cfg.LogLevel))
	if cfg.ScreenOutput {
		if s != nil {
			s = zapcore.NewMultiWriteSyncer(s, os.Stdout)
		} else {
			s = zapcore.AddSync(os.Stdout)
		}
	}
	if s != nil {
		core = zapcore.NewCore(zapcore.NewJSONEncoder(encoderConfig), s, atomicLevel)
	} else {
		core = zapcore.NewNopCore()
	}
	return zap.New(core, zap.AddCaller()), atomicLevel
}
