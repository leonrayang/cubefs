package log

import (
	"fmt"
	"os"
	"strings"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

type Config struct {
	LogFile    string `json:"logfile"`    //日志文件名
	LogLevel   string `json:"level"`      //日志级别
	MaxSize    int    `json:"maxsize"`    //单个日志文件的大小
	MaxBackups int    `json:"maxbackups"` //保留的日志文件个数
	MaxAge     int    `json:"maxage"`     //日志保留的最长时间: 天
	Compress   bool   `json:"compress"`   //日志是否压缩
}

type Logger struct {
	*zap.Logger
	zap.AtomicLevel
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

func NewStdLogger(level string) *Logger {
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:       "time",
		LevelKey:      "level",
		NameKey:       "logger",
		CallerKey:     "file",
		MessageKey:    "msg",
		StacktraceKey: "stacktrace",
		FunctionKey:   "func",
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
	atomicLevel.SetLevel(strToLevel(level))
	// output should also go to standard out.
	core := zapcore.NewCore(zapcore.NewJSONEncoder(encoderConfig), zapcore.Lock(os.Stdout), atomicLevel)

	return &Logger{
		Logger:      zap.New(core, zap.AddCaller()),
		AtomicLevel: atomicLevel,
	}
}

func NewDefaultLog(dir string, role string) *Logger {
	cfg := &Config{
		LogFile:    fmt.Sprintf("%s/%s.log", dir, role),
		MaxSize:    100,
		MaxBackups: 100,
		MaxAge:     30,
		Compress:   false,
		LogLevel:   "info",
	}

	return NewLogger(cfg)
}

func NewLogger(cfg *Config) *Logger {
	hook := &lumberjack.Logger{
		Filename:   cfg.LogFile,
		MaxSize:    cfg.MaxSize,
		MaxBackups: cfg.MaxBackups,
		MaxAge:     cfg.MaxAge,
		LocalTime:  true,
		Compress:   cfg.Compress,
	}

	encoderConfig := zapcore.EncoderConfig{
		TimeKey:       "time",
		LevelKey:      "level",
		NameKey:       "logger",
		CallerKey:     "file",
		MessageKey:    "msg",
		StacktraceKey: "stacktrace",
		FunctionKey:   "func",
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
	core := zapcore.NewCore(zapcore.NewJSONEncoder(encoderConfig), zapcore.AddSync(hook), atomicLevel)

	return &Logger{
		Logger:      zap.New(core, zap.AddCaller()),
		AtomicLevel: atomicLevel,
		hook:        hook,
	}
}

func (log *Logger) SetStrLevel(strLevel string) {
	level := strToLevel(strLevel)
	log.AtomicLevel.SetLevel(level)
}

func (log *Logger) Close() error {
	if log.hook != nil {
		defer log.hook.Close()
	}
	return log.Sync()
}

func (log *Logger) With(fields ...Field) *Logger {
	return &Logger{
		Logger: log.Logger.With(fields...),
	}
}

func Any(key string, value interface{}) Field {
	return zap.Any(key, value)
}

func Array(key string, val zapcore.ArrayMarshaler) Field {
	return zap.Array(key, val)
}

func Binary(key string, val []byte) Field {
	return zap.Binary(key, val)
}

func Bool(key string, val bool) Field {
	return zap.Bool(key, val)
}

func Bools(key string, bs []bool) Field {
	return zap.Bools(key, bs)
}

func ByteString(key string, val []byte) Field {
	return zap.ByteString(key, val)
}

func ByteStrings(key string, val [][]byte) Field {
	return zap.ByteStrings(key, val)
}

func Duration(key string, val time.Duration) Field {
	return zap.Duration(key, val)
}

func Durations(key string, ds []time.Duration) Field {
	return zap.Durations(key, ds)
}

func Error(err error) Field {
	return zap.Error(err)
}

func NamedError(key string, err error) Field {
	return zap.NamedError(key, err)
}

func Namespace(key string) Field {
	return zap.Namespace(key)
}

func Errors(key string, errs []error) Field {
	return zap.Errors(key, errs)
}

func Float64(key string, val float64) Field {
	return zap.Float64(key, val)
}

func Float64s(key string, vals []float64) Field {
	return zap.Float64s(key, vals)
}

func Float32(key string, val float32) Field {
	return zap.Float32(key, val)
}

func Float32s(key string, vals []float32) Field {
	return zap.Float32s(key, vals)
}

func Int(key string, val int) Field {
	return zap.Int(key, val)
}

func Ints(key string, vals []int) Field {
	return zap.Ints(key, vals)
}

func Uint(key string, val uint) Field {
	return zap.Uint(key, val)
}

func Uints(key string, vals []uint) Field {
	return zap.Uints(key, vals)
}

func Int64(key string, val int64) Field {
	return zap.Int64(key, val)
}

func Int64s(key string, vals []int64) Field {
	return zap.Int64s(key, vals)
}

func Uint64(key string, val uint64) Field {
	return zap.Uint64(key, val)
}

func Uint64s(key string, vals []uint64) Field {
	return zap.Uint64s(key, vals)
}

func Int32(key string, val int32) Field {
	return zap.Int32(key, val)
}

func Int32s(key string, vals []int32) Field {
	return zap.Int32s(key, vals)
}

func Uint32(key string, val uint32) Field {
	return zap.Uint32(key, val)
}

func Uint32s(key string, vals []uint32) Field {
	return zap.Uint32s(key, vals)
}

func Int16(key string, val int16) Field {
	return zap.Int16(key, val)
}

func Int16s(key string, vals []int16) Field {
	return zap.Int16s(key, vals)
}

func Uint16(key string, val uint16) Field {
	return zap.Uint16(key, val)
}

func Uint16s(key string, vals []uint16) Field {
	return zap.Uint16s(key, vals)
}

func Int8(key string, val int8) Field {
	return zap.Int8(key, val)
}

func Int8s(key string, vals []int8) Field {
	return zap.Int8s(key, vals)
}

func Uint8(key string, val uint8) Field {
	return zap.Uint8(key, val)
}

func Uint8s(key string, vals []uint8) Field {
	return zap.Uint8s(key, vals)
}

func String(key string, val string) Field {
	return zap.String(key, val)
}

func Strings(key string, ss []string) Field {
	return zap.Strings(key, ss)
}

func Time(key string, val time.Time) Field {
	return zap.Time(key, val)
}

func Times(key string, ts []time.Time) Field {
	return zap.Times(key, ts)
}

func Stack(key string) Field {
	return zap.Stack(key)
}

func Object(key string, val zapcore.ObjectMarshaler) Field {
	return zap.Object(key, val)
}
