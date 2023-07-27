package log

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestNewLogger(t *testing.T) {
	cfg := Config{
		LogFile:    "./test.log",
		LogLevel:   "debug",
		MaxSize:    1,
		MaxBackups: 1,
		MaxAge:     1,
		Compress:   false,
	}

	logger := NewLogger(&cfg)
	defer os.Remove(cfg.LogFile)
	logger.Debug("test", Int("i", 10))
	logger.Info("test", String("iur", "jdaurokdua"))
	logger.Warn("test", Time("time", time.Now()))
	logger.Error("test", NamedError("error", errors.New("dkjsahirueiuh")))
	//logger.Panic("test", zap.String("str", "isaurihkhgiu"))
	//logger.Fatal("test", zap.String("str", "isaurihkhgiu"))
}

func TestStrToLevel(t *testing.T) {
	level := strToLevel("debug")
	if level != zap.DebugLevel {
		t.Errorf("level=%d not DebugLevel", int(level))
	}

	level = strToLevel("dEbug")
	if level != zap.DebugLevel {
		t.Errorf("level=%d not DebugLevel", int(level))
	}

	level = strToLevel("dEbuge")
	if level == zap.DebugLevel {
		t.Errorf("level=%d DebugLevel", int(level))
	}

	if level != zap.InfoLevel {
		t.Errorf("level=%d not InfoLevel", int(level))
	}

	level = strToLevel("info")
	if level != zap.InfoLevel {
		t.Errorf("level=%d not InfoLevel", int(level))
	}

	level = strToLevel("Info")
	if level != zap.InfoLevel {
		t.Errorf("level=%d not InfoLevel", int(level))
	}

	level = strToLevel("Infoe") //默认info级别
	if level != zap.InfoLevel {
		t.Errorf("level=%d not InfoLevel", int(level))
	}

	level = strToLevel("warn")
	if level != zap.WarnLevel {
		t.Errorf("level=%d not WarnLevel", int(level))
	}

	level = strToLevel("Warn")
	if level != zap.WarnLevel {
		t.Errorf("level=%d not WarnLevel", int(level))
	}

	level = strToLevel("Warne") //默认info级别
	if level != zap.InfoLevel {
		t.Errorf("level=%d not InfoLevel", int(level))
	}

	level = strToLevel("error")
	if level != zap.ErrorLevel {
		t.Errorf("level=%d not ErrorLevel", int(level))
	}

	level = strToLevel("Error")
	if level != zap.ErrorLevel {
		t.Errorf("level=%d not ErrorLevel", int(level))
	}

	level = strToLevel("Errorr") //默认info级别
	if level != zap.InfoLevel {
		t.Errorf("level=%d not InfoLevel", int(level))
	}

	level = strToLevel("panic")
	if level != zap.PanicLevel {
		t.Errorf("level=%d not PanicLevel", int(level))
	}

	level = strToLevel("Panic")
	if level != zap.PanicLevel {
		t.Errorf("level=%d not PanicLevel", int(level))
	}

	level = strToLevel("Panicc") //默认info级别
	if level != zap.InfoLevel {
		t.Errorf("level=%d not InfoLevel", int(level))
	}

	level = strToLevel("fatal")
	if level != zap.FatalLevel {
		t.Errorf("level=%d not FatalLevel", int(level))
	}

	level = strToLevel("Fatal")
	if level != zap.FatalLevel {
		t.Errorf("level=%d not FatalLevel", int(level))
	}

	level = strToLevel("Fatall") //默认info级别
	if level != zap.InfoLevel {
		t.Errorf("level=%d not InfoLevel", int(level))
	}
}

func TestHttpSetLevel(t *testing.T) {
	cfg := Config{
		LogFile:    "./http_test.log",
		LogLevel:   "debug",
		MaxSize:    1,
		MaxBackups: 1,
		MaxAge:     1,
		Compress:   false,
	}

	logger := NewLogger(&cfg)
	defer func() {
		logger.Close()
		os.Remove(cfg.LogFile)
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/log/level", logger.ServeHTTP)

	logger.Info("TestHttpSetLevel")

	reader := strings.NewReader(`{"level":"error"}`)
	r, _ := http.NewRequest(http.MethodPut, "/log/level", reader)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Response code is %v", resp.StatusCode)
	}
	logger.Info("TestHttpSetLevel")
}

func TestLogWith(t *testing.T) {
	cfg := Config{
		LogFile:    "./test.log",
		LogLevel:   "debug",
		MaxSize:    1,
		MaxBackups: 1,
		MaxAge:     1,
		Compress:   false,
	}

	logger := NewLogger(&cfg)
	defer func() {
		logger.Close()
		os.Remove(cfg.LogFile)
	}()

	l := logger.With(String("reqid", "djaiuejrhtiu"))
	l.Info("this is a test log with", Int("counter", 5))
	ll := l.With(String("name", "zhangshan"))
	ll.Info("this is a test log with")
}
