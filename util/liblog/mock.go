package liblog

import "go.uber.org/zap"

func NewMockLogger() *zap.Logger {
	cfg := &Config{
		ScreenOutput: true,
		LogLevel:     "debug",
	}
	logger, _ := NewZapLoggerWithLevel(cfg)
	return logger
}
