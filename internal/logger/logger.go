package logger

import (
	"go.uber.org/zap"
)

var Log = zap.NewNop()

func Init() {
	config := zap.NewProductionConfig()
	config.Level = zap.NewAtomicLevelAt(zap.InfoLevel)

	var err error
	Log, err = config.Build()
	if err != nil {
		panic(err)
	}
}
