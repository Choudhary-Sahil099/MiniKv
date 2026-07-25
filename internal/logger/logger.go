package logger

import (
	"go.uber.org/zap"
)

var Log = zap.NewNop()
func Init() {

	var err error

	Log, err = zap.NewProduction()

	if err != nil {
		panic(err)
	}
}
