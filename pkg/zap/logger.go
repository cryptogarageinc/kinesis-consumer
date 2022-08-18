package zap

import (
	"fmt"

	kinesis_consumer "github.com/cryptogarageinc/kinesis-consumer"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Logger struct {
	logger *zap.Logger
}

var logLevelMap = map[kinesis_consumer.LogState]zapcore.Level{
	kinesis_consumer.LogError: zapcore.ErrorLevel,
	kinesis_consumer.LogWarn:  zapcore.WarnLevel,
	kinesis_consumer.LogInfo:  zapcore.InfoLevel,
	kinesis_consumer.LogDebug: zapcore.DebugLevel,
}

func (l Logger) Log(args ...interface{}) {
	level := zapcore.InfoLevel
	var msg string
	fields := make([]zap.Field, 0, len(args))
	for _, arg := range args {
		switch arg := arg.(type) {
		case kinesis_consumer.LogState:
			if lvl, ok := logLevelMap[arg]; ok {
				level = lvl
			}
		case kinesis_consumer.LogFunc:
			fields = append(fields, zap.String("consumerEventType", arg.Value()))
		case *kinesis_consumer.ErrorValue:
			fields = append(fields, zap.Error(arg.Err))
		case *kinesis_consumer.KeyValueString:
			fields = append(fields, zap.String(arg.Key, arg.Value))
		case string:
			if msg == "" {
				msg = arg
			} else {
				fields = append(fields, zap.String("text", arg))
			}
		default:
			fields = append(fields, zap.String("text", fmt.Sprintf("%s", arg)))
		}
	}

	if ce := l.logger.Check(level, msg); ce != nil {
		ce.Write(fields...)
	}
}

func NewConsumerLogger(logger *zap.Logger) Logger {
	return Logger{logger: logger}
}
