package consumer

import (
	"fmt"
	"log"
)

var (
	LogConsumer LogFunc = "CONSUMER"
	LogGroup    LogFunc = "GROUP"

	LogError LogState = "ERROR"
	LogWarn  LogState = "WARNING"
	LogInfo  LogState = "INFO"
	LogDebug LogState = "DEBUG"
)

// A Logger is a minimal interface to as a adaptor for external logging library to consumer
type Logger interface {
	Log(...interface{})
}

type ErrorValue struct {
	Err error
}

func Error(err error) *ErrorValue {
	return &ErrorValue{Err: err}
}

func (l *ErrorValue) String() string {
	if l.Err == nil {
		return "error=nil"
	}
	return l.Err.Error()
}

func (l *ErrorValue) Error() string {
	if l.Err == nil {
		return "nil"
	}
	return l.Err.Error()
}

type KeyValueString struct {
	Key   string
	Value string
}

func LogString(key, value string) *KeyValueString {
	return &KeyValueString{Key: key, Value: value}
}

func (l *KeyValueString) String() string {
	return fmt.Sprintf("%s=%s", l.Key, l.Value)
}

type LogState string

func (l LogState) String() string {
	return "[" + string(l) + "]"
}

func (l LogState) Value() string {
	return string(l)
}

type LogFunc string

func (l LogFunc) String() string {
	return "[" + string(l) + "]"
}

func (l LogFunc) Value() string {
	return string(l)
}

// noopLogger implements logger interface with discard
type noopLogger struct {
	logger *log.Logger
}

// Log using stdlib logger. See log.Println.
func (l noopLogger) Log(args ...interface{}) {
	l.logger.Println(args...)
}
