package logging

import (
	"fmt"
	"reflect"
	"strings"

	acacia "github.com/humanjuan/acacia/v2"
)

type Logger struct {
	AcaciaLog *acacia.Log
}

func NewLogger(acaciaLog *acacia.Log) *Logger {
	return &Logger{
		AcaciaLog: acaciaLog,
	}
}

func (l *Logger) Info(data interface{}, args ...interface{}) {
	if len(args) == 0 {
		if fields, ok := data.(map[string]interface{}); ok {
			redacted := l.Redact(fields)
			l.AcaciaLog.Info(redacted)
			return
		}
	}
	l.AcaciaLog.Info(data, args...)
}

func (l *Logger) Redact(fields map[string]interface{}) map[string]interface{} {
	res := redactRecursive(fields)
	if m, ok := res.(map[string]interface{}); ok {
		return m
	}
	return fields
}

func redactRecursive(data interface{}) interface{} {
	if data == nil {
		return nil
	}
	val := reflect.ValueOf(data)
	switch val.Kind() {
	case reflect.Map:
		newMap := make(map[string]interface{})
		for _, key := range val.MapKeys() {
			kStr := fmt.Sprint(key.Interface())
			kLower := strings.ToLower(kStr)
			v := val.MapIndex(key).Interface()
			if isSensitiveKey(kLower) {
				newMap[kStr] = "[REDACTED]"
			} else {
				newMap[kStr] = redactRecursive(v)
			}
		}
		return newMap
	case reflect.Slice:
		newSlice := make([]interface{}, val.Len())
		for i := 0; i < val.Len(); i++ {
			newSlice[i] = redactRecursive(val.Index(i).Interface())
		}
		return newSlice
	default:
		return data
	}
}

func isSensitiveKey(key string) bool {
	return strings.Contains(key, "cookie") ||
		strings.Contains(key, "authorization") ||
		strings.Contains(key, "token") ||
		strings.Contains(key, "password") ||
		strings.Contains(key, "secret") ||
		strings.Contains(key, "auth")
}
