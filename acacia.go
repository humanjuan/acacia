////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//                                                                                                                    //
//  Author: Juan Alejandro Perez Chandia                                                                              //
//  Contact: juan.alejandro@humanjuan.com                                                                             //
//  Website: https://humanjuan.com/                                                                                   //
//                                                                                                                    //
//  HumanJuan Acacia - High-performance concurrent logger with real file rotation                                     //
//                                                                                                                    //
//  Version: 2.3.1                                                                                                    //
//                                                                                                                    //
//  MIT License                                                                                                       //
//                                                                                                                    //
//  Copyright (c) 2020 Juan Alejandro                                                                                 //
//                                                                                                                    //
//  Permission is hereby granted, free of charge, to any person obtaining a copy                                      //
//  of this software and associated documentation files (the "Software"), to deal                                     //
//  in the Software without restriction, including without limitation the rights                                      //
//  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell                                         //
//  copies of the Software, and to permit persons to whom the Software is                                             //
//  furnished to do so, subject to the following conditions:                                                          //
//                                                                                                                    //
//  The above copyright notice and this permission notice shall be included in all                                    //
//  copies or substantial portions of the Software.                                                                   //
//                                                                                                                    //
//  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR                                        //
//  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,                                          //
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE                                       //
//  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER                                            //
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,                                     //
//  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE                                     //
//  SOFTWARE.                                                                                                         //
//                                                                                                                    //
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

package acacia

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	version                     = "2.3.1"
	DefaultBufferSize           = 500_000
	MinBufferSize               = 1_000
	DefaultBatchSize            = 64 * 1024 // 64 KB (deprecated: use bufferCap)
	flushInterval               = 100 * time.Millisecond
	cacheInterval               = 100 * time.Millisecond
	lastDayFormat               = "2006-01-02"
	defaultBufferInitialCap     = 64 * 1024
	defaultBufferFlushThreshold = 256 * 1024
	defaultBufferMaxRetainedCap = 512 * 1024
)

var (
	timestampFormat = TS.Special
)

var (
	levelDebug    = []byte("DEBUG")
	levelInfo     = []byte("INFO")
	levelWarn     = []byte("WARN")
	levelError    = []byte("ERROR")
	levelCritical = []byte("CRITICAL")
)

type BackpressurePolicy int

const (
	DropNewest BackpressurePolicy = iota
	BlockOnFull
	DropOldest
	DropDebugOnly
)

const (
	stateOpen uint32 = iota
	stateClosing
	stateClosed
)

var (
	ErrInvalidLogFilename  = errors.New("invalid log filename")
	ErrInvalidLogDirectory = errors.New("invalid log directory")
	ErrUnsafeSymlink       = errors.New("unsafe symlink")
	ErrInsecureFileMode    = errors.New("insecure file mode")
)

type config struct {
	bufferSize         int
	batchSize          int // Deprecated: prefer bufferCap for internal buffering capacity
	flushEvery         time.Duration
	bufferCap          int
	drainBurst         int
	dropOnFull         bool
	backpressurePolicy BackpressurePolicy
	fileMode           os.FileMode
	dirMode            os.FileMode
}

type Option func(*config)

func WithBufferSize(number int) Option {
	return func(conf *config) {
		if number >= MinBufferSize {
			conf.bufferSize = number
		}
	}
}

// WithBatchSize is deprecated: prefer WithBufferCap to size the internal writer buffers.
func WithBatchSize(number int) Option {
	return func(conf *config) {
		if number > 1024 {
			conf.batchSize = number
		}
	}
}

// WithFlushInterval sets the periodic flush interval for the writer.
func WithFlushInterval(d time.Duration) Option {
	return func(conf *config) {
		if d > 0 {
			conf.flushEvery = d
		}
	}
}

// WithBufferCap sets the initial capacity (in bytes) for the internal writer buffers.
// Use this to avoid re-allocations when logging large messages.
func WithBufferCap(bytes int) Option {
	return func(conf *config) {
		if bytes > 0 {
			conf.bufferCap = bytes
		}
	}
}

// WithDrainBurst sets how many events the writer drains opportunistically per wake-up.
func WithDrainBurst(n int) Option {
	return func(conf *config) {
		if n > 0 {
			conf.drainBurst = n
		}
	}
}

// WithDropOnFull enables dropping log events when the internal queue is full.
func WithDropOnFull(drop bool) Option {
	return func(conf *config) {
		conf.dropOnFull = drop
		if drop {
			conf.backpressurePolicy = DropNewest
		} else {
			conf.backpressurePolicy = BlockOnFull
		}
	}
}

// WithBackpressurePolicy sets the queue backpressure policy.
func WithBackpressurePolicy(policy BackpressurePolicy) Option {
	return func(conf *config) {
		conf.backpressurePolicy = policy
		if policy == BlockOnFull {
			conf.dropOnFull = false
		} else {
			conf.dropOnFull = true
		}
	}
}

// WithFileMode sets the file permissions mode for the active log file and backups.
func WithFileMode(mode os.FileMode) Option {
	return func(conf *config) {
		// Do not accept world-writable or group-writable bits
		if mode&0002 != 0 {
			mode &^= 0002
		}
		if mode&0020 != 0 {
			mode &^= 0020
		}
		conf.fileMode = mode
	}
}

// WithDirMode sets the directory permissions mode.
func WithDirMode(mode os.FileMode) Option {
	return func(conf *config) {
		mode &^= 0022
		conf.dirMode = mode
	}
}

type tsFormat struct {
	ANSIC       string // "Mon Jan _2 15:04:05 2006"
	UnixDate    string // "Mon Jan _2 15:04:05 MST 2006"
	RubyDate    string // "Mon Jan 02 15:04:05 -0700 2006"
	RFC822      string // "02 Jan 06 15:04 MST"
	RFC822Z     string // "02 Jan 06 15:04 -0700"
	RFC850      string // "Monday, 02-Jan-06 15:04:05 MST"
	RFC1123     string // "Mon, 02 Jan 2006 15:04:05 MST"
	RFC1123Z    string // "Mon, 02 Jan 2006 15:04:05 -0700"
	RFC3339     string // "2006-01-02T15:04:05Z07:00"
	RFC3339Nano string // "2006-01-02T15:04:05.999999999Z07:00"
	Kitchen     string // "3:04PM"
	Special     string // "Jan 2, 2006 15:04:05.000000 MST"
	Stamp       string // "Jan _2 15:04:05"
	StampMilli  string // "Jan _2 15:04:05.000"
	StampMicro  string // "Jan _2 15:04:05.000000"
	StampNano   string // "Jan _2 15:04:05.000000000"
}

var TS = tsFormat{
	ANSIC:       "Mon Jan _2 15:04:05 2006",
	UnixDate:    "Mon Jan _2 15:04:05 MST 2006",
	RubyDate:    "Mon Jan 02 15:04:05 -0700 2006",
	RFC822:      "02 Jan 06 15:04 MST",
	RFC822Z:     "02 Jan 06 15:04 -0700",
	RFC850:      "Monday, 02-Jan-06 15:04:05 MST",
	RFC1123:     "Mon, 02 Jan 2006 15:04:05 MST",
	RFC1123Z:    "Mon, 02 Jan 2006 15:04:05 -0700",
	RFC3339:     "2006-01-02T15:04:05Z07:00",
	RFC3339Nano: "2006-01-02T15:04:05.999999999Z07:00",
	Kitchen:     "3:04PM",
	Special:     "Jan 2, 2006 15:04:05.000000 MST",
	Stamp:       "Jan _2 15:04:05",
	StampMilli:  "Jan _2 15:04:05.000",
	StampMicro:  "Jan _2 15:04:05.000000",
	StampNano:   "Jan _2 15:04:05.000000000",
}

type getLevel struct {
	DEBUG    string
	INFO     string
	WARN     string
	ERROR    string
	CRITICAL string
}

var Level = getLevel{
	// DEBUG < INFO < WARN < ERROR < CRITICAL
	DEBUG:    "DEBUG",
	INFO:     "INFO",
	WARN:     "WARN",
	ERROR:    "ERROR",
	CRITICAL: "CRITICAL",
}

type Log struct {
	name, path, level  string
	structured         bool
	status             bool
	maxSize            int64
	maxRotation        int
	daily              bool
	lastDay            string
	file               atomic.Value
	events             chan logEvent
	wg                 sync.WaitGroup
	mtx                sync.Mutex
	buffer             []byte
	writeBuf           []byte
	flushEvery         time.Duration
	cachedTime         atomic.Value
	timeTicker         *time.Ticker
	done               chan struct{}
	closeOnce          sync.Once
	forceDailyRotate   bool
	enqueueSeq         uint64
	dequeueSeq         uint64
	control            chan controlReq
	currentSize        int64
	currentReq         controlReq
	drainBurstLimit    int
	dropOnFull         bool
	droppedSeq         uint64
	backpressurePolicy BackpressurePolicy
	enqueuedSeq        uint64
	writeErrorsSeq     uint64
	rotationErrorsSeq  uint64
	fileMode           os.FileMode
	dirMode            os.FileMode
	state              uint32
}

// controlReq is a control message for the writer goroutine.
// target indicates the last enqueued sequence that must be processed
// (and flushed) before acknowledging the request.
type controlReq struct {
	target      uint64
	ack         chan struct{}
	setRotation *rotationConfig
	setDaily    *bool
	isClose     bool
}

type rotationConfig struct {
	sizeMB int
	backup int
}

// logEvent is a lightweight message enqueued by producers.
// The writer goroutine performs the final formatting to minimize allocations.
type logEvent struct {
	level    string
	msgStr   string
	msgBytes []byte
	kind     uint8 // 0 = string, 1 = bytes, 2 = preformatted JSON
	seq      uint64
}

var (
	pool512     = sync.Pool{New: func() interface{} { return make([]byte, 0, 512) }}
	pool2k      = sync.Pool{New: func() interface{} { return make([]byte, 0, 2048) }}
	pool4k      = sync.Pool{New: func() interface{} { return make([]byte, 0, 4096) }}
	pool8k      = sync.Pool{New: func() interface{} { return make([]byte, 0, 8192) }}
	jsonPool512 = sync.Pool{New: func() interface{} { return make([]byte, 0, 512) }}
	jsonPool2K  = sync.Pool{New: func() interface{} { return make([]byte, 0, 2048) }}
	jsonPool4K  = sync.Pool{New: func() interface{} { return make([]byte, 0, 4096) }}
	jsonPool8K  = sync.Pool{New: func() interface{} { return make([]byte, 0, 8192) }}
)

func getBufCap(n int) []byte {
	switch {
	case n <= 512:
		return pool512.Get().([]byte)[:0]
	case n <= 2048:
		return pool2k.Get().([]byte)[:0]
	case n <= 4096:
		return pool4k.Get().([]byte)[:0]
	case n <= 8192:
		return pool8k.Get().([]byte)[:0]
	default:
		return make([]byte, 0, n)
	}
}

// putBufCap returns the buffer to the appropriate pool based on capacity.
func putBufCap(b []byte) {
	c := cap(b)
	switch {
	case c <= 512:
		pool512.Put(b[:0])
	case c <= 2048:
		pool2k.Put(b[:0])
	case c <= 4096:
		pool4k.Put(b[:0])
	default:
		pool8k.Put(b[:0])
	}
}

// getJSONBufCap obtains a reusable []byte from the appropriate JSON pool
func getJSONBufCap(n int) []byte {
	switch {
	case n <= 512:
		return jsonPool512.Get().([]byte)
	case n <= 2048:
		return jsonPool2K.Get().([]byte)
	case n <= 4096:
		return jsonPool4K.Get().([]byte)
	case n <= 8192:
		return jsonPool8K.Get().([]byte)
	default:
		return make([]byte, 0, n)
	}
}

// putJSONBuf returns a []byte to the appropriate JSON pool based on its capacity
func putJSONBuf(b []byte) {
	c := cap(b)
	if c == 0 || c > 8192 {
		return
	}
	b = b[:0]

	switch {
	case c <= 512:
		jsonPool512.Put(b)
	case c <= 2048:
		jsonPool2K.Put(b)
	case c <= 4096:
		jsonPool4K.Put(b)
	default:
		jsonPool8K.Put(b)
	}
}

///////////////////////////////////////
//          LOG METHODS              //
///////////////////////////////////////

func (_log *Log) StructuredJSON(state bool) {
	_log.structured = state
}

func (_log *Log) Status() bool {
	return atomic.LoadUint32(&_log.state) == stateOpen
}

type Stats struct {
	EnqueuedTotal       uint64
	WrittenTotal        uint64
	DroppedTotal        uint64
	QueueLen            int
	QueueCap            int
	WriteErrorsTotal    uint64
	RotationErrorsTotal uint64
}

func (_log *Log) Stats() Stats {
	return Stats{
		EnqueuedTotal:       atomic.LoadUint64(&_log.enqueuedSeq),
		WrittenTotal:        atomic.LoadUint64(&_log.dequeueSeq),
		DroppedTotal:        atomic.LoadUint64(&_log.droppedSeq),
		QueueLen:            len(_log.events),
		QueueCap:            cap(_log.events),
		WriteErrorsTotal:    atomic.LoadUint64(&_log.writeErrorsSeq),
		RotationErrorsTotal: atomic.LoadUint64(&_log.rotationErrorsSeq),
	}
}

func (_log *Log) Dropped() uint64 {
	return atomic.LoadUint64(&_log.droppedSeq)
}

func (_log *Log) log(level string, data interface{}, args ...interface{}) {
	if !_log.shouldLog(level) {
		return
	}

	// Structured JSON: prebuild and enqueue as preformatted (kind=2)
	if _log.structured {
		var fields map[string]interface{}
		if len(args) == 0 {
			if f, ok := data.(map[string]interface{}); ok {
				fields = f
			}
		}
		if fields == nil {
			msgStr := _log.formatMessageString(data, args...)
			fields = map[string]interface{}{"msg": msgStr}
		}
		raw := _log.formatStructuredLog(level, fields)
		_log.enqueueEvent(logEvent{level: level, msgBytes: raw, kind: 2})
		return
	}

	// Simple logs: send raw payload and let the writer format
	if len(args) == 0 {
		switch v := data.(type) {
		case string:
			_log.enqueueEvent(logEvent{level: level, msgStr: v, kind: 0})
			return
		case []byte:
			_log.enqueueEvent(logEvent{level: level, msgBytes: v, kind: 1})
			return
		}
	}

	// Formatted path or other types
	msgStr := _log.formatMessageString(data, args...)
	_log.enqueueEvent(logEvent{level: level, msgStr: msgStr, kind: 0})
}

func (_log *Log) logfBytes(level string, msgBytes []byte) {
	if !_log.shouldLog(level) {
		return
	}
	_log.enqueueEvent(logEvent{level: level, msgBytes: msgBytes, kind: 1})
}

func (_log *Log) shouldLog(level string) bool {
	switch _log.level {
	case Level.DEBUG:
		return true
	case Level.INFO:
		return level == Level.INFO || level == Level.WARN || level == Level.ERROR || level == Level.CRITICAL
	case Level.WARN:
		return level == Level.WARN || level == Level.ERROR || level == Level.CRITICAL
	case Level.ERROR:
		return level == Level.ERROR || level == Level.CRITICAL
	case Level.CRITICAL:
		return level == Level.CRITICAL
	}
	return false
}

func (_log *Log) enqueueEvent(ev logEvent) {
	if atomic.LoadUint32(&_log.state) != stateOpen {
		atomic.AddUint64(&_log.droppedSeq, 1)
		if ev.kind == 2 {
			putJSONBuf(ev.msgBytes)
		}
		return
	}

	ev.seq = atomic.AddUint64(&_log.enqueueSeq, 1)

	switch _log.backpressurePolicy {
	case BlockOnFull:
		select {
		case _log.events <- ev:
			atomic.AddUint64(&_log.enqueuedSeq, 1)
		case <-_log.done:
			atomic.AddUint64(&_log.droppedSeq, 1)
			if ev.kind == 2 {
				putJSONBuf(ev.msgBytes)
			}
		}

	case DropNewest:
		select {
		case _log.events <- ev:
			atomic.AddUint64(&_log.enqueuedSeq, 1)
		case <-_log.done:
			atomic.AddUint64(&_log.droppedSeq, 1)
			if ev.kind == 2 {
				putJSONBuf(ev.msgBytes)
			}
		default:
			atomic.AddUint64(&_log.droppedSeq, 1)
			if ev.kind == 2 {
				putJSONBuf(ev.msgBytes)
			}
		}

	case DropOldest:
		select {
		case _log.events <- ev:
			atomic.AddUint64(&_log.enqueuedSeq, 1)
		case <-_log.done:
			atomic.AddUint64(&_log.droppedSeq, 1)
			if ev.kind == 2 {
				putJSONBuf(ev.msgBytes)
			}
		default:
			// Queue is full, evict oldest
			select {
			case oldEv := <-_log.events:
				atomic.AddUint64(&_log.droppedSeq, 1)
				if oldEv.kind == 2 {
					putJSONBuf(oldEv.msgBytes)
				}
			case <-_log.done:
				atomic.AddUint64(&_log.droppedSeq, 1)
				if ev.kind == 2 {
					putJSONBuf(ev.msgBytes)
				}
				return
			default:
			}
			// Try enqueuing again
			select {
			case _log.events <- ev:
				atomic.AddUint64(&_log.enqueuedSeq, 1)
			case <-_log.done:
				atomic.AddUint64(&_log.droppedSeq, 1)
				if ev.kind == 2 {
					putJSONBuf(ev.msgBytes)
				}
			default:
				atomic.AddUint64(&_log.droppedSeq, 1)
				if ev.kind == 2 {
					putJSONBuf(ev.msgBytes)
				}
			}
		}

	case DropDebugOnly:
		if ev.level == Level.DEBUG || ev.level == Level.INFO {
			select {
			case _log.events <- ev:
				atomic.AddUint64(&_log.enqueuedSeq, 1)
			case <-_log.done:
				atomic.AddUint64(&_log.droppedSeq, 1)
				if ev.kind == 2 {
					putJSONBuf(ev.msgBytes)
				}
			default:
				atomic.AddUint64(&_log.droppedSeq, 1)
				if ev.kind == 2 {
					putJSONBuf(ev.msgBytes)
				}
			}
		} else {
			select {
			case _log.events <- ev:
				atomic.AddUint64(&_log.enqueuedSeq, 1)
			case <-_log.done:
				atomic.AddUint64(&_log.droppedSeq, 1)
				if ev.kind == 2 {
					putJSONBuf(ev.msgBytes)
				}
			}
		}
	}
}

func (_log *Log) Info(data interface{}, args ...interface{}) {
	_log.log(Level.INFO, data, args...)
}

func (_log *Log) Warn(data interface{}, args ...interface{}) {
	_log.log(Level.WARN, data, args...)
}

func (_log *Log) Error(data interface{}, args ...interface{}) {
	_log.log(Level.ERROR, data, args...)
}

func (_log *Log) Critical(data interface{}, args ...interface{}) {
	_log.log(Level.CRITICAL, data, args...)
}

func (_log *Log) Debug(data interface{}, args ...interface{}) {
	_log.log(Level.DEBUG, data, args...)
}

func (_log *Log) InfoBytes(msg []byte) {
	_log.logfBytes(Level.INFO, msg)
}

func (_log *Log) WarnBytes(msg []byte) {
	_log.logfBytes(Level.WARN, msg)
}

func (_log *Log) ErrorBytes(msg []byte) {
	_log.logfBytes(Level.ERROR, msg)
}

func (_log *Log) CriticalBytes(msg []byte) {
	_log.logfBytes(Level.CRITICAL, msg)
}

func (_log *Log) DebugBytes(msg []byte) {
	_log.logfBytes(Level.DEBUG, msg)
}

func (_log *Log) Write(p []byte) (int, error) {
	if !_log.shouldLog(Level.INFO) {
		return len(p), nil
	}
	_log.enqueueEvent(logEvent{level: Level.INFO, msgBytes: p, kind: 1})
	return len(p), nil
}

func (_log *Log) Rotation(sizeMB int, backup int) {
	if !_log.Status() {
		return
	}
	if backup < 1 {
		backup = 1
	}
	ack := make(chan struct{})
	req := controlReq{
		setRotation: &rotationConfig{sizeMB: sizeMB, backup: backup},
		ack:         ack,
	}
	select {
	case _log.control <- req:
		select {
		case <-ack:
		case <-_log.done:
		}
	case <-_log.done:
	}
}

func (_log *Log) DailyRotation(enabled bool) {
	if !_log.Status() {
		return
	}
	ack := make(chan struct{})
	req := controlReq{
		setDaily: &enabled,
		ack:      ack,
	}
	select {
	case _log.control <- req:
		select {
		case <-ack:
		case <-_log.done:
		}
	case <-_log.done:
	}
}

func (_log *Log) rotateByDate(day string) error {
	f := _log.getFile()
	if f == nil {
		return nil
	}

	base := f.Name()
	dir, name := filepath.Dir(base), filepath.Base(base)
	ext := filepath.Ext(name)
	baseNoExt := strings.TrimSuffix(name, ext)

	datedName := fmt.Sprintf("%s-%s%s", baseNoExt, day, ext)
	datedBase := filepath.Join(dir, datedName)

	oldFile := f

	maxRot := _log.maxRotation
	if maxRot < 1 {
		maxRot = 1
	}

	// Rotar backups con fecha: log.N → log.(N+1)
	for i := maxRot - 1; i >= 0; i-- {
		src := fmt.Sprintf("%s.%d", datedBase, i)
		dst := fmt.Sprintf("%s.%d", datedBase, i+1)

		if IsSymlink(src) || IsSymlink(dst) {
			atomic.AddUint64(&_log.rotationErrorsSeq, 1)
			continue
		}

		if _, err := os.Stat(src); err == nil {
			if err := os.Rename(src, dst); err != nil {
				atomic.AddUint64(&_log.rotationErrorsSeq, 1)
			}
		}
	}

	// Renombrar log_base.log → log_base-YYYY-MM-DD.log
	if IsSymlink(base) || IsSymlink(datedBase) {
		atomic.AddUint64(&_log.rotationErrorsSeq, 1)
		return ErrUnsafeSymlink
	}

	if err := os.Rename(base, datedBase); err != nil {
		reportInternalError("daily rotation rename: %v", err)
		atomic.AddUint64(&_log.rotationErrorsSeq, 1)
	}

	// Crear nuevo archivo base.log vacío
	newFile, err := os.OpenFile(base, os.O_CREATE|os.O_APPEND|os.O_WRONLY, _log.fileMode)
	if err != nil {
		reportInternalError("create base after daily rotation: %v", err)
		atomic.AddUint64(&_log.rotationErrorsSeq, 1)
		return err
	}

	_log.setFile(newFile)
	_log.currentSize = 0

	if oldFile != nil && oldFile.Fd() != newFile.Fd() {
		_ = oldFile.Close()
	}
	return nil
}

func (_log *Log) logRotate() error {
	f := _log.getFile()
	if f == nil {
		return nil
	}

	base := f.Name()
	oldFile := f

	maxRot := _log.maxRotation
	if maxRot < 1 {
		maxRot = 1
	}

	target := base

	if _log.daily {
		today := time.Now().Format(lastDayFormat)
		dir, name := filepath.Dir(base), filepath.Base(base)
		ext := filepath.Ext(name)
		baseNoExt := strings.TrimSuffix(name, ext)
		dated := filepath.Join(dir, fmt.Sprintf("%s-%s%s", baseNoExt, today, ext))
		target = dated
	}

	// target.N → target.(N+1)
	for i := maxRot - 1; i >= 0; i-- {
		src := fmt.Sprintf("%s.%d", target, i)
		dst := fmt.Sprintf("%s.%d", target, i+1)

		if IsSymlink(src) || IsSymlink(dst) {
			atomic.AddUint64(&_log.rotationErrorsSeq, 1)
			continue
		}

		if _, err := os.Stat(src); err == nil {
			if err := os.Rename(src, dst); err != nil {
				atomic.AddUint64(&_log.rotationErrorsSeq, 1)
			}
		}
	}

	firstBackup := target + ".0"
	if IsSymlink(base) || IsSymlink(firstBackup) {
		atomic.AddUint64(&_log.rotationErrorsSeq, 1)
		return ErrUnsafeSymlink
	}

	if err := os.Rename(base, firstBackup); err != nil {
		reportInternalError("size rotation rename: %v", err)
		atomic.AddUint64(&_log.rotationErrorsSeq, 1)
	}

	newFile, err := os.OpenFile(base, os.O_CREATE|os.O_APPEND|os.O_WRONLY, _log.fileMode)
	if err != nil {
		reportInternalError("create file after size rotation: %v", err)
		atomic.AddUint64(&_log.rotationErrorsSeq, 1)
		return err
	}

	_log.setFile(newFile)
	_log.currentSize = 0

	if oldFile != nil && oldFile.Fd() != newFile.Fd() {
		_ = oldFile.Close()
	}
	return nil
}

func (_log *Log) Close() {
	_log.closeOnce.Do(func() {
		// 1. Transition state to stateClosing
		atomic.StoreUint32(&_log.state, stateClosing)

		// 2. Close done channel to release any blocked producers
		if _log.done != nil {
			close(_log.done)
		}

		// 3. Stop time ticker
		if _log.timeTicker != nil {
			_log.timeTicker.Stop()
		}

		// 4. Send close control event to wait for writer to drain and flush
		ack := make(chan struct{})
		req := controlReq{ack: ack, isClose: true}
		select {
		case _log.control <- req:
			select {
			case <-ack:
			case <-time.After(5 * time.Second): // safety fallback
			}
		case <-time.After(1 * time.Second): // safety fallback if control channel is blocked/full
		}

		// 5. Wait for all background goroutines (writer, timestamp updater)
		_log.wg.Wait()

		// 6. Transition state to stateClosed
		atomic.StoreUint32(&_log.state, stateClosed)
	})
}

///////////////////////////////////////
//  P U B L I C   F U N C T I O N S  //
///////////////////////////////////////

func Start(logName, logPath, logLevel string, opts ...Option) (*Log, error) {
	if err := ValidateLogFilename(logName); err != nil {
		return nil, err
	}
	cleanedDir, err := ValidateLogDirectory(logPath)
	if err != nil {
		return nil, err
	}

	cfg := &config{
		bufferSize:         DefaultBufferSize,
		batchSize:          DefaultBatchSize,
		flushEvery:         flushInterval,
		bufferCap:          256 << 10, // default 256KB internal buffers for better large-log performance
		drainBurst:         512,       // default larger burst for better parallel throughput
		backpressurePolicy: BlockOnFull,
		fileMode:           0600, // secure default
		dirMode:            0700, // secure default
	}
	for _, opt := range opts {
		opt(cfg)
	}

	logLevel = strings.ToUpper(logLevel)
	if !verifyLevel(logLevel) {
		reportInternalError("warning: invalid log level '%s', falling back to INFO", logLevel)
		logLevel = Level.INFO
	}

	fullPath := filepath.Join(cleanedDir, logName)
	if IsSymlink(fullPath) {
		return nil, ErrUnsafeSymlink
	}

	f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, cfg.fileMode)
	if err != nil {
		return nil, err
	}

	// Determine internal buffer capacity: prefer bufferCap, fall back to (deprecated) batchSize.
	internalCap := cfg.bufferCap
	if internalCap <= 0 {
		internalCap = cfg.batchSize
	}
	if internalCap < defaultBufferInitialCap {
		internalCap = defaultBufferInitialCap
	}

	log := &Log{
		name:               logName,
		path:               cleanedDir + string(os.PathSeparator),
		level:              logLevel,
		maxSize:            0,
		maxRotation:        0,
		daily:              false,
		lastDay:            time.Now().Format(lastDayFormat),
		status:             true,
		events:             make(chan logEvent, cfg.bufferSize),
		buffer:             make([]byte, 0, internalCap),
		writeBuf:           make([]byte, 0, internalCap),
		flushEvery:         cfg.flushEvery,
		done:               make(chan struct{}),
		control:            make(chan controlReq, 8),
		drainBurstLimit:    cfg.drainBurst,
		dropOnFull:         cfg.dropOnFull,
		backpressurePolicy: cfg.backpressurePolicy,
		fileMode:           cfg.fileMode,
		dirMode:            cfg.dirMode,
		state:              stateOpen,
	}

	log.file.Store(f)

	if info, err := f.Stat(); err == nil {
		log.currentSize = info.Size()
	}
	log.updateTimestampCache()
	log.timeTicker = time.NewTicker(cacheInterval)
	log.wg.Add(1)
	go log.startTimestampCacheUpdater()

	log.wg.Add(1)
	go log.startWriting()

	return log, nil
}

///////////////////////////////////////
// P R I V A T E   F U N C T I O N S //
///////////////////////////////////////

func reportInternalError(format string, args ...interface{}) {
	_, err := fmt.Fprintf(os.Stderr, "Acacia Internal: "+format+"\n", args...)

	if err != nil {
		return
	}
}

func (_log *Log) startTimestampCacheUpdater() {
	defer _log.wg.Done()
	ticker := _log.timeTicker
	for {
		select {
		case <-ticker.C:
			_log.updateTimestampCache()
		case <-_log.done:
			return
		}
	}
}

func (_log *Log) updateTimestampCache() {
	buf := getBufCap(64)
	defer putBufCap(buf)
	now := time.Now()
	buf = now.AppendFormat(buf, timestampFormat)
	cachedCopy := make([]byte, len(buf))
	copy(cachedCopy, buf)
	_log.cachedTime.Store(cachedCopy)
}

func levelBytesFor(lvl string) []byte {
	switch lvl {
	case Level.DEBUG:
		return levelDebug
	case Level.INFO:
		return levelInfo
	case Level.WARN:
		return levelWarn
	case Level.ERROR:
		return levelError
	case Level.CRITICAL:
		return levelCritical
	default:
		return levelInfo
	}
}

func appendLine(dst []byte, ts []byte, lvl []byte, msg string) []byte {
	if len(ts) > 0 {
		dst = append(dst, ts...)
	}
	dst = append(dst, ' ')
	dst = append(dst, '[')
	dst = append(dst, lvl...)
	dst = append(dst, ']', ' ')
	dst = append(dst, msg...)
	if len(dst) == 0 || dst[len(dst)-1] != '\n' {
		dst = append(dst, '\n')
	}
	return dst
}

func appendLineBytes(dst []byte, ts []byte, lvl []byte, msg []byte) []byte {
	if len(ts) > 0 {
		dst = append(dst, ts...)
	}
	dst = append(dst, ' ')
	dst = append(dst, '[')
	dst = append(dst, lvl...)
	dst = append(dst, ']', ' ')
	dst = append(dst, msg...)
	if len(dst) == 0 || dst[len(dst)-1] != '\n' {
		dst = append(dst, '\n')
	}
	return dst
}

func (_log *Log) startWriting() {
	defer _log.wg.Done()

	interval := _log.flushEvery
	if interval <= 0 {
		interval = flushInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {

		case ev, ok := <-_log.events:
			if !ok {
				_log.flush()
				// Drain any pending control requests to avoid deadlocking concurrent callers
				for {
					select {
					case req := <-_log.control:
						if req.ack != nil {
							close(req.ack)
						}
					default:
						return
					}
				}
			}
			_log.writeEvent(ev)
			_log.drainBurst()

		case <-ticker.C:
			_log.flush()

		case req := <-_log.control:
			if req.isClose {
				// Drain remaining events
				for {
					select {
					case ev := <-_log.events:
						_log.writeEvent(ev)
						_log.drainBurst()
					default:
						goto drained
					}
				}
			drained:
				_log.flush()

				if f := _log.getFile(); f != nil {
					if err := f.Sync(); err != nil {
						reportInternalError("final file sync error: %v", err)
					}
					if err := f.Close(); err != nil {
						reportInternalError("final file close error: %v", err)
					}
				}

				if req.ack != nil {
					close(req.ack)
				}

				// Drain any other remaining control requests to avoid deadlocks
				for {
					select {
					case req2 := <-_log.control:
						if req2.ack != nil {
							close(req2.ack)
						}
					default:
						return
					}
				}
			}

			if req.setRotation != nil {
				backup := req.setRotation.backup
				if backup < 1 {
					backup = 1
				}

				_log.maxRotation = backup

				if req.setRotation.sizeMB <= 0 {
					_log.maxSize = 0
				} else {
					_log.maxSize = int64(req.setRotation.sizeMB) * 1024 * 1024
				}
				if req.ack != nil {
					close(req.ack)
				}
				continue
			}

			// Daily rotation
			if req.setDaily != nil {
				enabled := *req.setDaily
				_log.daily = enabled
				if enabled {
					// Establecer el día base al actual para evitar rotación inmediata al habilitar
					_log.lastDay = time.Now().Format(lastDayFormat)
				}

				if req.ack != nil {
					close(req.ack)
				}
				continue
			}

			_log.currentReq = req
			_log.doSyncInternal()
		}
	}
}

func (_log *Log) writeEvent(ev logEvent) {
	// Single place where the final line is built and appended to the buffer
	var ts []byte
	if cached := _log.cachedTime.Load(); cached != nil {
		ts = cached.([]byte)
	}
	lvl := levelBytesFor(ev.level)

	// Estimate the size of the line to append to avoid unbounded buffer growth
	lineLen := 0
	switch ev.kind {
	case 0: // string payload
		lineLen = len(lvl) + len(ev.msgStr) + 4
		if len(ts) > 0 {
			lineLen += len(ts)
		}
		if len(ev.msgStr) == 0 || ev.msgStr[len(ev.msgStr)-1] != '\n' {
			lineLen++
		}
	case 1: // raw []byte payload
		lineLen = len(lvl) + len(ev.msgBytes) + 4
		if len(ts) > 0 {
			lineLen += len(ts)
		}
		if len(ev.msgBytes) == 0 || ev.msgBytes[len(ev.msgBytes)-1] != '\n' {
			lineLen++
		}
	case 2: // preformatted JSON bytes
		lineLen = len(ev.msgBytes)
	}

	largeLine := lineLen > defaultBufferFlushThreshold

	// Flush proactively if the active buffer size exceeds our threshold
	if len(_log.buffer) > 0 && len(_log.buffer)+lineLen > defaultBufferFlushThreshold {
		_log.flush()
	}

	switch ev.kind {
	case 0: // string payload, format here
		ms := ev.msgStr
		// Trim trailing LF/CR only for emptiness detection
		if len(ms) > 0 && ms[len(ms)-1] == '\n' {
			ms = ms[:len(ms)-1]
			if len(ms) > 0 && ms[len(ms)-1] == '\r' {
				ms = ms[:len(ms)-1]
			}
		}
		if len(ms) > 0 {
			_log.buffer = appendLine(_log.buffer, ts, lvl, ev.msgStr)
		}
	case 1: // raw []byte payload, format here
		mb := ev.msgBytes
		if len(mb) > 0 && mb[len(mb)-1] == '\n' {
			mb = mb[:len(mb)-1]
			if len(mb) > 0 && mb[len(mb)-1] == '\r' {
				mb = mb[:len(mb)-1]
			}
		}
		if len(mb) > 0 {
			_log.buffer = appendLineBytes(_log.buffer, ts, lvl, ev.msgBytes)
		}
	case 2: // preformatted JSON bytes; just append and return buffer to JSON pool
		_log.buffer = append(_log.buffer, ev.msgBytes...)
		putJSONBuf(ev.msgBytes)
	}

	if largeLine && len(_log.buffer) > 0 {
		_log.flush()
	}

	atomic.AddUint64(&_log.dequeueSeq, 1)
}

func (_log *Log) drainBurst() {
	limit := _log.drainBurstLimit
	if limit <= 0 {
		limit = 256
	}
	for i := 0; i < limit; i++ {
		select {
		case ev, ok := <-_log.events:
			if !ok {
				return
			}
			_log.writeEvent(ev)
		default:
			return
		}
	}
}

// doSyncInternal must run only in the writer goroutine.
func (_log *Log) doSyncInternal() {
	req := _log.currentReq

	// Drain pending events
	for {
		select {
		case ev, ok := <-_log.events:
			if !ok {
				goto drainedDone
			}
			_log.writeEvent(ev)
		default:
			goto drainedDone
		}
	}

drainedDone:
	_log.flush()

	if req.ack != nil {
		if req.target == 0 || atomic.LoadUint64(&_log.dequeueSeq) >= req.target {
			close(req.ack)
		}
	}
}

// Public Sync: ordering barrier; does NOT drain from producers.
// Sends a control message to the writer with the target seq and waits for ack.
func (_log *Log) Sync() {
	if atomic.LoadUint32(&_log.state) != stateOpen {
		return
	}
	// Snapshot current enqueued sequence as the target barrier
	target := atomic.LoadUint64(&_log.enqueuedSeq)
	ack := make(chan struct{})
	req := controlReq{target: target, ack: ack}
	select {
	case _log.control <- req:
		select {
		case <-ack:
		case <-_log.done:
		}
	case <-_log.done:
	}
}

func (_log *Log) flush() {
	if len(_log.buffer) == 0 {
		return
	}

	// Double buffer (lock-free swap on the hot path)
	_log.writeBuf, _log.buffer = _log.buffer, _log.writeBuf[:0]
	if cap(_log.buffer) > defaultBufferMaxRetainedCap {
		_log.buffer = make([]byte, 0, defaultBufferInitialCap)
	}
	remaining := _log.writeBuf

	// Protect rotation and file I/O under a single mutex
	_log.mtx.Lock()
	defer _log.mtx.Unlock()

	// Rotation and file handling must be protected
	today := time.Now().Format(lastDayFormat)
	needDaily := false
	rotateDay := _log.lastDay

	if _log.daily && (today != _log.lastDay) {
		needDaily = true
	}

	if needDaily {
		if f := _log.getFile(); f != nil {
			// Write remaining data before rotation
			if written, err := f.Write(remaining); written > 0 {
				_log.currentSize += int64(written)
			} else if err != nil {
				atomic.AddUint64(&_log.writeErrorsSeq, 1)
			}
		}

		// Rotate using the previous day (lastDay) to name the rotated file
		_ = _log.rotateByDate(rotateDay)

		_log.lastDay = today
		_log.writeBuf = _log.writeBuf[:0]
		return
	}

	// Size-based rotation
	for len(remaining) > 0 {
		f := _log.getFile()
		if f == nil {
			break
		}

		if _log.maxSize <= 0 {
			if written, err := f.Write(remaining); written > 0 {
				_log.currentSize += int64(written)
			} else if err != nil {
				atomic.AddUint64(&_log.writeErrorsSeq, 1)
			}
			remaining = nil
			break
		}

		lineEnd := bytes.IndexByte(remaining, '\n')
		var line []byte
		if lineEnd < 0 {
			line = remaining
		} else {
			line = remaining[:lineEnd+1]
		}

		cur := _log.currentSize
		maxSize := _log.maxSize

		// Si está lleno, rotar
		if cur >= maxSize {
			_ = _log.logRotate()
			continue
		}

		allowed := maxSize - cur

		if int64(len(line)) > allowed {
			if cur == 0 {
				// escribir línea demasiado grande
				if written, err := f.Write(line); written > 0 {
					_log.currentSize += int64(written)
				} else if err != nil {
					atomic.AddUint64(&_log.writeErrorsSeq, 1)
				}
				remaining = remaining[len(line):]
				_log.logRotate()
				continue
			}
			// rotar primero
			_log.logRotate()
			continue
		}

		if written, err := f.Write(line); written > 0 {
			_log.currentSize += int64(written)
		} else if err != nil {
			atomic.AddUint64(&_log.writeErrorsSeq, 1)
		}
		remaining = remaining[len(line):]
	}

	// Limpiar writeBuf antes de salir del lock
	_log.writeBuf = _log.writeBuf[:0]
	if cap(_log.writeBuf) > defaultBufferMaxRetainedCap {
		_log.writeBuf = make([]byte, 0, defaultBufferInitialCap)
	}
}

func (_log *Log) formatMessageString(data interface{}, args ...interface{}) string {
	if len(args) == 0 {
		switch v := data.(type) {
		case string:
			return v
		case []byte:
			return string(v)
		case int, int8, int16, int32, int64, float32, float64, bool:
			return fmt.Sprint(v)
		default:
			return fmt.Sprint(v)
		}
	}

	if fmtStr, ok := data.(string); ok {
		return fmt.Sprintf(fmtStr, args...)
	}

	return fmt.Sprintf(data.(string), args...)
}

func (_log *Log) formatStructuredLog(level string, fields map[string]interface{}) []byte {
	var tsBytes []byte

	if cachedTS := _log.cachedTime.Load(); cachedTS != nil {
		tsBytes = cachedTS.([]byte)
	}

	levelBytes := levelBytesFor(level)
	need := estimateJSONSize(fields, tsBytes, levelBytes)
	buf := getJSONBufCap(need)
	buf = buf[:0]

	buf = append(buf, '{')

	// Timestamp
	if len(tsBytes) > 0 {
		buf = append(buf, `"ts":"`...)
		buf = append(buf, tsBytes...)
		buf = append(buf, '"')
	}

	// Level
	buf = append(buf, `,"level":"`...)
	buf = append(buf, levelBytes...)
	buf = append(buf, '"')

	// Campos
	for k, v := range fields {
		buf = append(buf, `,"`...)
		buf = append(buf, k...)
		buf = append(buf, `":`...)

		switch x := v.(type) {
		case string:
			buf = append(buf, '"')
			buf = escapeJSONInto(buf, x)
			buf = append(buf, '"')
		case int:
			buf = strconv.AppendInt(buf, int64(x), 10)
		case int8:
			buf = strconv.AppendInt(buf, int64(x), 10)
		case int16:
			buf = strconv.AppendInt(buf, int64(x), 10)
		case int32:
			buf = strconv.AppendInt(buf, int64(x), 10)
		case int64:
			buf = strconv.AppendInt(buf, x, 10)
		case uint:
			buf = strconv.AppendUint(buf, uint64(x), 10)
		case uint8:
			buf = strconv.AppendUint(buf, uint64(x), 10)
		case uint16:
			buf = strconv.AppendUint(buf, uint64(x), 10)
		case uint32:
			buf = strconv.AppendUint(buf, uint64(x), 10)
		case uint64:
			buf = strconv.AppendUint(buf, x, 10)
		case float32:
			buf = strconv.AppendFloat(buf, float64(x), 'f', -1, 32)
		case float64:
			buf = strconv.AppendFloat(buf, x, 'f', -1, 64)

		case bool:
			if x {
				buf = append(buf, "true"...)
			} else {
				buf = append(buf, "false"...)
			}

		default:
			buf = append(buf, '"')
			buf = escapeJSONInto(buf, fmt.Sprint(x))
			buf = append(buf, '"')
		}
	}

	buf = append(buf, '}', '\n')
	return buf
}

func ensureJSONCapacity(buf []byte, extra int) []byte {
	needed := len(buf) + extra
	if cap(buf) >= needed {
		return buf
	}
	newCap := cap(buf) * 2
	if newCap < needed {
		newCap = needed
	}
	newBuf := getJSONBufCap(newCap)
	newBuf = newBuf[:len(buf)]
	copy(newBuf, buf)
	putJSONBuf(buf)
	return newBuf
}

func escapeJSONInto(dst []byte, s string) []byte {
	const hex = "0123456789abcdef"
	dst = ensureJSONCapacity(dst, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '\\', '"':
			dst = ensureJSONCapacity(dst, 2)
			dst = append(dst, '\\', c)
		case '\n':
			dst = ensureJSONCapacity(dst, 2)
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = ensureJSONCapacity(dst, 2)
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = ensureJSONCapacity(dst, 2)
			dst = append(dst, '\\', 't')
		case '\b':
			dst = ensureJSONCapacity(dst, 2)
			dst = append(dst, '\\', 'b')
		case '\f':
			dst = ensureJSONCapacity(dst, 2)
			dst = append(dst, '\\', 'f')
		default:
			if c < 32 {
				dst = ensureJSONCapacity(dst, 6)
				dst = append(dst, '\\', 'u', '0', '0', hex[c>>4], hex[c&0x0f])
			} else {
				dst = append(dst, c)
			}
		}
	}
	return dst
}

func (_log *Log) setFormatBytesFromString(msg string, level string) []byte {
	// Timestamp cache
	var tsBytes []byte
	if cachedTS := _log.cachedTime.Load(); cachedTS != nil {
		tsBytes = cachedTS.([]byte)
	}
	levelBytes := levelBytesFor(level)

	need := len(tsBytes) + 1 + 1 + len(levelBytes) + 2 + len(msg) + 1
	if need < 64 {
		need = 64
	}

	buf := getBufCap(need)
	// buf = buf[:0]

	if len(tsBytes) > 0 {
		buf = append(buf, tsBytes...)
	}
	buf = append(buf, ' ')
	buf = append(buf, '[')
	buf = append(buf, levelBytes...)
	buf = append(buf, ']', ' ')
	buf = append(buf, msg...)
	if len(buf) == 0 || buf[len(buf)-1] != '\n' {
		buf = append(buf, '\n')
	}

	return buf
}

func estimateJSONSize(fields map[string]interface{}, tsBytes []byte, level []byte) int {
	size := len(tsBytes) + len(level) + 32

	for k, v := range fields {
		size += len(k) + 4

		switch x := v.(type) {
		case string:
			size += len(x) + 2
		case int, int8, int16, int32, int64:
			size += 20
		case uint, uint8, uint16, uint32, uint64:
			size += 20
		case float32, float64:
			size += 24
		case bool:
			size += 5
		default:
			size += len(fmt.Sprint(x)) + 2
		}
	}

	return size + 4
}

func (_log *Log) TimestampFormat(format string) {
	timestampFormat = format
	_log.updateTimestampCache()
}

func ValidateLogFilename(name string) error {
	if name == "" {
		return ErrInvalidLogFilename
	}
	if name == "." || name == ".." {
		return ErrInvalidLogFilename
	}
	if len(name) > 255 {
		return ErrInvalidLogFilename
	}
	if strings.Contains(name, "\x00") {
		return ErrInvalidLogFilename
	}
	if strings.ContainsAny(name, `/\`) {
		return ErrInvalidLogFilename
	}
	if strings.Contains(name, "..") {
		return ErrInvalidLogFilename
	}
	if filepath.Clean(name) != name {
		return ErrInvalidLogFilename
	}
	if filepath.Base(name) != name {
		return ErrInvalidLogFilename
	}
	return nil
}

func ValidateLogDirectory(dir string) (string, error) {
	if dir == "" {
		dir = "./"
	}
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return "", ErrInvalidLogDirectory
	}
	info, err := os.Lstat(absDir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", err
		}
		return "", ErrInvalidLogDirectory
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return "", ErrUnsafeSymlink
	}
	if !info.IsDir() {
		return "", ErrInvalidLogDirectory
	}
	return absDir, nil
}

func IsSymlink(path string) bool {
	info, err := os.Lstat(path)
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeSymlink != 0
}

func verifyLevel(lvl string) bool {
	switch lvl {
	case Level.DEBUG, Level.INFO, Level.WARN, Level.ERROR, Level.CRITICAL:
		return true
	default:
		return false
	}
}

func (_log *Log) getFile() *os.File {
	if v := _log.file.Load(); v != nil {
		return v.(*os.File)
	}
	return nil
}

func (_log *Log) setFile(f *os.File) {
	if f != nil {
		_log.file.Store(f)
	} else {
		_log.file.Store((*os.File)(nil))
	}
}

// --- Event Builder API (Fase 02, Fase 03 & Fase 04) ---

type Event struct {
	logger *Log
	level  string
	buf    []byte
	done   bool
}

var eventPool = sync.Pool{
	New: func() interface{} {
		return &Event{}
	},
}

func isSensitiveKey(key string) bool {
	var buf [64]byte
	w := 0
	for i := 0; i < len(key); i++ {
		c := key[i]
		if c == '-' || c == '_' || c == '.' {
			continue
		}
		if w >= 64 {
			return false
		}
		if c >= 'A' && c <= 'Z' {
			buf[w] = c + ('a' - 'A')
		} else {
			buf[w] = c
		}
		w++
	}
	normalized := buf[:w]

	switch string(normalized) {
	case "authorization", "cookie", "setcookie", "token", "accesstoken", "refreshtoken",
		"password", "passwd", "pwd", "apikey", "privatekey", "clientsecret", "secret",
		"bearer", "jwt", "session", "sessionid", "csrf", "xsrf", "xapikey":
		return true
	default:
		return false
	}
}

func (e *Event) appendRedactedField(key string) *Event {
	e.buf = ensureJSONCapacity(e.buf, len(key)+16)
	e.buf = append(e.buf, `,"`...)
	e.buf = append(e.buf, key...)
	e.buf = append(e.buf, `":"[REDACTED]"`...)
	return e
}

func (l *Log) NewEvent(level string) *Event {
	if !l.shouldLog(level) {
		return nil
	}
	if l.backpressurePolicy == DropNewest || l.backpressurePolicy == DropDebugOnly {
		if len(l.events) >= cap(l.events) {
			if l.backpressurePolicy == DropNewest || (level == Level.DEBUG || level == Level.INFO) {
				atomic.AddUint64(&l.droppedSeq, 1)
				return nil
			}
		}
	}
	e := eventPool.Get().(*Event)
	e.logger = l
	e.level = level
	e.done = false

	e.buf = getJSONBufCap(256)[:0]

	var tsBytes []byte
	if cachedTS := l.cachedTime.Load(); cachedTS != nil {
		tsBytes = cachedTS.([]byte)
	}
	levelBytes := levelBytesFor(level)

	e.buf = ensureJSONCapacity(e.buf, 60)
	e.buf = append(e.buf, '{')
	if len(tsBytes) > 0 {
		e.buf = append(e.buf, `"ts":"`...)
		e.buf = append(e.buf, tsBytes...)
		e.buf = append(e.buf, `",`...)
	}
	e.buf = append(e.buf, `"level":"`...)
	e.buf = append(e.buf, levelBytes...)
	e.buf = append(e.buf, '"')
	return e
}

func (l *Log) Event(level string) *Event {
	return l.NewEvent(level)
}

func (l *Log) InfoEvent() *Event {
	return l.NewEvent(Level.INFO)
}

func (l *Log) WarnEvent() *Event {
	return l.NewEvent(Level.WARN)
}

func (l *Log) ErrorEvent() *Event {
	return l.NewEvent(Level.ERROR)
}

func (l *Log) DebugEvent() *Event {
	return l.NewEvent(Level.DEBUG)
}

func (l *Log) CriticalEvent() *Event {
	return l.NewEvent(Level.CRITICAL)
}

func (e *Event) Str(key, val string) *Event {
	if e == nil {
		return nil
	}
	if isSensitiveKey(key) {
		return e.appendRedactedField(key)
	}
	e.buf = ensureJSONCapacity(e.buf, len(key)+len(val)+6)
	e.buf = append(e.buf, `,"`...)
	e.buf = append(e.buf, key...)
	e.buf = append(e.buf, `":"`...)
	e.buf = escapeJSONInto(e.buf, val)
	e.buf = append(e.buf, '"')
	return e
}

func (e *Event) Int(key string, val int) *Event {
	if e == nil {
		return nil
	}
	if isSensitiveKey(key) {
		return e.appendRedactedField(key)
	}
	e.buf = ensureJSONCapacity(e.buf, len(key)+24)
	e.buf = append(e.buf, `,"`...)
	e.buf = append(e.buf, key...)
	e.buf = append(e.buf, `":`...)
	e.buf = strconv.AppendInt(e.buf, int64(val), 10)
	return e
}

func (e *Event) Int64(key string, val int64) *Event {
	if e == nil {
		return nil
	}
	if isSensitiveKey(key) {
		return e.appendRedactedField(key)
	}
	e.buf = ensureJSONCapacity(e.buf, len(key)+24)
	e.buf = append(e.buf, `,"`...)
	e.buf = append(e.buf, key...)
	e.buf = append(e.buf, `":`...)
	e.buf = strconv.AppendInt(e.buf, val, 10)
	return e
}

func (e *Event) Uint64(key string, val uint64) *Event {
	if e == nil {
		return nil
	}
	if isSensitiveKey(key) {
		return e.appendRedactedField(key)
	}
	e.buf = ensureJSONCapacity(e.buf, len(key)+24)
	e.buf = append(e.buf, `,"`...)
	e.buf = append(e.buf, key...)
	e.buf = append(e.buf, `":`...)
	e.buf = strconv.AppendUint(e.buf, val, 10)
	return e
}

func (e *Event) Bool(key string, val bool) *Event {
	if e == nil {
		return nil
	}
	if isSensitiveKey(key) {
		return e.appendRedactedField(key)
	}
	e.buf = ensureJSONCapacity(e.buf, len(key)+10)
	e.buf = append(e.buf, `,"`...)
	e.buf = append(e.buf, key...)
	e.buf = append(e.buf, `":`...)
	if val {
		e.buf = append(e.buf, "true"...)
	} else {
		e.buf = append(e.buf, "false"...)
	}
	return e
}

func (e *Event) Float64(key string, val float64) *Event {
	if e == nil {
		return nil
	}
	if isSensitiveKey(key) {
		return e.appendRedactedField(key)
	}
	e.buf = ensureJSONCapacity(e.buf, len(key)+28)
	e.buf = append(e.buf, `,"`...)
	e.buf = append(e.buf, key...)
	e.buf = append(e.buf, `":`...)
	e.buf = strconv.AppendFloat(e.buf, val, 'f', -1, 64)
	return e
}

func (e *Event) ErrKey(key string, err error) *Event {
	if e == nil {
		return nil
	}
	if err == nil {
		return e
	}
	if isSensitiveKey(key) {
		return e.appendRedactedField(key)
	}
	errStr := err.Error()
	e.buf = ensureJSONCapacity(e.buf, len(key)+len(errStr)+12)
	e.buf = append(e.buf, `,"`...)
	e.buf = append(e.buf, key...)
	e.buf = append(e.buf, `":"`...)
	e.buf = escapeJSONInto(e.buf, errStr)
	e.buf = append(e.buf, '"')
	return e
}

func (e *Event) Err(err error) *Event {
	return e.ErrKey("error", err)
}

func (e *Event) Dur(key string, val time.Duration) *Event {
	if e == nil {
		return nil
	}
	if isSensitiveKey(key) {
		return e.appendRedactedField(key)
	}
	e.buf = ensureJSONCapacity(e.buf, len(key)+24)
	e.buf = append(e.buf, `,"`...)
	e.buf = append(e.buf, key...)
	e.buf = append(e.buf, `":`...)
	e.buf = strconv.AppendInt(e.buf, int64(val), 10)
	return e
}

func (e *Event) Time(key string, val time.Time) *Event {
	if e == nil {
		return nil
	}
	if isSensitiveKey(key) {
		return e.appendRedactedField(key)
	}
	e.buf = ensureJSONCapacity(e.buf, len(key)+40)
	e.buf = append(e.buf, `,"`...)
	e.buf = append(e.buf, key...)
	e.buf = append(e.buf, `":"`...)
	e.buf = val.AppendFormat(e.buf, time.RFC3339Nano)
	e.buf = append(e.buf, '"')
	return e
}

func (e *Event) Secret(key, val string) *Event {
	if e == nil {
		return nil
	}
	e.buf = ensureJSONCapacity(e.buf, len(key)+16)
	e.buf = append(e.buf, `,"`...)
	e.buf = append(e.buf, key...)
	e.buf = append(e.buf, `":"[REDACTED]"`...)
	return e
}

func (e *Event) StrRedacted(key, val string) *Event {
	if e == nil {
		return nil
	}
	e.buf = ensureJSONCapacity(e.buf, len(key)+16)
	e.buf = append(e.buf, `,"`...)
	e.buf = append(e.buf, key...)
	e.buf = append(e.buf, `":"[REDACTED]"`...)
	return e
}

func (e *Event) Msg(msg string) {
	if e == nil {
		return
	}
	if e.done {
		return
	}
	e.done = true

	e.buf = ensureJSONCapacity(e.buf, len(msg)+11)
	e.buf = append(e.buf, `,"msg":"`...)
	e.buf = escapeJSONInto(e.buf, msg)
	e.buf = append(e.buf, '"', '}', '\n')

	e.sendEvent()
}

func (e *Event) Send() {
	if e == nil {
		return
	}
	if e.done {
		return
	}
	e.done = true

	e.buf = ensureJSONCapacity(e.buf, 2)
	e.buf = append(e.buf, '}', '\n')

	e.sendEvent()
}

func (e *Event) sendEvent() {
	e.logger.enqueueEvent(logEvent{level: e.level, msgBytes: e.buf, kind: 2})
	e.buf = nil
	e.logger = nil
	eventPool.Put(e)
}
