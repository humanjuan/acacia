////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//                                                                                                                    //
//  Author: Juan Alejandro Perez Chandia                                                                              //
//  Contact: juan.alejandro@humanjuan.com                                                                             //
//  Website: https://humanjuan.com/                                                                                   //
//                                                                                                                    //
//  HumanJuan Acacia - High-performance concurrent logger with real file rotation                                     //
//                                                                                                                    //
//  Version: 2.3.0                                                                                                    //
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
	version           = "2.3.0"
	DefaultBufferSize = 500_000
	MinBufferSize     = 1_000
	DefaultBatchSize  = 64 * 1024 // 64 KB (deprecated: use bufferCap)
	flushInterval     = 100 * time.Millisecond
	cacheInterval     = 100 * time.Millisecond
	lastDayFormat     = "2006-01-02"
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

type config struct {
	bufferSize int
	batchSize  int // Deprecated: prefer bufferCap for internal buffering capacity
	flushEvery time.Duration
	bufferCap  int
	drainBurst int
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
	name, path, level string
	structured        bool
	status            bool
	maxSize           int64
	maxRotation       int
	daily             bool
	lastDay           string
	file              atomic.Value
	events            chan logEvent
	wg                sync.WaitGroup
	mtx               sync.Mutex
	buffer            []byte
	writeBuf          []byte
	flushEvery        time.Duration
	cachedTime        atomic.Value
	timeTicker        *time.Ticker
	done              chan struct{}
	closeOnce         sync.Once
	forceDailyRotate  bool
	enqueueSeq        uint64
	dequeueSeq        uint64
	control           chan controlReq
	currentSize       int64
	currentReq        controlReq
	drainBurstLimit   int
}

// controlReq is a control message for the writer goroutine.
// target indicates the last enqueued sequence that must be processed
// (and flushed) before acknowledging the request.
type controlReq struct {
	target      uint64
	ack         chan struct{}
	setRotation *rotationConfig
	setDaily    *bool
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
	return _log.status
}

func (_log *Log) Dropped() uint64 { return 0 }

func (_log *Log) logfString(level string, data interface{}, args ...interface{}) {
	if !_log.shouldLog(level) {
		return
	}
	seq := atomic.AddUint64(&_log.enqueueSeq, 1)

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
		_log.events <- logEvent{level: level, msgBytes: raw, kind: 2, seq: seq}
		return
	}

	// Simple logs: send raw payload and let the writer format
	if len(args) == 0 {
		switch v := data.(type) {
		case string:
			_log.events <- logEvent{level: level, msgStr: v, kind: 0, seq: seq}
			return
		case []byte:
			_log.events <- logEvent{level: level, msgBytes: v, kind: 1, seq: seq}
			return
		}
	}

	// Formatted path or other types
	msgStr := _log.formatMessageString(data, args...)
	_log.events <- logEvent{level: level, msgStr: msgStr, kind: 0, seq: seq}
}

func (_log *Log) logfBytes(level string, msgBytes []byte) {
	if !_log.shouldLog(level) {
		return
	}
	seq := atomic.AddUint64(&_log.enqueueSeq, 1)
	// Delegate final formatting to the writer
	_log.events <- logEvent{level: level, msgBytes: msgBytes, kind: 1, seq: seq}
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

func (_log *Log) Info(data interface{}, args ...interface{}) {
	_log.logfString(Level.INFO, data, args...)
}

func (_log *Log) Warn(data interface{}, args ...interface{}) {
	_log.logfString(Level.WARN, data, args...)
}

func (_log *Log) Error(data interface{}, args ...interface{}) {
	_log.logfString(Level.ERROR, data, args...)
}

func (_log *Log) Critical(data interface{}, args ...interface{}) {
	_log.logfString(Level.CRITICAL, data, args...)
}

func (_log *Log) Debug(data interface{}, args ...interface{}) {
	_log.logfString(Level.DEBUG, data, args...)
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
	seq := atomic.AddUint64(&_log.enqueueSeq, 1)
	_log.events <- logEvent{level: Level.INFO, msgBytes: p, kind: 1, seq: seq}
	return len(p), nil
}

func (_log *Log) Rotation(sizeMB int, backup int) {
	if backup < 1 {
		backup = 1
	}
	ack := make(chan struct{})
	req := controlReq{
		setRotation: &rotationConfig{sizeMB: sizeMB, backup: backup},
		ack:         ack,
	}
	_log.control <- req
	// Wait for ack to ensure the configuration is applied before returning
	<-ack
}

func (_log *Log) DailyRotation(enabled bool) {
	ack := make(chan struct{})
	req := controlReq{
		setDaily: &enabled,
		ack:      ack,
	}
	_log.control <- req
	// Wait for ack to ensure the configuration is applied before returning
	<-ack
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
		if _, err := os.Stat(src); err == nil {
			_ = os.Rename(src, dst)
		}
	}

	// Renombrar log_base.log → log_base-YYYY-MM-DD.log
	if err := os.Rename(base, datedBase); err != nil {
		reportInternalError("daily rotation rename: %v", err)
	}

	// Crear nuevo archivo base.log vacío
	newFile, err := os.OpenFile(base, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		reportInternalError("create base after daily rotation: %v", err)
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
		if _, err := os.Stat(src); err == nil {
			_ = os.Rename(src, dst)
		}
	}

	firstBackup := target + ".0"
	if err := os.Rename(base, firstBackup); err != nil {
		reportInternalError("size rotation rename: %v", err)
	}

	newFile, err := os.OpenFile(base, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		reportInternalError("create file after size rotation: %v", err)
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

		if _log.done != nil {
			close(_log.done)
		}

		if _log.timeTicker != nil {
			_log.timeTicker.Stop()
		}

		if _log.events != nil {
			close(_log.events)
		}

		_log.wg.Wait()

		if f := _log.getFile(); f != nil {
			if err := f.Sync(); err != nil {
				reportInternalError("final file sync error: %v", err)
			}
			if err := f.Close(); err != nil {
				reportInternalError("final file close error: %v", err)
			}
		}
	})
}

///////////////////////////////////////
//  P U B L I C   F U N C T I O N S  //
///////////////////////////////////////

func Start(logName, logPath, logLevel string, opts ...Option) (*Log, error) {
	if logName == "" {
		return nil, fmt.Errorf("log name cannot be empty")
	}
	if logPath == "" {
		logPath = "./"
	}
	logPath = filepath.Clean(logPath) + string(os.PathSeparator)

	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("path %s does not exist", logPath)
	}

	logLevel = strings.ToUpper(logLevel)
	if !verifyLevel(logLevel) {
		reportInternalError("warning: invalid log level '%s', falling back to INFO", logLevel)
		logLevel = Level.INFO
	}

	fullPath := filepath.Join(logPath, logName)
	f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	cfg := &config{
		bufferSize: DefaultBufferSize,
		batchSize:  DefaultBatchSize,
		flushEvery: flushInterval,
		bufferCap:  256 << 10, // default 256KB internal buffers for better large-log performance
		drainBurst: 512,       // default larger burst for better parallel throughput
	}
	for _, opt := range opts {
		opt(cfg)
	}

	// header := fmt.Sprintf("=== HumanJuan Logger v%s started at %s ===\n", version, time.Now().Format(time.RFC3339))
	// _, _ = f.WriteString(header)

	// Determine internal buffer capacity: prefer bufferCap, fall back to (deprecated) batchSize.
	internalCap := cfg.bufferCap
	if internalCap <= 0 {
		internalCap = cfg.batchSize
	}

	log := &Log{
		name:            logName,
		path:            logPath,
		level:           logLevel,
		maxSize:         0,
		maxRotation:     0,
		daily:           false,
		lastDay:         time.Now().Format(lastDayFormat),
		status:          true,
		events:          make(chan logEvent, cfg.bufferSize),
		buffer:          make([]byte, 0, internalCap),
		writeBuf:        make([]byte, 0, internalCap),
		flushEvery:      cfg.flushEvery,
		done:            make(chan struct{}),
		control:         make(chan controlReq, 8),
		drainBurstLimit: cfg.drainBurst,
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
				return
			}
			_log.writeEvent(ev)
			_log.drainBurst()

		case <-ticker.C:
			_log.flush()

		case req := <-_log.control:
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
					_log.forceDailyRotate = true
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

	atomic.AddUint64(&_log.dequeueSeq, 1)
}

func (_log *Log) drainBurst() {
	limit := _log.drainBurstLimit
	if limit <= 0 {
		limit = 256
	}
	for i := 0; i < limit; i++ {
		select {
		case ev := <-_log.events:
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
		case ev := <-_log.events:
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
	// Snapshot current enqueue sequence as the target barrier
	target := atomic.LoadUint64(&_log.enqueueSeq)
	ack := make(chan struct{})
	req := controlReq{target: target, ack: ack}
	_log.control <- req
	<-ack
}

func (_log *Log) flush() {
	if len(_log.buffer) == 0 {
		return
	}

	// Double buffer (lock-free swap on the hot path)
	_log.writeBuf, _log.buffer = _log.buffer, _log.writeBuf[:0]
	remaining := _log.writeBuf

	// Protect rotation and file I/O under a single mutex
	_log.mtx.Lock()
	defer _log.mtx.Unlock()

	// Rotation and file handling must be protected
	today := time.Now().Format(lastDayFormat)
	needDaily := false
	rotateDay := _log.lastDay

	if _log.daily && (today != _log.lastDay || _log.forceDailyRotate) {
		needDaily = true
	}

	if needDaily {
		if f := _log.getFile(); f != nil {
			// Write remaining data before rotation
			if written, _ := f.Write(remaining); written > 0 {
				_log.currentSize += int64(written)
			}
		}

		// Rotate using the previous day (lastDay) to name the rotated file
		_ = _log.rotateByDate(rotateDay)

		_log.lastDay = today
		_log.forceDailyRotate = false
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
			if written, _ := f.Write(remaining); written > 0 {
				_log.currentSize += int64(written)
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
				if written, _ := f.Write(line); written > 0 {
					_log.currentSize += int64(written)
				}
				remaining = remaining[len(line):]
				_log.logRotate()
				continue
			}
			// rotar primero
			_log.logRotate()
			continue
		}

		if written, _ := f.Write(line); written > 0 {
			_log.currentSize += int64(written)
		}
		remaining = remaining[len(line):]
	}

	// Limpiar writeBuf antes de salir del lock
	_log.writeBuf = _log.writeBuf[:0]
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

func escapeJSONInto(dst []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '\\', '"':
			dst = append(dst, '\\', c)
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		default:
			dst = append(dst, c)
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
