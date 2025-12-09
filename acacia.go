////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//                                                                                                                    //
//  Author: Juan Alejandro Perez Chandia                                                                              //
//  Contact: juan.alejandro@humanjuan.com                                                                             //
//  Website: https://humanjuan.com/                                                                                   //
//                                                                                                                    //
//  HumanJuan Acacia - High-performance concurrent logger with real file rotation                                     //
//                                                                                                                    //
//  Version: 2.2.0                                                                                                    //
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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	version           = "2.2.0"
	DefaultBufferSize = 500_000
	MinBufferSize     = 1_000
	DefaultBatchSize  = 64 * 1024 // 64 kb
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
	batchSize  int
	flushEvery time.Duration
}

type Option func(*config)

func WithBufferSize(number int) Option {
	return func(conf *config) {
		if number >= MinBufferSize {
			conf.bufferSize = number
		}
	}
}

func WithBatchSize(number int) Option {
	return func(conf *config) {
		if number > 1024 {
			conf.batchSize = number
		}
	}
}

// WithFlushInterval permite configurar cada cuánto el writer dispara un flush periodico.
func WithFlushInterval(d time.Duration) Option {
	return func(conf *config) {
		if d > 0 {
			conf.flushEvery = d
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
	message           chan []byte
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
}

// controlReq es un mensaje de control hacia el writer.
// target indica el número de mensajes encolados que deben haber sido
// consumidos (y flushados) antes de responder el ack.
type controlReq struct {
	target uint64
	ack    chan struct{}
}

// logEvent representa un evento ligero que será formateado por la goroutine writer.
// Evita construir []byte por mensaje en el productor para reducir allocs/op.
type logEvent struct {
	level    string
	msgStr   string
	msgBytes []byte
	kind     uint8 // 0 = string, 1 = bytes
}

var (
	smallPool = sync.Pool{New: func() interface{} { return make([]byte, 0, 512) }}
	medPool   = sync.Pool{New: func() interface{} { return make([]byte, 0, 2048) }}
	midPool   = sync.Pool{New: func() interface{} { return make([]byte, 0, 4096) }}
	bigPool   = sync.Pool{New: func() interface{} { return make([]byte, 0, 8192) }}
)

// getBuf returns a small default buffer (legacy callers).
func getBuf() []byte {
	return smallPool.Get().([]byte)
}

// getBufCap returns a buffer with at least the requested capacity.
func getBufCap(n int) []byte {
	switch {
	case n <= 512:
		return smallPool.Get().([]byte)
	case n <= 2048:
		return medPool.Get().([]byte)
	case n <= 4096:
		return midPool.Get().([]byte)
	default:
		return bigPool.Get().([]byte)
	}
}

// putBuf returns the buffer to the appropriate pool based on capacity.
func putBuf(b []byte) {
	c := cap(b)
	switch {
	case c <= 512:
		smallPool.Put(b[:0])
	case c <= 2048:
		medPool.Put(b[:0])
	case c <= 4096:
		midPool.Put(b[:0])
	default:
		bigPool.Put(b[:0])
	}
}

///////////////////////////////////////
//       L O G   M E T H O D S       //
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
		atomic.AddUint64(&_log.enqueueSeq, 1)
		_log.message <- raw
		return
	}
	// FAST: sin formato y sin '%'
	if len(args) == 0 {
		if msgStr, ok := data.(string); ok {
			if strings.IndexByte(msgStr, '%') == -1 {
				atomic.AddUint64(&_log.enqueueSeq, 1)
				_log.events <- logEvent{level: level, msgStr: msgStr, kind: 0}
				return
			}
		}
	}

	msgStr := _log.formatMessageString(data, args...)
	raw := _log.setFormatBytesFromString(msgStr, level)
	atomic.AddUint64(&_log.enqueueSeq, 1)
	_log.message <- raw
}

func (_log *Log) logfBytes(level string, msgBytes []byte) {
	if !_log.shouldLog(level) {
		return
	}
	atomic.AddUint64(&_log.enqueueSeq, 1)
	_log.events <- logEvent{level: level, msgBytes: msgBytes, kind: 1}
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
	atomic.AddUint64(&_log.enqueueSeq, 1)
	_log.events <- logEvent{level: Level.INFO, msgBytes: p, kind: 1}
	return len(p), nil
}

func (_log *Log) Rotation(sizeMB int, backup int) {
	if backup < 1 {
		backup = 1
	}
	_log.maxRotation = backup

	if sizeMB <= 0 {
		_log.maxSize = 0
		return
	}
	_log.maxSize = int64(sizeMB) * 1024 * 1024
}

func (_log *Log) DailyRotation(enabled bool) {
	_log.mtx.Lock()
	_log.daily = enabled
	if enabled {
		_log.lastDay = time.Now().Format(lastDayFormat)
		_log.forceDailyRotate = true
	}
	_log.mtx.Unlock()
}

// app.log → app-2025-11-18.log
// app.log.0 → app-2025-11-18.log.0
// app.log.1 → app-2025-11-18.log.1
func (_log *Log) rotateByDate(day string) error {
	_log.mtx.Lock()
	base := _log.getFile().Name()
	dir, name := filepath.Dir(base), filepath.Base(base)
	oldFile := _log.getFile()
	maxRot := _log.maxRotation
	_log.mtx.Unlock()

	// baseName-YYYY-MM-DD.ext
	ext := filepath.Ext(name)
	baseNoExt := strings.TrimSuffix(name, ext)
	datedName := fmt.Sprintf("%s-%s%s", baseNoExt, day, ext)
	datedBase := filepath.Join(dir, datedName)

	limit := maxRot
	if limit <= 0 {
		limit = 1000 // Límite de seguridad
	}

	// Rotar backups fechados: dated.N -> dated.(N+1)
	for i := limit - 1; i >= 0; i-- {
		src := fmt.Sprintf("%s.%d", datedBase, i)
		dst := fmt.Sprintf("%s.%d", datedBase, i+1)
		if _, err := os.Stat(src); err == nil {
			if err := os.Rename(src, dst); err != nil {
				reportInternalError("rotating dated backup file %s: %v", src, err)
			}
		}
	}

	if err := os.Rename(base, datedBase); err != nil {
		reportInternalError("renaming base file to dated: %v", err)
	}

	newFile, err := os.OpenFile(base, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		reportInternalError("opening new file after daily rotation: %v", err)
		return err
	}
	_log.setFile(newFile)
	_log.currentSize = 0

	if oldFile != nil {
		if err := oldFile.Close(); err != nil {
			reportInternalError("closing old file after daily rotation: %v", err)
		}
	}
	return nil
}

func (_log *Log) logRotate() error {
	_log.mtx.Lock()
	base := _log.getFile().Name()
	oldFile := _log.getFile()
	maxRot := _log.maxRotation
	dailyEnabled := _log.daily
	today := time.Now().Format(lastDayFormat)
	_log.mtx.Unlock()

	targetStem := base
	if dailyEnabled {
		dir, name := filepath.Dir(base), filepath.Base(base)
		ext := filepath.Ext(name)
		baseNoExt := strings.TrimSuffix(name, ext)
		datedName := fmt.Sprintf("%s-%s%s", baseNoExt, today, ext)
		targetStem = filepath.Join(dir, datedName)
	}

	// Rotar la cadena existente targetStem.(n) -> targetStem.(n+1)
	for i := maxRot - 1; i >= 0; i-- {
		src := fmt.Sprintf("%s.%d", targetStem, i)
		dst := fmt.Sprintf("%s.%d", targetStem, i+1)
		if _, err := os.Stat(src); err == nil {
			if err := os.Rename(src, dst); err != nil {
				reportInternalError("rotating file %s: %v", src, err)
			}
		}
	}

	firstBackup := targetStem + ".0"
	if err := os.Rename(base, firstBackup); err != nil {
		reportInternalError("renaming base file for size rotation: %v", err)
	}

	newFile, err := os.OpenFile(base, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		reportInternalError("opening new file: %v", err)
		return err
	}
	_log.setFile(newFile)
	_log.currentSize = 0

	if oldFile != nil {
		if err := oldFile.Close(); err != nil {
			reportInternalError("closing old file after size rotation: %v", err)
		}
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
		close(_log.message)
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
	}
	for _, opt := range opts {
		opt(cfg)
	}

	// header := fmt.Sprintf("=== HumanJuan Logger v%s started at %s ===\n", version, time.Now().Format(time.RFC3339))
	// _, _ = f.WriteString(header)

	log := &Log{
		name:        logName,
		path:        logPath,
		level:       logLevel,
		maxSize:     0,
		maxRotation: 0,
		daily:       false,
		lastDay:     time.Now().Format(lastDayFormat),
		status:      true,
		message:     make(chan []byte, cfg.bufferSize),
		events:      make(chan logEvent, 4096),
		buffer:      make([]byte, 0, cfg.batchSize),
		writeBuf:    make([]byte, 0, cfg.batchSize),
		flushEvery:  cfg.flushEvery,
		done:        make(chan struct{}),
		control:     make(chan controlReq, 8),
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
	buf := getBuf()
	defer putBuf(buf)
	now := time.Now()
	buf = now.AppendFormat(buf, timestampFormat)
	cachedCopy := make([]byte, len(buf))
	copy(cachedCopy, buf)
	_log.cachedTime.Store(cachedCopy)
}

func (_log *Log) startWriting() {
	defer _log.wg.Done()
	interval := _log.flushEvery
	if interval <= 0 {
		interval = flushInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	batch := make([][]byte, 0, 1024)

	levelBytesFor := func(lvl string) []byte {
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
	appendLine := func(dst []byte, ts []byte, lvl []byte, msg string) []byte {
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
	appendLineBytes := func(dst []byte, ts []byte, lvl []byte, msg []byte) []byte {
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

	for {
		select {
		case first, ok := <-_log.message:
			if !ok {
				if len(batch) > 0 {
					_log.mtx.Lock()
					for i := range batch {
						_log.buffer = append(_log.buffer, batch[i]...)
						putBuf(batch[i])
					}
					_log.mtx.Unlock()
					batch = batch[:0]
				}
				// vaciar eventos pendientes antes de finalizar
				for {
					select {
					case ev, ok2 := <-_log.events:
						if !ok2 {
							_log.events = nil
							goto events_drained_on_close
						}
						var ts []byte
						if cachedTS := _log.cachedTime.Load(); cachedTS != nil {
							ts = cachedTS.([]byte)
						}
						lvl := levelBytesFor(ev.level)
						_log.mtx.Lock()
						if ev.kind == 0 {
							_log.buffer = appendLine(_log.buffer, ts, lvl, ev.msgStr)
						} else { // kind == 1 (bytes)
							_log.buffer = appendLineBytes(_log.buffer, ts, lvl, ev.msgBytes)
						}
						_log.mtx.Unlock()
					default:
						goto events_drained_on_close
					}
				}
			events_drained_on_close:
				_log.flush()
				return
			}

			batch = append(batch, first)
			qlen := len(_log.message)
			drainLimit := 256

			if qlen > 10_000 {
				drainLimit = 4096
			} else if qlen > 1000 {
				drainLimit = 1024
			}

			if qlen > 1000 && cap(batch) < 2048 {
				nb := make([][]byte, 0, 2048)
				nb = append(nb, batch...)
				batch = nb
			}
			for i := 1; i < drainLimit; i++ {
				select {
				case msg := <-_log.message:
					batch = append(batch, msg)
				default:
					i = drainLimit
				}
			}

			_log.mtx.Lock()
			for i := range batch {
				_log.buffer = append(_log.buffer, batch[i]...)
				putBuf(batch[i])
			}
			// Dispara flush más agresivo cuando el intervalo es corto (<= 100ms):
			// umbral = 2/3 de la capacidad; de lo contrario, 1/2 como antes.
			capBuf := cap(_log.buffer)
			threshold := capBuf / 2
			if interval <= 100*time.Millisecond {
				threshold = (capBuf * 2) / 3
			}
			shouldFlush := len(_log.buffer) >= threshold
			_log.mtx.Unlock()
			atomic.AddUint64(&_log.dequeueSeq, uint64(len(batch)))
			batch = batch[:0]

			if shouldFlush {
				_log.flush()
			}

		case ev, ok := <-_log.events:
			if !ok {
				_log.events = nil
				break
			}
			processed := 0
			var ts []byte
			if cachedTS := _log.cachedTime.Load(); cachedTS != nil {
				ts = cachedTS.([]byte)
			}
			lvl := levelBytesFor(ev.level)
			_log.mtx.Lock()
			if ev.kind == 0 {
				_log.buffer = appendLine(_log.buffer, ts, lvl, ev.msgStr)
			} else { // kind == 1 (bytes)
				_log.buffer = appendLineBytes(_log.buffer, ts, lvl, ev.msgBytes)
			}
			capBuf := cap(_log.buffer)
			threshold := capBuf / 2
			if interval <= 100*time.Millisecond {
				threshold = (capBuf * 2) / 3
			}
			shouldFlush := len(_log.buffer) >= threshold
			_log.mtx.Unlock()
			processed++

			// vaciar más eventos disponibles en ráfagas
			evDrain := 256
			qlen := len(_log.events)
			if qlen > 10_000 {
				evDrain = 4096
			} else if qlen > 1000 {
				evDrain = 1024
			}
			for i := 0; i < evDrain; i++ {
				select {
				case ev2 := <-_log.events:
					lvl2 := levelBytesFor(ev2.level)
					_log.mtx.Lock()
					if ev2.kind == 0 {
						_log.buffer = appendLine(_log.buffer, ts, lvl2, ev2.msgStr)
					} else {
						_log.buffer = appendLineBytes(_log.buffer, ts, lvl2, ev2.msgBytes)
					}
					if !shouldFlush {
						capBuf := cap(_log.buffer)
						threshold := capBuf / 2
						if interval <= 100*time.Millisecond {
							threshold = (capBuf * 2) / 3
						}
						if len(_log.buffer) >= threshold {
							shouldFlush = true
						}
					}
					_log.mtx.Unlock()
					processed++
				default:
					i = evDrain
				}
			}
			if processed > 0 {
				atomic.AddUint64(&_log.dequeueSeq, uint64(processed))
			}
			if shouldFlush {
				_log.flush()
			}

		case <-ticker.C:
			_log.flush()

		case req := <-_log.control:
			for {
				drained := make([][]byte, 0, 1024)
				drainedCount := 0
				for {
					select {
					case msg := <-_log.message:
						drained = append(drained, msg)
						drainedCount++
					default:
						goto drained_done
					}
				}
			drained_done:
				if drainedCount > 0 {
					_log.mtx.Lock()
					for i := range drained {
						_log.buffer = append(_log.buffer, drained[i]...)
						putBuf(drained[i])
					}
					_log.mtx.Unlock()
				}

				evCount := 0
				var ts2 []byte
				if cachedTS := _log.cachedTime.Load(); cachedTS != nil {
					ts2 = cachedTS.([]byte)
				}
				for {
					select {
					case ev := <-_log.events:
						lvl := levelBytesFor(ev.level)
						_log.mtx.Lock()
						if ev.kind == 0 {
							_log.buffer = appendLine(_log.buffer, ts2, lvl, ev.msgStr)
						} else {
							_log.buffer = appendLineBytes(_log.buffer, ts2, lvl, ev.msgBytes)
						}
						_log.mtx.Unlock()
						evCount++
					default:
						goto drained_events_done
					}
				}
			drained_events_done:
				_log.flush()

				if drainedCount > 0 {
					atomic.AddUint64(&_log.dequeueSeq, uint64(drainedCount))
				}
				if evCount > 0 {
					atomic.AddUint64(&_log.dequeueSeq, uint64(evCount))
				}

				if atomic.LoadUint64(&_log.dequeueSeq) >= req.target {
					if req.ack != nil {
						close(req.ack)
					}
					break
				}
			}
		}
	}
}

func (_log *Log) Sync() {
	target := atomic.LoadUint64(&_log.enqueueSeq)
	ack := make(chan struct{})
	req := controlReq{target: target, ack: ack}

	select {
	case _log.control <- req:
		// ok
	case <-time.After(2 * time.Second):
		// fallback: no bloquear al caller si el writer no responde
	}

	select {
	case <-ack:
	case <-time.After(5 * time.Second):
	}
	if f := _log.getFile(); f != nil {
		_ = f.Sync()
	}
}

func (_log *Log) flush() {
	_log.mtx.Lock()
	_log.buffer, _log.writeBuf = _log.writeBuf[:0], _log.buffer

	needDaily := false
	dayForRotate := ""
	if _log.daily {
		if _log.forceDailyRotate {
			needDaily = true
			dayForRotate = _log.lastDay
		} else {
			today := time.Now().Format(lastDayFormat)
			if today != _log.lastDay {
				needDaily = true
				dayForRotate = _log.lastDay
			}
		}
	}
	_log.mtx.Unlock()

	remaining := _log.writeBuf

	if needDaily {
		if f := _log.getFile(); f != nil && len(remaining) > 0 {
			if written, _ := f.Write(remaining); written > 0 {
				_log.currentSize += int64(written)
			}
		}
		_ = _log.rotateByDate(dayForRotate)
		_log.mtx.Lock()
		_log.lastDay = time.Now().Format(lastDayFormat)
		_log.forceDailyRotate = false
		_log.mtx.Unlock()
		_log.writeBuf = _log.writeBuf[:0]
		return
	}

	for len(remaining) > 0 {
		f := _log.getFile()
		if f == nil {
			break
		}

		if _log.maxSize <= 0 {
			if written, _ := f.Write(remaining); written > 0 {
				_log.currentSize += int64(written)
			}
			remaining = remaining[:0]
			break
		}

		lineEnd := bytes.IndexByte(remaining, '\n')
		var line []byte
		if lineEnd >= 0 {
			line = remaining[:lineEnd+1]
		} else {
			line = remaining
		}

		cur := _log.currentSize
		if cur >= _log.maxSize {
			_ = _log.logRotate()
			continue
		}
		allowed := _log.maxSize - cur
		if int64(len(line)) > allowed && cur > 0 {
			_ = _log.logRotate()
			continue
		}

		if int64(len(line)) > allowed && cur == 0 {
			if written, _ := f.Write(line); written > 0 {
				_log.currentSize += int64(written)
			}
			remaining = remaining[len(line):]
			_ = _log.logRotate()
			continue
		}

		if written, _ := f.Write(line); written > 0 {
			_log.currentSize += int64(written)
		}
		remaining = remaining[len(line):]
	}
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
	var ts string
	if cachedTS := _log.cachedTime.Load(); cachedTS != nil {
		ts = string(cachedTS.([]byte))
	} else {
		ts = time.Now().Format(timestampFormat)
	}

	finalFields := make(map[string]interface{}, len(fields)+2)
	finalFields["ts"] = ts
	finalFields["level"] = level

	for k, v := range fields {
		finalFields[k] = v
	}

	jsonBytes, err := json.Marshal(finalFields)
	if err != nil {
		fallback := fmt.Sprintf(`{"ts":"%s","level":"CRITICAL","msg":"Acacia JSON Marshal failed: %v"}`, ts, err)
		return []byte(fallback)
	}

	buf := getBuf()
	buf = append(buf, jsonBytes...)
	buf = append(buf, '\n')

	return buf
}

func (_log *Log) setFormatBytesFromString(msg string, level string) []byte {
	var tsBytes []byte
	if cachedTS := _log.cachedTime.Load(); cachedTS != nil {
		tsBytes = cachedTS.([]byte)
	}

	var levelBytes []byte
	switch level {
	case Level.DEBUG:
		levelBytes = levelDebug
	case Level.INFO:
		levelBytes = levelInfo
	case Level.WARN:
		levelBytes = levelWarn
	case Level.ERROR:
		levelBytes = levelError
	case Level.CRITICAL:
		levelBytes = levelCritical
	}

	need := len(tsBytes) + 1 + 1 + len(levelBytes) + 2 + len(msg) + 1
	if need <= 0 {
		need = 64 // fallback minimal
	}
	buf := getBufCap(need)

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
