package mcp

import (
	"fmt"
	"sync"
	"time"
)

type BudgetLimits struct {
	MaxCallsPerMinute int           `yaml:"max_calls_per_minute"`
	MaxCallsPerHour   int           `yaml:"max_calls_per_hour"`
	MaxCallsPerDay    int           `yaml:"max_calls_per_day"`
	TimeoutSec        int           `yaml:"timeout_sec"`
	MaxExecTimeSec    int           `yaml:"max_total_execution_time_sec"`
}

type BudgetManager struct {
	globalLimits  BudgetLimits
	perToolLimits map[string]BudgetLimits

	minuteWindow  *timeWindow
	hourWindow    *timeWindow
	dayWindow     *timeWindow
	toolCounters  map[string]*toolCounters

	mu sync.RWMutex
}

type timeWindow struct {
	count     int
	startTime time.Time
	duration  time.Duration
	mu        sync.Mutex
}

type toolCounters struct {
	minute *timeWindow
	hour   *timeWindow
	day    *timeWindow
	totalExecTime time.Duration
}

func NewBudgetManager(global BudgetLimits, perTool map[string]BudgetLimits) *BudgetManager {
	bm := &BudgetManager{
		globalLimits:  global,
		perToolLimits: perTool,
		minuteWindow:  newTimeWindow(time.Minute),
		hourWindow:    newTimeWindow(time.Hour),
		dayWindow:     newTimeWindow(24 * time.Hour),
		toolCounters:  make(map[string]*toolCounters),
	}

	for toolName := range perTool {
		bm.toolCounters[toolName] = &toolCounters{
			minute: newTimeWindow(time.Minute),
			hour:   newTimeWindow(time.Hour),
			day:    newTimeWindow(24 * time.Hour),
		}
	}

	return bm
}

func newTimeWindow(duration time.Duration) *timeWindow {
	return &timeWindow{
		startTime: time.Now(),
		duration:  duration,
	}
}

func (tw *timeWindow) increment() error {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	now := time.Now()
	if now.Sub(tw.startTime) > tw.duration {
		tw.count = 0
		tw.startTime = now
	}

	tw.count++
	return nil
}

func (tw *timeWindow) check(limit int) bool {
	if limit <= 0 {
		return true
	}

	tw.mu.Lock()
	defer tw.mu.Unlock()

	now := time.Now()
	if now.Sub(tw.startTime) > tw.duration {
		return true
	}

	return tw.count < limit
}

func (bm *BudgetManager) CheckAndIncrement(toolName string) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if !bm.minuteWindow.check(bm.globalLimits.MaxCallsPerMinute) {
		return fmt.Errorf("global rate limit exceeded: max %d calls per minute", bm.globalLimits.MaxCallsPerMinute)
	}
	if !bm.hourWindow.check(bm.globalLimits.MaxCallsPerHour) {
		return fmt.Errorf("global rate limit exceeded: max %d calls per hour", bm.globalLimits.MaxCallsPerHour)
	}

	if limits, exists := bm.perToolLimits[toolName]; exists {
		counters := bm.getOrCreateCounters(toolName)

		if !counters.minute.check(limits.MaxCallsPerMinute) {
			return fmt.Errorf("tool '%s' rate limit exceeded: max %d calls per minute", toolName, limits.MaxCallsPerMinute)
		}
		if !counters.hour.check(limits.MaxCallsPerHour) {
			return fmt.Errorf("tool '%s' rate limit exceeded: max %d calls per hour", toolName, limits.MaxCallsPerHour)
		}
		if !counters.day.check(limits.MaxCallsPerDay) {
			return fmt.Errorf("tool '%s' rate limit exceeded: max %d calls per day", toolName, limits.MaxCallsPerDay)
		}

		if limits.MaxExecTimeSec > 0 {
			maxDuration := time.Duration(limits.MaxExecTimeSec) * time.Second
			if counters.totalExecTime >= maxDuration {
				return fmt.Errorf("tool '%s' execution time limit exceeded: max %d seconds", toolName, limits.MaxExecTimeSec)
			}
		}
	}

	bm.minuteWindow.increment()
	bm.hourWindow.increment()
	if counters, exists := bm.toolCounters[toolName]; exists {
		counters.minute.increment()
		counters.hour.increment()
		counters.day.increment()
	}

	return nil
}

func (bm *BudgetManager) RecordExecution(toolName string, duration time.Duration) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if counters, exists := bm.toolCounters[toolName]; exists {
		counters.totalExecTime += duration
	}
}

func (bm *BudgetManager) GetTimeout(toolName string) time.Duration {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	if limits, exists := bm.perToolLimits[toolName]; exists && limits.TimeoutSec > 0 {
		return time.Duration(limits.TimeoutSec) * time.Second
	}

	return 30 * time.Second
}

func (bm *BudgetManager) getOrCreateCounters(toolName string) *toolCounters {
	if counters, exists := bm.toolCounters[toolName]; exists {
		return counters
	}

	counters := &toolCounters{
		minute: newTimeWindow(time.Minute),
		hour:   newTimeWindow(time.Hour),
		day:    newTimeWindow(24 * time.Hour),
	}
	bm.toolCounters[toolName] = counters
	return counters
}
