package paxi

// 这段代码定义了一个用于控制操作速率的 Limiter 结构体及其方法。它通过等待时间来限制操作的频率，确保每秒最多执行指定次数的操作。
import (
	"sync"
	"time"
)

// Limiter limits operation rate when used with Wait function
type Limiter struct {
	sync.Mutex
	last     time.Time
	sleep    time.Duration
	interval time.Duration
	slack    time.Duration
}

// NewLimiter creates a new rate limiter, where rate is operations per second
func NewLimiter(rate int) *Limiter {
	return &Limiter{
		interval: time.Second / time.Duration(rate),
		slack:    -10 * time.Second / time.Duration(rate),
	}
}

// Wait blocks for the limit
func (l *Limiter) Wait() {
	l.Lock()
	defer l.Unlock()

	now := time.Now()

	if l.last.IsZero() {
		l.last = now
		return
	}

	l.sleep += l.interval - now.Sub(l.last)

	if l.sleep < l.slack {
		l.sleep = l.slack
	}

	if l.sleep > 0 {
		time.Sleep(l.sleep)
		l.last = now.Add(l.sleep)
		l.sleep = 0
	} else {
		l.last = now
	}
}
