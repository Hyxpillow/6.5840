package raft

import "sync"
import "time"
import "math/rand"

type RaftTimer struct {
	timerChan chan bool
	lower int64	// 超时下限 (ms)
	randRange int64	// 随机范围 (ms)

	currentGap int64    // 当前随机产生的定时间隔
	lastReset int64    // 上次计时 (ms)
	mu sync.Mutex
}

// lower表示超时下限，randRange随机范围，单位均为ms
func (rt *RaftTimer) init(lower, randRange int64) {
	rt.timerChan = make(chan bool)
	rt.lower = lower
	rt.randRange = randRange
	rt.lastReset = time.Now().UnixNano() / 1e6
}

// 协程内运行  每隔20ms检测是否超时，如果超时，就写入channel，并重置更新时间
func (rt *RaftTimer) begin() {
	for {
		time.Sleep(time.Millisecond * 15)
		
		rt.mu.Lock()
		if  time.Now().UnixNano() / 1e6 - rt.lastReset < rt.currentGap {
			rt.mu.Unlock()
			continue
		}
		rt.timerChan <- true
		rt.mu.Unlock()
		
		rt.reset()
	}
}


func (rt *RaftTimer) reset() {
	rt.mu.Lock()
	rt.lastReset = time.Now().UnixNano() / 1e6
	rt.currentGap = rt.lower + (rand.Int63() % rt.randRange)
	rt.mu.Unlock()
}

func (rt *RaftTimer) getChan() chan bool {
	return rt.timerChan
}
