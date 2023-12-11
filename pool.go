// Package gohbase provides a pool of hbase clients

package gohbase

import (
	"sync"
	"sync/atomic"
	"time"
)

var timers = sync.Pool{
	New: func() interface{} {
		t := time.NewTimer(time.Hour)
		t.Stop()
		return t
	},
}

// Stats contains pool state information and accumulated stats.
type Stats struct {
	Hits     uint32 // number of times free connection was found in the pool
	Misses   uint32 // number of times free connection was NOT found in the pool
	Timeouts uint32 // number of times a wait timeout occurred

	TotalConns uint32 // number of total connections in the pool
	IdleConns  uint32 // number of idle connections in the pool
	StaleConns uint32 // number of stale connections removed from the pool
}

// Thrift连接池
type ThriftConnPool struct {
	opt             *Options
	dialErrorsNum   uint32 // atomic
	lastDialErrorMu sync.RWMutex
	lastDialError   error
	poolMu          sync.Mutex    // lock
	queue           chan struct{} // 在用的链接队列
	conns           []*ThriftConn
	idleConns       []*ThriftConn
	poolSize        int
	idleConnsLen    int
	stats           Stats
	_closed         uint32 // atomic
}

func NewThriftConnPool(opt *Options) *ThriftConnPool {
	p := &ThriftConnPool{
		opt:       opt,
		queue:     make(chan struct{}, opt.PoolSize),
		conns:     make([]*ThriftConn, 0, opt.PoolSize),
		idleConns: make([]*ThriftConn, 0, opt.PoolSize),
	}

	for i := 0; i < opt.MinIdleConns; i++ {
		p.checkMinIdleConns()
	}

	if opt.IdleTimeout > 0 && opt.IdleCheckFrequency > 0 {
		go p.reaper(opt.IdleCheckFrequency)
	}

	return p
}

func (tp *ThriftConnPool) closed() bool {
	return atomic.LoadUint32(&tp._closed) == 1
}

func (tp *ThriftConnPool) reapStaleConn() *ThriftConn {
	if len(tp.idleConns) == 0 {
		return nil
	}

	cn := tp.idleConns[0]
	if !tp.isStaleConn(cn) {
		return nil
	}

	tp.idleConns = append(tp.idleConns[:0], tp.idleConns[1:]...)
	tp.idleConnsLen--

	return cn
}

func (tp *ThriftConnPool) ReapStaleConns() (int, error) {
	var n int
	for {
		tp.getTurn()

		tp.poolMu.Lock()
		cn := tp.reapStaleConn()
		tp.poolMu.Unlock()

		if cn != nil {
			tp.removeConn(cn)
		}

		tp.freeTurn()

		if cn != nil {
			cn.Close()
			n++
		} else {
			break
		}
	}
	return n, nil
}

func (tp *ThriftConnPool) reaper(frequency time.Duration) {
	ticker := time.NewTicker(frequency)
	defer ticker.Stop()

	for range ticker.C {
		if tp.closed() {
			break
		}
		n, err := tp.ReapStaleConns()
		if err != nil {
			// internal.Logf("ReapStaleConns failed: %s", err)
			continue
		}
		atomic.AddUint32(&tp.stats.StaleConns, uint32(n))
	}
}

func (tp *ThriftConnPool) getTurn() {
	tp.queue <- struct{}{}
}

func (tp *ThriftConnPool) waitTurn() error {
	select {
	case tp.queue <- struct{}{}:
		return nil
	default:
		timer := timers.Get().(*time.Timer)
		timer.Reset(tp.opt.PoolTimeout)

		select {
		case tp.queue <- struct{}{}:
			if !timer.Stop() {
				<-timer.C
			}
			timers.Put(timer)
			return nil
		case <-timer.C:
			timers.Put(timer)
			atomic.AddUint32(&tp.stats.Timeouts, 1)
			return ErrPoolTimeout
		}
	}
}

func (tp *ThriftConnPool) freeTurn() {
	<-tp.queue
}

func (tp *ThriftConnPool) setLastDialError(err error) {
	tp.lastDialErrorMu.Lock()
	tp.lastDialError = err
	tp.lastDialErrorMu.Unlock()
}

func (tp *ThriftConnPool) getLastDialError() error {
	tp.lastDialErrorMu.RLock()
	err := tp.lastDialError
	tp.lastDialErrorMu.RUnlock()
	return err
}

func (tp *ThriftConnPool) Stats() *Stats {
	idleLen := tp.IdleLen()
	return &Stats{
		Hits:     atomic.LoadUint32(&tp.stats.Hits),
		Misses:   atomic.LoadUint32(&tp.stats.Misses),
		Timeouts: atomic.LoadUint32(&tp.stats.Timeouts),

		TotalConns: uint32(tp.Len()),
		IdleConns:  uint32(idleLen),
		StaleConns: atomic.LoadUint32(&tp.stats.StaleConns),
	}
}

// 尝试拨号/链接
func (tp *ThriftConnPool) tryDial() {
	for {
		if tp.closed() {
			return
		}

		conn, err := NewThriftConn(tp.opt.Addr, tp.opt.DialTimeout)
		if err != nil {
			tp.setLastDialError(err)
			time.Sleep(time.Second)
			continue
		}

		atomic.StoreUint32(&tp.dialErrorsNum, 0)
		_ = conn.Close()
		return
	}
}

func (tp *ThriftConnPool) newConn(pooled bool) (*ThriftConn, error) {
	if tp.closed() {
		return nil, ErrClosed
	}

	if atomic.LoadUint32(&tp.dialErrorsNum) >= uint32(tp.opt.PoolSize) {
		return nil, tp.getLastDialError()
	}

	conn, err := NewThriftConn(tp.opt.Addr, tp.opt.DialTimeout)
	if err != nil {
		tp.setLastDialError(err)
		if atomic.AddUint32(&tp.dialErrorsNum, 1) == uint32(tp.opt.PoolSize) {
			go tp.tryDial()
		}
		return nil, err
	}
	conn.pooled = pooled
	return conn, nil
}

func (tp *ThriftConnPool) addIdleConn() {
	cn, err := tp.newConn(true)
	if err != nil {
		return
	}

	tp.poolMu.Lock()
	tp.conns = append(tp.conns, cn)
	tp.idleConns = append(tp.idleConns, cn)
	tp.poolMu.Unlock()
}

func (tp *ThriftConnPool) checkMinIdleConns() {
	if tp.opt.MinIdleConns == 0 {
		return
	}
	if tp.poolSize < tp.opt.PoolSize && tp.idleConnsLen < tp.opt.MinIdleConns {
		tp.poolSize++
		tp.idleConnsLen++
		go tp.addIdleConn()
	}
}

func (tp *ThriftConnPool) popIdle() *ThriftConn {
	if len(tp.idleConns) == 0 {
		return nil
	}

	idx := len(tp.idleConns) - 1
	cn := tp.idleConns[idx]
	tp.idleConns = tp.idleConns[:idx]
	tp.idleConnsLen--
	tp.checkMinIdleConns()
	return cn
}

func (tp *ThriftConnPool) isStaleConn(cn *ThriftConn) bool {
	if tp.opt.IdleTimeout == 0 {
		return false
	}

	now := time.Now()
	if tp.opt.IdleTimeout > 0 && now.Sub(cn.UsedTime()) >= tp.opt.IdleTimeout {
		return true
	}

	return false
}

func (tp *ThriftConnPool) removeConn(cn *ThriftConn) {
	tp.poolMu.Lock()
	for i, c := range tp.conns {
		if c == cn {
			tp.conns = append(tp.conns[:i], tp.conns[i+1:]...)
			if cn.pooled {
				tp.poolSize--
				tp.checkMinIdleConns()
			}
			break
		}
	}
	tp.poolMu.Unlock()
}

func (tp *ThriftConnPool) Remove(cn *ThriftConn, reason error) {
	tp.removeConn(cn)
	tp.freeTurn()
	_ = cn.Close()
}

// CloseConn 关闭链接并从连接池中移除
func (tp *ThriftConnPool) CloseConn(cn *ThriftConn) error {
	tp.removeConn(cn)
	return cn.Close()
}

// NewConn 创建链接
func (tp *ThriftConnPool) NewConn(pooled bool) (*ThriftConn, error) {
	cn, err := tp.newConn(pooled)
	if err != nil {
		return nil, err
	}

	tp.poolMu.Lock()
	tp.conns = append(tp.conns, cn)
	if pooled {
		if tp.poolSize < tp.opt.PoolSize {
			tp.poolSize++
		} else {
			cn.pooled = false
		}
	}
	tp.poolMu.Unlock()
	return cn, nil
}

//
func (tp *ThriftConnPool) Get() (*ThriftConn, error) {
	if tp.closed() {
		return nil, ErrClosed
	}

	err := tp.waitTurn()
	if err != nil {
		return nil, err
	}

	for {
		tp.poolMu.Lock()
		cn := tp.popIdle()
		tp.poolMu.Unlock()

		if cn == nil {
			break
		}

		if tp.isStaleConn(cn) {
			_ = tp.CloseConn(cn)
			continue
		}

		atomic.AddUint32(&tp.stats.Hits, 1)
		return cn, nil
	}

	atomic.AddUint32(&tp.stats.Misses, 1)

	newcn, err := tp.NewConn(true)
	if err != nil {
		tp.freeTurn()
		return nil, err
	}

	return newcn, nil
}

//
func (tp *ThriftConnPool) Put(cn *ThriftConn) {
	if !cn.pooled {
		tp.Remove(cn, nil)
		return
	}

	tp.poolMu.Lock()
	_ = cn.UpdateUsedTime()
	tp.idleConns = append(tp.idleConns, cn)
	tp.idleConnsLen++
	tp.poolMu.Unlock()
	tp.freeTurn()
}

// Len returns total number of connections.
func (tp *ThriftConnPool) Len() int {
	tp.poolMu.Lock()
	n := len(tp.conns)
	tp.poolMu.Unlock()
	return n
}

// IdleLen returns number of idle connections.
func (tp *ThriftConnPool) IdleLen() int {
	tp.poolMu.Lock()
	n := tp.idleConnsLen
	tp.poolMu.Unlock()
	return n
}

func (tp *ThriftConnPool) Close() error {
	if !atomic.CompareAndSwapUint32(&tp._closed, 0, 1) {
		return ErrClosed
	}

	var firstErr error
	tp.poolMu.Lock()
	for _, cn := range tp.conns {
		if err := cn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	tp.conns = nil
	tp.poolSize = 0
	tp.idleConns = nil
	tp.idleConnsLen = 0
	tp.poolMu.Unlock()

	return firstErr
}
