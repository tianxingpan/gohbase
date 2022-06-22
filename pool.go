// Package gohbase provides a pool of hbase clients

package gohbase

import (
	"fmt"
	"sync/atomic"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
)

type HBase struct {
	Endpoint    string           // 服务端的端点
	DialTimeout time.Duration    // 拨号超时/连接超时
	IdleTimeout time.Duration    // 空闲连接超时时长，默认10s
	MaxSize     int32            // 连接池最大连接数，如果没有设置最大值，默认100个
	InitSize    int32            // 连接池初始连接数，最小值为1
	used        int32            // 已用连接数
	idle        int32            // 空闲连接数（即在 clients 中的连接数）
	assessTime  int64            // 最近异常调用Get或者Put的时间，根据它来判定该池是否活跃
	_closed     uint32           // 关闭连接池
	clients     chan *ThriftConn // thrift连接队列
}

// type ConnPool struct {
// }

// Get 从连接池取一个连接，
// 应和 Put 一对一成对调用
// 返回两个值：
// 1) ThriftConn 指针
// 2) 错误信息
func (h *HBase) Get() (*ThriftConn, error) {
	return h.get(false)
}

func (h *HBase) get(doNotNew bool) (*ThriftConn, error) {
	accessTime := time.Now().Unix()
	atomic.StoreInt64(&h.assessTime, accessTime)
	curUsed := h.addUsed()

	select {
	case conn := <-h.clients:
		h.subIdle()
		return conn, nil
	default:
		if doNotNew {
			h.subUsed()
			return nil, nil
		}
		if curUsed > h.MaxSize {
			newUsed := h.subUsed()
			return nil, fmt.Errorf("thriftpool empty, used:%d/%d, init:%d, max:%d",
				curUsed, newUsed, h.InitSize, h.MaxSize)
		}
		var err error
		var socket *thrift.TSocket

		if h.DialTimeout > 0 {
			socket, err = thrift.NewTSocketTimeout(h.Endpoint, h.DialTimeout)
		} else {
			socket, err = thrift.NewTSocket(h.Endpoint)
		}

		if err != nil {
			// 错误处理还得继续
			h.subUsed()
			return nil, err
		}

		err = socket.Open()
		if err != nil {
			// 错误错误处理
			h.subUsed()
			return nil, err
		}
		conn := new(ThriftConn)
		conn.Endpoint = h.Endpoint
		conn.closed = false
		conn.socket = socket
		conn.usedTime = time.Now()
		return conn, nil
	}
}

// Put 连接用完后归还回池，应和 Get 一对一成对调用
// 约束：同一 conn 不应同时被多个协程使用
// 传参：
// ThriftConn指针
// 返回值：
// 2) 错误信息
func (h *HBase) Put(conn *ThriftConn) error {
	return h.put(conn, false)
}

func (h *HBase) put(conn *ThriftConn, doNotNew bool) error {
	accessTime := time.Now().Unix()
	atomic.StoreInt64(&h.assessTime, accessTime)
	defer func() {
		// 捕获panic，因为channel关闭时，再向关闭的channel写数据时，会导致panic
		if err := recover(); err != nil {
			_ = conn.Close()
			h.subIdle()
		}
	}()

	used := h.subUsed()
	closed := atomic.LoadUint32(&h._closed)
	if closed == 1 {
		if !conn.IsClose() {
			_ = conn.Close()
		}
		return nil
	}
	if conn.IsClose() {
		// 如果ThriftConn关闭时，无需返回队列
		return nil
	}
	idle := h.addIdle()
	usedTime := conn.GetUsedTime()
	var nowTime int64
	if !doNotNew {
		nowTime = conn.UpdateUsedTime()
	} else {
		nowTime = time.Now().UnixNano()
	}

	if idle > h.InitSize {
		if nowTime > usedTime {
			iTime := nowTime - usedTime
			if iTime > int64(h.IdleTimeout) {
				_ = conn.Close()
				h.subIdle()
				// 闲置连接，回收连接资源
				return nil
			}
			// 创建的资源大于最大连接数时，关闭连接，回收连接资源
			if idle > h.MaxSize {
				_ = conn.Close()
				h.subIdle()
				return nil
			}
		}
	}
	select {
	case h.clients <- conn:
		return nil
	default:
		_ = conn.Close()
		h.subIdle()
		return fmt.Errorf("use:%d, init:%d, idle:%d", used, h.InitSize, h.GetIdle())
	}
}

func (h *HBase) GetAssessTime() int64 {
	return atomic.LoadInt64(&h.assessTime)
}

// Close 关闭连接池（释放资源）
func (h *HBase) Close() {
	swp := atomic.CompareAndSwapUint32(&h._closed, 0, 1)
	if !swp {
		return
	}

	close(h.clients)
	for conn := range h.clients {
		if conn == nil {
			continue
		}
		_ = conn.Close()
	}
	h.used = 0
	h.idle = 0
}

func (h *HBase) closed() bool {
	return atomic.LoadUint32(&h._closed) == 1
}

func (h *HBase) reaper(frequency time.Duration) {
	ticker := time.NewTicker(frequency)
	defer ticker.Stop()

	for range ticker.C {
		if h.closed() {
			break
		}
		initSize := h.GetInitSize()
		idleSize := h.GetIdle()
		usedSize := h.GetUsed()
		// 当闲置连接大于在用连接，说明连接池比较空闲
		if idleSize > initSize && usedSize < idleSize {
			for i := 0; i < int(idleSize); i++ {
				conn, _ := h.get(true)
				if conn == nil {
					break
				}
				err := h.put(conn, true)
				if err != nil {
					fmt.Printf("relase idle Conn failed:%s\n", err.Error())
				}
			}
		}
	}
}

func (h *HBase) addUsed() int32 {
	return atomic.AddInt32(&h.used, 1)
}

func (h *HBase) subUsed() int32 {
	return atomic.AddInt32(&h.used, -1)
}

func (h *HBase) addIdle() int32 {
	return atomic.AddInt32(&h.idle, 1)
}

func (h *HBase) subIdle() int32 {
	return atomic.AddInt32(&h.idle, -1)
}

func (h *HBase) GetIdle() int32 {
	return atomic.LoadInt32(&h.idle)
}

func (h *HBase) GetUsed() int32 {
	return atomic.LoadInt32(&h.used)
}

func (h *HBase) GetInitSize() int32 {
	return h.InitSize
}

func (h *HBase) GetMaxSize() int32 {
	return h.MaxSize
}

func (h *HBase) GetEndpoint() string {
	return h.Endpoint
}

func (h *HBase) SetIdleTimeout(timeout int32) {
	if timeout < 1 {
		h.IdleTimeout = time.Duration(1000) * time.Millisecond
	} else {
		h.IdleTimeout = time.Duration(timeout) * time.Millisecond
	}
}

func (h *HBase) SetDialTimeout(timeout int32) {
	if timeout < 1 {
		h.DialTimeout = time.Duration(1000) * time.Millisecond
	} else {
		h.DialTimeout = time.Duration(timeout) * time.Millisecond
	}
}

func (h *HBase) GetChanSize() int32 {
	tmp := len(h.clients)
	return int32(tmp)
}
