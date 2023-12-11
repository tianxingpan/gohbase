// Package gohbase provides a pool of hbase clients
package gohbase

import (
	"sync/atomic"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/tianxingpan/gohbase/hbase"
)

// ThriftConn thrift连接
// 约束：同一个conn不应该同时被多个协程使用
type ThriftConn struct {
	Endpoint   string          // 服务端的端点
	closed     bool            // 为 true 表示已被关闭，这种状态的不能再使用和放回池
	socket     *thrift.TSocket // thrift连接
	usedTime   atomic.Value    // 最近使用时间
	createTime time.Time       // 链接创建时间
	pooled     bool
}

func (t *ThriftConn) GetEndpoint() string {
	return t.Endpoint
}

func (t *ThriftConn) GetSocket() *thrift.TSocket {
	return t.socket
}

func (t *ThriftConn) UsedTime() time.Time {
	return t.usedTime.Load().(time.Time)
}

func (t *ThriftConn) SetUsedTime(tm time.Time) {
	t.usedTime.Store(tm)
}

// GetUsedTime 纳秒
func (t *ThriftConn) GetUsedTime() int64 {
	ut := t.UsedTime()
	return ut.UnixNano()
}

func (t *ThriftConn) UpdateUsedTime() int64 {
	tm := time.Now()
	t.SetUsedTime(tm)
	return tm.UnixNano()
}

// Close 关闭thrift连接
func (t *ThriftConn) Close() error {
	if t.closed {
		return nil
	}
	t.closed = true
	return t.socket.Close()
}

// IsClose 是否关闭
func (t *ThriftConn) IsClose() bool {
	return t.closed
}

func (t *ThriftConn) GetHbaseClient() *hbase.THBaseServiceClient {
	transF := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protoF := thrift.NewTBinaryProtocolFactoryDefault()
	useTrans := transF.GetTransport(t.socket)
	return hbase.NewTHBaseServiceClientFactory(useTrans, protoF)
}

func NewThriftConn(endpoint string, dialTimeout time.Duration) (*ThriftConn, error) {
	var err error
	var socket *thrift.TSocket

	if dialTimeout > 0 {
		socket, err = thrift.NewTSocketTimeout(endpoint, dialTimeout)
	} else {
		socket, err = thrift.NewTSocket(endpoint)
	}

	if err != nil {
		return nil, err
	}

	err = socket.Open()
	if err != nil {
		return nil, err
	}
	conn := &ThriftConn{
		Endpoint:   endpoint,
		closed:     false,
		socket:     socket,
		createTime: time.Now(),
	}
	_ = conn.UpdateUsedTime()
	return conn, nil
}
