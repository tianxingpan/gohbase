// Package gohbase provides a pool of hbase clients
package gohbase

import (
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
	thbase "github.com/tianxingpan/gohbase/thrift"
)

// ThriftConn thrift连接
// 约束：同一个conn不应该同时被多个协程使用
type ThriftConn struct {
	Endpoint string          // 服务端的端点
	closed   bool            // 为 true 表示已被关闭，这种状态的不能再使用和放回池
	socket   *thrift.TSocket // thrift连接
	//transport	thrift.TTransport	// thrift transport
	usedTime time.Time // 最近使用时间
}

func (t *ThriftConn) GetEndpoint() string {
	return t.Endpoint
}

func (t *ThriftConn) GetSocket() *thrift.TSocket {
	return t.socket
}

//func (t *ThriftConn) GetTransport() thrift.TTransport {
//	return t.transport
//}

// GetUsedTime 纳秒
func (t *ThriftConn) GetUsedTime() int64 {
	return t.usedTime.UnixNano()
}

func (t *ThriftConn) UpdateUsedTime() int64 {
	t.usedTime = time.Now()
	return t.usedTime.UnixNano()
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

func (t *ThriftConn) GetHbaseClient() *thbase.THBaseServiceClient {
	transF := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protoF := thrift.NewTBinaryProtocolFactoryDefault()
	useTrans := transF.GetTransport(t.socket)
	return thbase.NewTHBaseServiceClientFactory(useTrans, protoF)
}
