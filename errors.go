// Package gohbase provides a pool of hbase clients

package gohbase

import "errors"

var (
	ErrClosed      = errors.New("HBase: client is closed")
	ErrPoolTimeout = errors.New("HBase: connection pool timeout")
)
