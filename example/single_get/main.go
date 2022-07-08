// package main provides gohbase test cases
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/tianxingpan/gohbase"
	"github.com/tianxingpan/gohbase/hbase"
)

var (
	help        = flag.Bool("h", false, "Display a help message and exit")
	addr        = flag.String("addr", "127.0.0.1:9898", "Server of Thrift to connect.")
	minSize     = flag.Int("min_size", 1, "Initial size of Thrift pool.")
	maxSize     = flag.Int("max_size", 3, "Max size of Thrift pool.")
	dialTimeout = flag.Uint("dial_timeout", 5000, "Dial timeout in Millisecond.")
	idleTimeout = flag.Uint("idle_timeout", 5000, "Idle timeout in Millisecond.")
	table       = flag.String("table", "", "HBase table.")
	row         = flag.String("rowkey", "", "HBase row.")
	columns     = flag.String("columns", "", "HBase colums. Format: 'cf1:q1,q2' or 'cf1:q1,cf2:q2'")
)

func main() {
	flag.Parse()
	if *help {
		flag.Usage()
		os.Exit(1)
	}
	if !checkParams() {
		flag.Usage()
		os.Exit(-1)
	}
	hb := gohbase.NewHBase(&gohbase.Options{
		Addr:         *addr,
		MaxRetries:   0,
		DialTimeout:  time.Duration(*dialTimeout) * time.Millisecond,
		IdleTimeout:  time.Duration(*idleTimeout) * time.Millisecond,
		PoolSize:     *maxSize,
		MinIdleConns: *minSize,
	})
	cm := hbase.TGet{
		Row:     []byte(*row),
		Columns: []*hbase.TColumn{},
	}
	if *columns != "" {
		cols := strings.Split(*columns, ",")
		for _, col := range cols {
			fq := strings.Split(col, ":")
			if len(fq) < 2 {
				panic("Parameter[-columns] format error, Format:.'cf1:q1,q2' or 'cf1:q1,cf2:q2'")
			} else {
				qs := strings.Split(fq[1], ",")
				for _, v := range qs {
					cm.Columns = append(cm.Columns, &hbase.TColumn{
						Family:    []byte(fq[0]),
						Qualifier: []byte(v),
					})
				}
			}
		}
	}

	r, err := hb.Get([]byte(*table), &cm)
	if err != nil {
		panic(err.Error())
	}
	fmt.Printf("result=%v", r)
}

func checkParams() bool {
	if *addr == "" {
		fmt.Println("Parameter[-addr] is not set.")
		return false
	}
	if *table == "" {
		fmt.Println("Parameter[-table] is not set.")
		return false
	}
	if *row == "" {
		fmt.Println("Parameter[-table] is not set.")
		return false
	}
	// 验证一下空值时，能否查询到所有的列簇信息
	// if *columns == "" {
	// }
	return true
}
