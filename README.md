# gohbase
Code implementation of HBase client go language

## Install and Usage
Install the package with:

```bash
go get github.com/tianxingpan/gohbase
```

## Example

```go
import (
	"github.com/tianxingpan/gohbase"
	"github.com/tianxingpan/gohbase/hbase"
)
// create a hbase to be used with thrift pool
hb := gohbase.NewHBase(&gohbase.Options{
	Addr: *addr,
})
cm := hbase.TGet{
	Row:     []byte("rowkey"),
	Columns: []*hbase.TColumn{},
}

// close the underlying connection instead of returning it to pool
// it is useful when acceptor has already closed connection and conn.Write() returns error
r, err := hb.Get([]byte("hbase:table"), &cm)
if err != nil {
    panic(err.Error())
}
if r != nil {
	for _, cv := range r.ColumnValues {
		fmt.Printf("%s\tcolumn=%s:%s, timestamp=%d, value=%s\n", string(r.Row), string(cv.Family), string(cv.Qualifier), *cv.Timestamp, string(cv.Value))
	}
}

// close pool any time you want, this closes all the connections inside a pool
_ = hb.Close()

```


## Credits

 * [Patrick](https://github.com/tianxingpan)

## License

The Apache-2.0 license - see LICENSE for more details