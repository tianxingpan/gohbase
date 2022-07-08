// Package gohbase provides a pool of hbase clients
package gohbase

import "github.com/tianxingpan/gohbase/hbase"

type HBase interface {
	// Test for the existence of columns in the table, as specified in the TGet.
	//
	// @return true if the specified TGet matches one or more keys, false if not
	//
	// Parameters:
	//  - Table: the table to check on
	//  - Tget: the TGet to check for
	Exists(table []byte, tget *hbase.TGet) (r bool, err error)
	// Method for getting data from a row.
	//
	// If the row cannot be found an empty Result is returned.
	// This can be checked by the empty field of the TResult
	//
	// @return the result
	//
	// Parameters:
	//  - Table: the table to get from
	//  - Tget: the TGet to fetch
	Get(table []byte, tget *hbase.TGet) (r *hbase.TResult_, err error)
	// Method for getting multiple rows.
	//
	// If a row cannot be found there will be a null
	// value in the result list for that TGet at the
	// same position.
	//
	// So the Results are in the same order as the TGets.
	//
	// Parameters:
	//  - Table: the table to get from
	//  - Tgets: a list of TGets to fetch, the Result list
	// will have the Results at corresponding positions
	// or null if there was an error
	GetMultiple(table []byte, tgets []*hbase.TGet) (r []*hbase.TResult_, err error)
	// Commit a TPut to a table.
	//
	// Parameters:
	//  - Table: the table to put data in
	//  - Tput: the TPut to put
	Put(table []byte, tput *hbase.TPut) (err error)
	// Atomically checks if a row/family/qualifier value matches the expected
	// value. If it does, it adds the TPut.
	//
	// @return true if the new put was executed, false otherwise
	//
	// Parameters:
	//  - Table: to check in and put to
	//  - Row: row to check
	//  - Family: column family to check
	//  - Qualifier: column qualifier to check
	//  - Value: the expected value, if not provided the
	// check is for the non-existence of the
	// column in question
	//  - Tput: the TPut to put if the check succeeds\
	CheckAndPut(table, row, family, qualifier, value []byte, tput *hbase.TPut) (r bool, err error)
	// Commit a List of Puts to the table.
	//
	// Parameters:
	//  - Table: the table to put data in
	//  - Tputs: a list of TPuts to commit
	PutMultiple(table []byte, tputs []*hbase.TPut) (err error)
	// Deletes as specified by the TDelete.
	//
	// Note: "delete" is a reserved keyword and cannot be used in Thrift
	// thus the inconsistent naming scheme from the other functions.
	//
	// Parameters:
	//  - Table: the table to delete from
	//  - Tdelete: the TDelete to delete
	DeleteSingle(table []byte, tdelete *hbase.TDelete) (err error)
	// Bulk commit a List of TDeletes to the table.
	//
	// Throws a TIOError if any of the deletes fail.
	//
	// Always returns an empty list for backwards compatibility.
	//
	// Parameters:
	//  - Table: the table to delete from
	//  - Tdeletes: list of TDeletes to delete
	DeleteMultiple(table []byte, tdeletes []*hbase.TDelete) (r []*hbase.TDelete, err error)
	// Atomically checks if a row/family/qualifier value matches the expected
	// value. If it does, it adds the delete.
	//
	// @return true if the new delete was executed, false otherwise
	//
	// Parameters:
	//  - Table: to check in and delete from
	//  - Row: row to check
	//  - Family: column family to check
	//  - Qualifier: column qualifier to check
	//  - Value: the expected value, if not provided the
	// check is for the non-existence of the
	// column in question
	//  - Tdelete: the TDelete to execute if the check succeeds
	CheckAndDelete(table, row, family, qualifier, value []byte, tdelete *hbase.TDelete) (r bool, err error)
	// Parameters:
	//  - Table: the table to increment the value on
	//  - Tincrement: the TIncrement to increment
	Increment(table []byte, tincrement *hbase.TIncrement) (r *hbase.TResult_, err error)
	// Parameters:
	//  - Table: the table to append the value on
	//  - Tappend: the TAppend to append
	Append(table []byte, tappend *hbase.TAppend) (r *hbase.TResult_, err error)
	// Get a Scanner for the provided TScan object.
	//
	// @return Scanner Id to be used with other scanner procedures
	//
	// Parameters:
	//  - Table: the table to get the Scanner for
	//  - Tscan: the scan object to get a Scanner for
	OpenScanner(table []byte, tscan *hbase.TScan) (r int32, err error)
	// Grabs multiple rows from a Scanner.
	//
	// @return Between zero and numRows TResults
	//
	// Parameters:
	//  - ScannerId: the Id of the Scanner to return rows from. This is an Id returned from the openScanner function.
	//  - NumRows: number of rows to return
	GetScannerRows(scannerId int32, numRows int32) (r []*hbase.TResult_, err error)
	// Closes the scanner. Should be called to free server side resources timely.
	// Typically close once the scanner is not needed anymore, i.e. after looping
	// over it to get all the required rows.
	//
	// Parameters:
	//  - ScannerId: the Id of the Scanner to close *
	CloseScanner(scannerId int32) (err error)
	// mutateRow performs multiple mutations atomically on a single row.
	//
	// Parameters:
	//  - Table: table to apply the mutations
	//  - TrowMutations: mutations to apply
	MutateRow(table []byte, trowMutations *hbase.TRowMutations) (err error)
	// Get results for the provided TScan object.
	// This helper function opens a scanner, get the results and close the scanner.
	//
	// @return between zero and numRows TResults
	//
	// Parameters:
	//  - Table: the table to get the Scanner for
	//  - Tscan: the scan object to get a Scanner for
	//  - NumRows: number of rows to return
	GetScannerResults(table []byte, tscan *hbase.TScan, numRows int32) (r []*hbase.TResult_, err error)
	// Given a table and a row get the location of the region that
	// would contain the given row key.
	//
	// reload = true means the cache will be cleared and the location
	// will be fetched from meta.
	//
	// Parameters:
	//  - Table
	//  - Row
	//  - Reload
	GetRegionLocation(table, row []byte, reload bool) (r *hbase.THRegionLocation, err error)
	// Get all of the region locations for a given table.
	//
	//
	// Parameters:
	//  - Table
	GetAllRegionLocations(table []byte) (r []*hbase.THRegionLocation, err error)
}

func NewHBase(opt *Options) HBase {
	return &hBaseCMD{
		opt:            opt,
		thriftConnPool: NewThriftConnPool(opt),
	}
}

type hBaseCMD struct {
	opt            *Options
	thriftConnPool *ThriftConnPool
}

// Append implements HBase
func (h *hBaseCMD) Append(table []byte, tappend *hbase.TAppend) (r *hbase.TResult_, err error) {
	var cn *ThriftConn
	cn, err = h.thriftConnPool.Get()
	if err != nil {
		return
	}
	defer h.thriftConnPool.Put(cn)
	hc := cn.GetHbaseClient()
	r, err = hc.Append(table, tappend)
	return
}

// CheckAndDelete implements HBase
func (h *hBaseCMD) CheckAndDelete(table []byte, row []byte, family []byte, qualifier []byte, value []byte, tdelete *hbase.TDelete) (r bool, err error) {
	var cn *ThriftConn
	cn, err = h.thriftConnPool.Get()
	if err != nil {
		return
	}
	defer h.thriftConnPool.Put(cn)
	hc := cn.GetHbaseClient()
	r, err = hc.CheckAndDelete(table, row, family, qualifier, value, tdelete)
	return
}

// CheckAndPut implements HBase
func (h *hBaseCMD) CheckAndPut(table []byte, row []byte, family []byte, qualifier []byte, value []byte, tput *hbase.TPut) (r bool, err error) {
	var cn *ThriftConn
	cn, err = h.thriftConnPool.Get()
	if err != nil {
		return
	}
	defer h.thriftConnPool.Put(cn)
	hc := cn.GetHbaseClient()
	r, err = hc.CheckAndPut(table, row, family, qualifier, value, tput)
	return
}

// CloseScanner implements HBase
func (h *hBaseCMD) CloseScanner(scannerId int32) (err error) {
	var cn *ThriftConn
	cn, err = h.thriftConnPool.Get()
	if err != nil {
		return
	}
	defer h.thriftConnPool.Put(cn)
	hc := cn.GetHbaseClient()
	err = hc.CloseScanner(scannerId)
	return
}

// DeleteMultiple implements HBase
func (h *hBaseCMD) DeleteMultiple(table []byte, tdeletes []*hbase.TDelete) (r []*hbase.TDelete, err error) {
	var cn *ThriftConn
	cn, err = h.thriftConnPool.Get()
	if err != nil {
		return
	}
	defer h.thriftConnPool.Put(cn)
	hc := cn.GetHbaseClient()
	r, err = hc.DeleteMultiple(table, tdeletes)
	return
}

// DeleteSingle implements HBase
func (h *hBaseCMD) DeleteSingle(table []byte, tdelete *hbase.TDelete) (err error) {
	var cn *ThriftConn
	cn, err = h.thriftConnPool.Get()
	if err != nil {
		return
	}
	defer h.thriftConnPool.Put(cn)
	hc := cn.GetHbaseClient()
	err = hc.DeleteSingle(table, tdelete)
	return
}

// Exists implements HBase
func (h *hBaseCMD) Exists(table []byte, tget *hbase.TGet) (r bool, err error) {
	var cn *ThriftConn
	cn, err = h.thriftConnPool.Get()
	if err != nil {
		return
	}
	defer h.thriftConnPool.Put(cn)
	hc := cn.GetHbaseClient()
	r, err = hc.Exists(table, tget)
	return
}

// Get implements HBase
func (h *hBaseCMD) Get(table []byte, tget *hbase.TGet) (r *hbase.TResult_, err error) {
	var cn *ThriftConn
	cn, err = h.thriftConnPool.Get()
	if err != nil {
		return
	}
	defer h.thriftConnPool.Put(cn)
	hc := cn.GetHbaseClient()
	r, err = hc.Get(table, tget)
	return
}

// GetAllRegionLocations implements HBase
func (h *hBaseCMD) GetAllRegionLocations(table []byte) (r []*hbase.THRegionLocation, err error) {
	var cn *ThriftConn
	cn, err = h.thriftConnPool.Get()
	if err != nil {
		return
	}
	defer h.thriftConnPool.Put(cn)
	hc := cn.GetHbaseClient()
	r, err = hc.GetAllRegionLocations(table)
	return
}

// GetMultiple implements HBase
func (h *hBaseCMD) GetMultiple(table []byte, tgets []*hbase.TGet) (r []*hbase.TResult_, err error) {
	var cn *ThriftConn
	cn, err = h.thriftConnPool.Get()
	if err != nil {
		return
	}
	defer h.thriftConnPool.Put(cn)
	hc := cn.GetHbaseClient()
	r, err = hc.GetMultiple(table, tgets)
	return
}

// GetRegionLocation implements HBase
func (h *hBaseCMD) GetRegionLocation(table []byte, row []byte, reload bool) (r *hbase.THRegionLocation, err error) {
	var cn *ThriftConn
	cn, err = h.thriftConnPool.Get()
	if err != nil {
		return
	}
	defer h.thriftConnPool.Put(cn)
	hc := cn.GetHbaseClient()
	r, err = hc.GetRegionLocation(table, row, reload)
	return
}

// GetScannerResults implements HBase
func (h *hBaseCMD) GetScannerResults(table []byte, tscan *hbase.TScan, numRows int32) (r []*hbase.TResult_, err error) {
	var cn *ThriftConn
	cn, err = h.thriftConnPool.Get()
	if err != nil {
		return
	}
	defer h.thriftConnPool.Put(cn)
	hc := cn.GetHbaseClient()
	r, err = hc.GetScannerResults(table, tscan, numRows)
	return
}

// GetScannerRows implements HBase
func (h *hBaseCMD) GetScannerRows(scannerId int32, numRows int32) (r []*hbase.TResult_, err error) {
	var cn *ThriftConn
	cn, err = h.thriftConnPool.Get()
	if err != nil {
		return
	}
	defer h.thriftConnPool.Put(cn)
	hc := cn.GetHbaseClient()
	r, err = hc.GetScannerRows(scannerId, numRows)
	return
}

// Increment implements HBase
func (h *hBaseCMD) Increment(table []byte, tincrement *hbase.TIncrement) (r *hbase.TResult_, err error) {
	var cn *ThriftConn
	cn, err = h.thriftConnPool.Get()
	if err != nil {
		return
	}
	defer h.thriftConnPool.Put(cn)
	hc := cn.GetHbaseClient()
	r, err = hc.Increment(table, tincrement)
	return
}

// MutateRow implements HBase
func (h *hBaseCMD) MutateRow(table []byte, trowMutations *hbase.TRowMutations) (err error) {
	var cn *ThriftConn
	cn, err = h.thriftConnPool.Get()
	if err != nil {
		return
	}
	defer h.thriftConnPool.Put(cn)
	hc := cn.GetHbaseClient()
	err = hc.MutateRow(table, trowMutations)
	return
}

// OpenScanner implements HBase
func (h *hBaseCMD) OpenScanner(table []byte, tscan *hbase.TScan) (r int32, err error) {
	var cn *ThriftConn
	cn, err = h.thriftConnPool.Get()
	if err != nil {
		return
	}
	defer h.thriftConnPool.Put(cn)
	hc := cn.GetHbaseClient()
	r, err = hc.OpenScanner(table, tscan)
	return
}

// Put implements HBase
func (h *hBaseCMD) Put(table []byte, tput *hbase.TPut) (err error) {
	var cn *ThriftConn
	cn, err = h.thriftConnPool.Get()
	if err != nil {
		return
	}
	defer h.thriftConnPool.Put(cn)
	hc := cn.GetHbaseClient()
	err = hc.Put(table, tput)
	return
}

// PutMultiple implements HBase
func (h *hBaseCMD) PutMultiple(table []byte, tputs []*hbase.TPut) (err error) {
	var cn *ThriftConn
	cn, err = h.thriftConnPool.Get()
	if err != nil {
		return
	}
	defer h.thriftConnPool.Put(cn)
	hc := cn.GetHbaseClient()
	err = hc.PutMultiple(table, tputs)
	return
}
