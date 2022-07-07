// Package gohbase provides a pool of hbase clients

package gohbase

type HColumnValue struct {
	Family    string `json:"family"`
	Qualifier string `json:"qualifier"`
	Value     string `json:"value"`
	Timestamp int64  `json:"timestamp"`
	Tags      string `json:"tags"`
}

type HResult struct {
	Row          string          `json:"row"`
	ColumnValues []*HColumnValue `json:"columnValues"`
}

type HRegionInfo struct {
	RegionId  int64  `json:"regionId"`
	TableName string `json:"tableName"`
	StartKey  string `json:"startKey"`
	EndKey    string `json:"endKey"`
	Offline   bool   `json:"offline"`
	Split     bool   `json:"split"`
	ReplicaId int32  `json:"replicaId"`
}

type HServerName struct {
	HostName  string `json:"hostName"`
	Port      int32  `json:"port"`
	StartCode int64  `json:"startCode"`
}

type HRegionLocation struct {
	ServerName *HServerName `json:"serverName"`
	RegionInfo *HRegionInfo `json:"regionInfo"`
}
