package csvdb

const (
	cRModePlain           = "plain"
	cRModeGZip            = "gzip"
	cMaxPartitionDitigs   = 10
	cTblIniExt            = "tbl.ini"
	cDefaultPartitionID   = "default"
	cDefaultPartitionSize = 10000
	cDefaultMaxPartitions = 100
	cWriteModeAppend      = "a"
	cWriteModeWrite       = "w"
	cTableTypeCsv         = "csv"
	cTableTypeRotating    = "rot"
)
