package kaiwudb

import "time"
import bulkQuerygen "github.com/aryachanwu/BDC-TS/bulk_query_gen"

// KaiwuVehicleSingleHost produces Kaiwu-specific queries for the Vehicle single-host case.
type KaiwuVehicleSingleHost struct {
	KaiwuVehicle
}

func NewKaiwuVehicleSingleHost(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newKaiwuVehicleCommon(dbConfig, queriesFullRange, queryInterval, scaleVar).(*KaiwuVehicle)
	return &KaiwuVehicleSingleHost{
		KaiwuVehicle: *underlying,
	}
}

func (d *KaiwuVehicleSingleHost) Dispatch(i int) bulkQuerygen.Query {
	q := NewSQLQuery() // from pool
	d.AvergeValueOneDayOneHost(q)
	return q
}
