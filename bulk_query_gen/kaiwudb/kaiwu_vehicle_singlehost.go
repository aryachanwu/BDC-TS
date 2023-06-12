package kaiwudb

import "time"
import bulkQuerygen "github.com/aryachanwu/BDC-TS/bulk_query_gen"

// KaiwuVehicleSingleHost produces Timescale-specific queries for the Vehicle single-host case.
type KaiwuVehicleSingleHost struct {
	TimescaleVehicle
}

func NewKaiwuVehicleSingleHost(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newTimescaleVehicleCommon(dbConfig, queriesFullRange, queryInterval, scaleVar).(*TimescaleVehicle)
	return &KaiwuVehicleSingleHost{
		TimescaleVehicle: *underlying,
	}
}

func (d *KaiwuVehicleSingleHost) Dispatch(i int) bulkQuerygen.Query {
	q := NewSQLQuery() // from pool
	d.AvergeValueOneDayOneHost(q)
	return q
}
