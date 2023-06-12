package timescaledb

import "time"
import bulkQuerygen "github.com/aryachanwu/BDC-TS/bulk_query_gen"

// TimescaleVehicleSingleHost produces Timescale-specific queries for the Vehicle single-host case.
type TimescaleVehicleSingleHost struct {
	TimescaleVehicle
}

func NewTimescaleVehicleSingleHost(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newTimescaleVehicleCommon(dbConfig, queriesFullRange, queryInterval, scaleVar).(*TimescaleVehicle)
	return &TimescaleVehicleSingleHost{
		TimescaleVehicle: *underlying,
	}
}

func (d *TimescaleVehicleSingleHost) Dispatch(i int) bulkQuerygen.Query {
	q := NewSQLQuery() // from pool
	d.AvergeValueOneDayOneHost(q)
	return q
}
