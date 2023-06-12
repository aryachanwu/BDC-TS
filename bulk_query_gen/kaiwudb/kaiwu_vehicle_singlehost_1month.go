package kaiwudb

import "time"
import bulkQuerygen "github.com/aryachanwu/BDC-TS/bulk_query_gen"

// TimescaleVehicleSingleHostOneMonth produces Timescale-specific queries for the Vehicle single-host case.
type TimescaleVehicleSingleHostOneMonth struct {
	TimescaleVehicle
}

func NewTimescaleVehicleSingleHostOneMonth(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newTimescaleVehicleCommon(dbConfig, queriesFullRange, queryInterval, scaleVar).(*TimescaleVehicle)
	return &TimescaleVehicleSingleHostOneMonth{
		TimescaleVehicle: *underlying,
	}
}

func (d *TimescaleVehicleSingleHostOneMonth) Dispatch(i int) bulkQuerygen.Query {
	q := NewSQLQuery() // from pool
	d.AvergeValueOneMonthOneHost(q)
	return q
}
