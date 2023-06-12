package kaiwudb

import "time"
import bulkQuerygen "github.com/aryachanwu/BDC-TS/bulk_query_gen"

// TimescaleVehicleLast produces Timescale-specific queries for the Vehicle single-host case.
type TimescaleVehicleLast struct {
	TimescaleVehicle
}

func NewTimescaleVehicleLast(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newTimescaleVehicleCommon(dbConfig, queriesFullRange, queryInterval, scaleVar).(*TimescaleVehicle)
	return &TimescaleVehicleLast{
		TimescaleVehicle: *underlying,
	}
}

func (d *TimescaleVehicleLast) Dispatch(i int) bulkQuerygen.Query {
	q := NewSQLQuery() // from pool
	d.LastValueOneHost(q)
	return q
}
