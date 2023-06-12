package timescaledb

import "time"
import bulkQuerygen "github.com/aryachanwu/BDC-TS/bulk_query_gen"

// TimescaleVehicleSingleHostSevenDays produces Timescale-specific queries for the Vehicle single-host case.
type TimescaleVehicleSingleHostSevenDays struct {
	TimescaleVehicle
}

func NewTimescaleVehicleSingleHostSevenDays(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newTimescaleVehicleCommon(dbConfig, queriesFullRange, queryInterval, scaleVar).(*TimescaleVehicle)
	return &TimescaleVehicleSingleHostSevenDays{
		TimescaleVehicle: *underlying,
	}
}

func (d *TimescaleVehicleSingleHostSevenDays) Dispatch(i int) bulkQuerygen.Query {
	q := NewSQLQuery() // from pool
	d.AvergeValueSevenDaysOneHost(q)
	return q
}
