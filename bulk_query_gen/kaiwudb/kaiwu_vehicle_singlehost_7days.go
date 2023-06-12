package kaiwudb

import "time"
import bulkQuerygen "github.com/aryachanwu/BDC-TS/bulk_query_gen"

// KaiwuVehicleSingleHostSevenDays produces Kaiwu-specific queries for the Vehicle single-host case.
type KaiwuVehicleSingleHostSevenDays struct {
	KaiwuVehicle
}

func NewKaiwuVehicleSingleHostSevenDays(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newKaiwuVehicleCommon(dbConfig, queriesFullRange, queryInterval, scaleVar).(*KaiwuVehicle)
	return &KaiwuVehicleSingleHostSevenDays{
		KaiwuVehicle: *underlying,
	}
}

func (d *KaiwuVehicleSingleHostSevenDays) Dispatch(i int) bulkQuerygen.Query {
	q := NewSQLQuery() // from pool
	d.AvergeValueSevenDaysOneHost(q)
	return q
}
