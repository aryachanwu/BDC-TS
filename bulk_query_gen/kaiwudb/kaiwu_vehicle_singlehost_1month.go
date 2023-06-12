package kaiwudb

import "time"
import bulkQuerygen "github.com/aryachanwu/BDC-TS/bulk_query_gen"

// KaiwuVehicleSingleHostOneMonth produces Kaiwu-specific queries for the Vehicle single-host case.
type KaiwuVehicleSingleHostOneMonth struct {
	KaiwuVehicle
}

func NewKaiwuVehicleSingleHostOneMonth(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newKaiwuVehicleCommon(dbConfig, queriesFullRange, queryInterval, scaleVar).(*KaiwuVehicle)
	return &KaiwuVehicleSingleHostOneMonth{
		KaiwuVehicle: *underlying,
	}
}

func (d *KaiwuVehicleSingleHostOneMonth) Dispatch(i int) bulkQuerygen.Query {
	q := NewSQLQuery() // from pool
	d.AvergeValueOneMonthOneHost(q)
	return q
}
