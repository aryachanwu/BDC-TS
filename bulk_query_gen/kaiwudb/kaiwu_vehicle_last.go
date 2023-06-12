package kaiwudb

import "time"
import bulkQuerygen "github.com/aryachanwu/BDC-TS/bulk_query_gen"

// KaiwuVehicleLast produces Kaiwu-specific queries for the Vehicle single-host case.
type KaiwuVehicleLast struct {
	KaiwuVehicle
}

func NewKaiwuVehicleLast(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newKaiwuVehicleCommon(dbConfig, queriesFullRange, queryInterval, scaleVar).(*KaiwuVehicle)
	return &KaiwuVehicleLast{
		KaiwuVehicle: *underlying,
	}
}

func (d *KaiwuVehicleLast) Dispatch(i int) bulkQuerygen.Query {
	q := NewSQLQuery() // from pool
	d.LastValueOneHost(q)
	return q
}
