package graphite

import "time"
import bulkQuerygen "github.com/aryachanwu/BDC-TS/bulk_query_gen"

// InfluxDevopsSingleHost12hr produces Influx-specific queries for the devops single-host case over a 12hr period.
type GraphiteDevopsSingleHost12hr struct {
	GraphiteDevops
}

func NewGraphiteDevopsSingleHost12hr(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newGraphiteDevopsCommon(queriesFullRange, queryInterval, scaleVar).(*GraphiteDevops)
	return &GraphiteDevopsSingleHost12hr{
		GraphiteDevops: *underlying,
	}
}

func (d *GraphiteDevopsSingleHost12hr) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MaxCPUUsage12HoursByMinuteOneHost(q)
	return q
}
