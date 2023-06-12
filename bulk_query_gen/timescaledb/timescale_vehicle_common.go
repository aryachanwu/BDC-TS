package timescaledb

import (
	"fmt"
	bulkQuerygen "github.com/aryachanwu/BDC-TS/bulk_query_gen"
	"math/rand"
	"strconv"
	"time"
)

// TimescaleVehicle produces Timescale-specific queries for all the Vehicle query types.
type TimescaleVehicle struct {
	bulkQuerygen.CommonParams
	DatabaseName string
}

// newTimescaleVehicleCommon makes an TimescaleVehicle object ready to generate Queries.
func newTimescaleVehicleCommon(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	if _, ok := dbConfig[bulkQuerygen.DatabaseName]; !ok {
		panic("need timescale database name")
	}

	return &TimescaleVehicle{
		CommonParams: *bulkQuerygen.NewCommonParams(interval, scaleVar),
		DatabaseName: dbConfig[bulkQuerygen.DatabaseName],
	}
}

// Dispatch fulfills the QueryGenerator interface.
func (d *TimescaleVehicle) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	bulkQuerygen.VehicleDispatchAll(d, i, q, d.ScaleVar)
	return q
}

func (d *TimescaleVehicle) AvergeValueOneDayOneHost(q bulkQuerygen.Query) {
	d.AvergeValueNHosts(q, 1, time.Hour*24)
}

func (d *TimescaleVehicle) AvergeValueSevenDaysOneHost(q bulkQuerygen.Query) {
	d.AvergeValueNHosts(q, 1, time.Hour*24*7)
}

func (d *TimescaleVehicle) AvergeValueOneMonthOneHost(q bulkQuerygen.Query) {
	d.AvergeValueNHosts(q, 1, time.Hour*24*7*30)
}

func (d *TimescaleVehicle) LastValueOneHost(q bulkQuerygen.Query) {
	d.LastValueNHosts(q, 1)
}

// AvergeValueNHosts
// select avg(value5) from t.p.v where time_stamp >= start_time and time_stamp <= end_time
func (d *TimescaleVehicle) AvergeValueNHosts(qi bulkQuerygen.Query, nhosts int, timeRange time.Duration) {
	interval := d.AllInterval.RandWindow(timeRange)
	nn := rand.Perm(d.ScaleVar)[:nhosts]

	hostnames := []string{}
	for _, n := range nn {
		hostnames = append(hostnames, fmt.Sprintf("host_%d", n))
	}

	hostnameClauses := []string{}
	for _, s := range hostnames {
		hostnameClauses = append(hostnameClauses, fmt.Sprintf("hostname = '%s'", s))
	}

	//combinedHostnameClause := strings.Join(hostnameClauses, " or ")

	humanLabel := fmt.Sprintf("Timescale max cpu, rand %4d hosts, rand %s by 1m", nhosts, timeRange)

	q := qi.(*SQLQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	valueId := rand.Intn(59) + 1
	valueName := "value" + strconv.Itoa(valueId)
	q.QuerySQL = []byte(fmt.Sprintf("select avg(%s) from vehicle and time >=%d and time < %d", valueName, interval.StartUnixNano(), interval.EndUnixNano()))
}

// LastValueNHosts
// select timestamp, VIN, value4 from (select timestamp, VIN, value4, rank() over(order by timestamp desc range between unbounded preceding and unbounded following) as rank from accounts) t where rank=1
func (d *TimescaleVehicle) LastValueNHosts(qi bulkQuerygen.Query, nhosts int) {
	//interval := d.AllInterval.RandWindow(timeRange)
	nn := rand.Perm(d.ScaleVar)[:nhosts]

	hostnames := []string{}
	for _, n := range nn {
		hostnames = append(hostnames, fmt.Sprintf("host_%d", n))
	}

	hostnameClauses := []string{}
	for _, s := range hostnames {
		hostnameClauses = append(hostnameClauses, fmt.Sprintf("hostname = '%s'", s))
	}

	//combinedHostnameClause := strings.Join(hostnameClauses, " or ")

	humanLabel := fmt.Sprintf("Timescale max cpu, rand %4d hosts", nhosts)

	q := qi.(*SQLQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel))
	q.QuerySQL = []byte(fmt.Sprintf("select timestamp, VIN, value4 from (select timestamp, VIN, value4, rank() over(order by timestamp desc range between unbounded preceding and unbounded following) as rank from accounts) t where rank=1"))
}
