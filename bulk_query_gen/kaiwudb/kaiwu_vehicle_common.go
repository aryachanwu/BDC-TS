package kaiwudb

import (
	"fmt"
	bulkQuerygen "github.com/aryachanwu/BDC-TS/bulk_query_gen"
	"math/rand"
	"strconv"
	"time"
)

// KaiwuVehicle produces Kaiwu-specific queries for all the Vehicle query types.
type KaiwuVehicle struct {
	bulkQuerygen.CommonParams
	DatabaseName string
}

// newKaiwuVehicleCommon makes an KaiwuVehicle object ready to generate Queries.
func newKaiwuVehicleCommon(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	if _, ok := dbConfig[bulkQuerygen.DatabaseName]; !ok {
		panic("need Kaiwu database name")
	}

	return &KaiwuVehicle{
		CommonParams: *bulkQuerygen.NewCommonParams(interval, scaleVar),
		DatabaseName: dbConfig[bulkQuerygen.DatabaseName],
	}
}

// Dispatch fulfills the QueryGenerator interface.
func (d *KaiwuVehicle) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	bulkQuerygen.VehicleDispatchAll(d, i, q, d.ScaleVar)
	return q
}

func (d *KaiwuVehicle) AvergeValueOneDayOneHost(q bulkQuerygen.Query) {
	d.AvergeValueNHosts(q, 1, time.Hour*24)
}

func (d *KaiwuVehicle) AvergeValueSevenDaysOneHost(q bulkQuerygen.Query) {
	d.AvergeValueNHosts(q, 1, time.Hour*24*7)
}

func (d *KaiwuVehicle) AvergeValueOneMonthOneHost(q bulkQuerygen.Query) {
	d.AvergeValueNHosts(q, 1, time.Hour*24*30)
}

func (d *KaiwuVehicle) LastValueOneHost(q bulkQuerygen.Query) {
	d.LastValueNHosts(q, 1)
}

// AvergeValueNHosts
// select avg(value5) from t.p.v where time_stamp >= start_time and time_stamp <= end_time
func (d *KaiwuVehicle) AvergeValueNHosts(qi bulkQuerygen.Query, nhosts int, timeRange time.Duration) {
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

	humanLabel := fmt.Sprintf("Kaiwu averge value, rand %4d hosts", nhosts)

	q := qi.(*SQLQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	valueId := rand.Intn(59) + 1
	valueName := "value" + strconv.Itoa(valueId)
	q.QuerySQL = []byte(fmt.Sprintf("select avg(%s) from vehicle where time >=%d and time < %d", valueName, interval.StartUnixNano(), interval.EndUnixNano()))
}

// LastValueNHosts
// select time, VIN, value4 from (select time, VIN, value4, rank() over(order by time desc range between unbounded preceding and unbounded following) as rank from vehicle) t where rank=1
func (d *KaiwuVehicle) LastValueNHosts(qi bulkQuerygen.Query, nhosts int) {
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

	humanLabel := fmt.Sprintf("Kaiwu last value, rand %4d hosts", nhosts)

	q := qi.(*SQLQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel))
	q.QuerySQL = []byte(fmt.Sprintf("select time, VIN, value4 from (select time, VIN, value4, rank() over(order by time desc range between unbounded preceding and unbounded following) as rank from vehicle) t where rank=1"))
}
