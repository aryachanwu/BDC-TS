package bulk_query_gen

// Devops describes a devops query generator.
type Vehicle interface {
	AvergeValueOneDayOneHost(Query)
	AvergeValueSevenDaysOneHost(Query)
	AvergeValueOneMonthOneHost(Query)
	LastValueOneHost(Query)

	//CountCPUUsageDayByHourAllHostsGroupbyHost(Query)

	Dispatch(int) Query
}

// VehicleDispatchAll round-robins through the different devops queries.
func VehicleDispatchAll(d Vehicle, iteration int, q Query, scaleVar int) {
	d.AvergeValueOneDayOneHost(q)
}
