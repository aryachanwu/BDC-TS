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
	switch iteration {
	case 0:
		d.AvergeValueOneDayOneHost(q)
	case 1:
		d.AvergeValueSevenDaysOneHost(q)
	case 2:
		d.AvergeValueOneMonthOneHost(q)
	case 3:
		d.LastValueOneHost(q)
	default:
		panic("logic error in switch statement")
	}
}
