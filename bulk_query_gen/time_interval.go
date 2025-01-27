package bulk_query_gen

import (
	"math/rand"
	"time"
)

// TimeInterval represents an interval of time.
type TimeInterval struct {
	Start, End time.Time
}

// NewTimeInterval constructs a TimeInterval.
func NewTimeInterval(start, end time.Time) TimeInterval {
	return TimeInterval{
		Start: start,
		End:   end,
	}
}

// Duration converts a TimeInterval to a time.Duration.
func (ti *TimeInterval) Duration() time.Duration {
	return ti.End.Sub(ti.Start)
}

// RandWindow creates a TimeInterval of duration `window` at a uniformly-random
// start time within this time interval.
func (ti *TimeInterval) RandWindow(window time.Duration) TimeInterval {
	lower := ti.Start.UnixNano()
	upper := ti.End.Add(-window).UnixNano()

	if upper <= lower {
		intermediate := lower + 1
		lower = upper
		upper = intermediate
	}

	start := lower + rand.Int63n(upper-lower)
	end := start + window.Nanoseconds()

	x := NewTimeInterval(time.Unix(0, start), time.Unix(0, end))
	if x.Duration() != window {
		panic("logic error: generated interval does not equal window")
	}

	return x
}

// StartString formats the start of the time interval.
func (ti *TimeInterval) StartString() string {
	return ti.Start.UTC().Format(time.RFC3339)
}

// EndString formats the end of the time interval.
func (ti *TimeInterval) EndString() string {
	return ti.End.UTC().Format(time.RFC3339)
}

// StartUnixNano returns the start time as nanoseconds.
func (ti *TimeInterval) StartUnixNano() int64 {
	return ti.Start.UTC().UnixNano()
}

// EndUnixNano returns the end time as nanoseconds.
func (ti *TimeInterval) EndUnixNano() int64 {
	return ti.End.UTC().UnixNano()
}

// StartUnixNano returns the start time as msec.
func (ti *TimeInterval) StartUnix() int64 {
	return ti.Start.UTC().Unix()
}

// EndUnixNano returns the end time as msec.
func (ti *TimeInterval) EndUnix() int64 {
	return ti.End.UTC().Unix()
}
