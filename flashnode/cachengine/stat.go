package cachengine

import "sync/atomic"

var (
	StatMap    = make(map[string]*MetricStat)
	VolStatMap = make(map[string]*MetricStat)
)

type MetricStat struct {
	ReadBytes  uint64
	ReadCount  uint64
	WriteBytes uint64
	WriteCount uint64
}

func updateWriteBytesMetric(size uint64, d string) {
	if stat, ok := StatMap[d]; ok {
		atomic.AddUint64(&stat.WriteBytes, size)
	}
}

func updateWriteCountMetric(d string) {
	if stat, ok := StatMap[d]; ok {
		atomic.AddUint64(&stat.WriteCount, 1)
	}
}

func updateVolWriteBytesMetric(size uint64, d string) {
	if stat, ok := VolStatMap[d]; ok {
		atomic.AddUint64(&stat.WriteBytes, size)
	}
}

func updateVolWriteCountMetric(d string) {
	if stat, ok := VolStatMap[d]; ok {
		atomic.AddUint64(&stat.WriteCount, 1)
	}
}
