package kvraft

func percentile(sorted []int64, p int) float64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(float64(len(sorted)) * float64(p) / 100.0)
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return float64(sorted[index])
}

// Helper function to calculate average latency in milliseconds
func avgLatencyMs(totalMicros int64, count int64) float64 {
	if count == 0 {
		return 0
	}
	return float64(totalMicros) / float64(count) / 1000.0
}
