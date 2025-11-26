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
