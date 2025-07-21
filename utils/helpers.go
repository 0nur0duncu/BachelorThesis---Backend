package utils

import (
	"math"
)

// CalculateProgress calculates job progress percentage
func CalculateProgress(completedTasks, totalTasks int) float64 {
	if totalTasks <= 0 {
		return 0.0
	}
	progress := (float64(completedTasks) / float64(totalTasks)) * 100.0
	return math.Min(progress, 100.0)
}

// Contains checks if a slice contains an integer
func Contains(slice []int, item int) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
