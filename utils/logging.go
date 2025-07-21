package utils

import (
	"fmt"
	"log"
	"os"
	"time"
)

// SetupLogging initializes the logging to file with day-month-year format
func SetupLogging() *os.File {
	// Create logs directory if it doesn't exist
	logDir := "./logs"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Fatalf("Failed to create log directory: %v", err)
	}

	// Create log file with current date
	currentTime := time.Now()
	logFileName := fmt.Sprintf("%s/%02d-%02d-%d.log",
		logDir,
		currentTime.Day(),
		currentTime.Month(),
		currentTime.Year())

	file, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}

	// Set standard logger to write only to file, not to console
	log.SetOutput(file)

	// Set log flags to include date and time
	log.SetFlags(log.Ldate | log.Ltime)

	log.Printf("Logging to file: %s", logFileName)

	// Start a goroutine to check and rotate logs daily
	go monitorLogRotation(logDir, file)
	
	return file
}

// monitorLogRotation checks if the log file needs to be rotated and does it when needed
func monitorLogRotation(logDir string, logFile *os.File) {
	for {
		// Sleep until midnight
		now := time.Now()
		next := now.Add(24 * time.Hour)
		next = time.Date(next.Year(), next.Month(), next.Day(), 0, 0, 0, 0, next.Location())
		duration := next.Sub(now)

		time.Sleep(duration)

		// Close current log file
		if logFile != nil {
			logFile.Close()
		}

		// Create new log file with new date
		currentTime := time.Now()
		logFileName := fmt.Sprintf("%s/%02d-%02d-%d.log",
			logDir,
			currentTime.Day(),
			currentTime.Month(),
			currentTime.Year())

		file, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			// Since we can't log to the file, temporarily log to console
			stdLog := log.New(os.Stdout, "", log.Ldate|log.Ltime)
			stdLog.Printf("Failed to rotate log file: %v", err)
			continue
		}

		logFile = file

		// Update log output to the new file
		log.SetOutput(file)

		log.Printf("Rotated log file to: %s", logFileName)
	}
}
