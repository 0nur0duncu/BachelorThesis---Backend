package config

import (
	"fmt"
	"os"
)

// GetEnv retrieves environment variables with defaults
func GetEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

// GetDBConfig returns the database configuration
func GetDBConfig() (string, string, string, string) {
	dbUser := GetEnv("DB_USER", "guest")
	dbPass := GetEnv("DB_PASS", "guest")
	dbHost := GetEnv("DB_HOST", "localhost")
	dbName := GetEnv("DB_NAME", "lisanstezi")

	return dbUser, dbPass, dbHost, dbName
}

// GetRabbitMQURL returns the RabbitMQ connection URL
func GetRabbitMQURL() string {
	rabbitmqHost := GetEnv("RABBITMQ_HOST", "localhost")
	return fmt.Sprintf("amqp://guest:guest@%s/", rabbitmqHost)
}
