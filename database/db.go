package database

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/BachelorThesis/Backend/config"

	_ "github.com/lib/pq"
)

// InitDB initializes the database connection
func InitDB() *sql.DB {
	// Database connection setup
	dbUser, dbPass, dbHost, dbName := config.GetDBConfig()

	// Display database connection info (excluding password for security)
	log.Printf("Connecting to database: PostgreSQL on %s, database: %s, user: %s",
		dbHost, dbName, dbUser)

	// First, connect to the default postgres database to check if our database exists
	defaultDbURL := fmt.Sprintf("postgres://%s:%s@%s/postgres?sslmode=disable", dbUser, dbPass, dbHost)
	defaultDb, err := sql.Open("postgres", defaultDbURL)
	if err != nil {
		log.Fatalf("Failed to connect to default database: %v", err)
	}

	err = defaultDb.Ping()
	if err != nil {
		log.Fatalf("Cannot connect to PostgreSQL server: %v", err)
	}
	log.Printf("Successfully connected to PostgreSQL server at %s", dbHost)

	// Check if our database exists
	var exists bool
	err = defaultDb.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)", dbName).Scan(&exists)
	if err != nil {
		log.Fatalf("Error checking if database exists: %v", err)
	}

	// Create the database if it doesn't exist
	if !exists {
		log.Printf("Database '%s' does not exist, creating...", dbName)
		_, err = defaultDb.Exec(fmt.Sprintf("CREATE DATABASE %s", dbName))
		if err != nil {
			log.Fatalf("Failed to create database: %v", err)
		}
		log.Printf("Database '%s' created successfully", dbName)
	} else {
		log.Printf("Using existing database: %s", dbName)
	}

	// Close the connection to the default database
	defaultDb.Close()

	// Now connect to our actual database
	dbURL := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", dbUser, dbPass, dbHost, dbName)
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatalf("Database ping failed: %v", err)
	}

	log.Printf("Successfully connected to database: %s", dbName)

	// Make sure tables exist
	createTablesIfNotExist(db)

	// Log database data directory
	var dataDir string
	err = db.QueryRow("SHOW data_directory").Scan(&dataDir)
	if err != nil {
		log.Printf("Could not determine PostgreSQL data directory: %v", err)
	} else {
		log.Printf("PostgreSQL data directory: %s", dataDir)
	}

	// Show current database size
	var dbSize string
	err = db.QueryRow("SELECT pg_size_pretty(pg_database_size($1))", dbName).Scan(&dbSize)
	if err != nil {
		log.Printf("Could not determine database size: %v", err)
	} else {
		log.Printf("Database '%s' size: %s", dbName, dbSize)
	}

	return db
}

// Create tables if not exist
func createTablesIfNotExist(db *sql.DB) {
	// Create jobs table if not exists
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS jobs (
			job_id VARCHAR(36) PRIMARY KEY,
			filename VARCHAR(255),
			status VARCHAR(50) DEFAULT 'pending',
			total_tasks INTEGER DEFAULT 0,
			completed_tasks INTEGER DEFAULT 0,
			excluded_pages INTEGER[] DEFAULT '{}',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		log.Fatalf("Failed to create jobs table: %v", err)
	}
	log.Println("Jobs table created or already exists")

	// Create worker_results table if not exists
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS worker_results (
			id SERIAL PRIMARY KEY,
			job_id VARCHAR(36) REFERENCES jobs(job_id),
			page_number INTEGER,
			processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			path VARCHAR(255),
			text TEXT
		)
	`)
	if err != nil {
		log.Fatalf("Failed to create worker_results table: %v", err)
	}
	log.Println("Worker_results table created or already exists")

	// Create settings table if not exists
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS settings (
			id SERIAL PRIMARY KEY,
			google_cloud_vision_api_key TEXT DEFAULT '',
			ocr_worker_count INTEGER DEFAULT 2,
			dataset_worker_count INTEGER DEFAULT 2,
			model_api_key TEXT DEFAULT '',
			model_name TEXT DEFAULT '',
			items_per_page INTEGER DEFAULT 10,
			test_model_api_endpoint TEXT DEFAULT '',
			cohere_api_key TEXT DEFAULT '',
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		log.Fatalf("Failed to create settings table: %v", err)
	}
	log.Println("Settings table created or already exists")

	// Insert default settings if not exists
	_, err = db.Exec(`
		INSERT INTO settings (id, google_cloud_vision_api_key, ocr_worker_count, dataset_worker_count, model_api_key, model_name, items_per_page, test_model_api_endpoint, cohere_api_key)
		SELECT 1, '', 2, 2, '', '', 10, '', ''
		WHERE NOT EXISTS (SELECT 1 FROM settings WHERE id = 1)
	`)
	if err != nil {
		log.Fatalf("Failed to insert default settings: %v", err)
	}
	log.Println("Default settings inserted if not exists")

	// Check if path column exists in worker_results
	var columnExists bool
	err = db.QueryRow(`
		SELECT EXISTS (
			SELECT 1 
			FROM information_schema.columns 
			WHERE table_name = 'worker_results' AND column_name = 'path'
		)
	`).Scan(&columnExists)

	if err != nil {
		log.Fatalf("Failed to check for path column: %v", err)
	}

	// Add path column if it doesn't exist
	if !columnExists {
		_, err := db.Exec("ALTER TABLE worker_results ADD COLUMN path VARCHAR(255)")
		if err != nil {
			log.Fatalf("Failed to add path column: %v", err)
		}
		log.Println("Added path column to worker_results table")
	}

	// Create necessary indexes
	createIndexIfNotExists(db, "idx_jobs_status", "jobs", "status")
	createIndexIfNotExists(db, "idx_worker_results_job_id", "worker_results", "job_id")
	createIndexIfNotExists(db, "idx_worker_results_page", "worker_results", "page_number")

	// Log table schemas for verification
	log.Println("Database tables structure:")
	tables := []string{"jobs", "worker_results"}
	for _, table := range tables {
		rows, err := db.Query(fmt.Sprintf("SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = '%s'", table))
		if err != nil {
			log.Printf("Error querying schema for table %s: %v", table, err)
			continue
		}
		defer rows.Close()

		log.Printf("Table: %s", table)
		for rows.Next() {
			var colName, dataType string
			var maxLength sql.NullInt64
			if err := rows.Scan(&colName, &dataType, &maxLength); err != nil {
				log.Printf("Error scanning column metadata: %v", err)
				continue
			}
			if maxLength.Valid {
				log.Printf("  - %s: %s(%d)", colName, dataType, maxLength.Int64)
			} else {
				log.Printf("  - %s: %s", colName, dataType)
			}
		}
	}
}

// Create an index if it doesn't already exist
func createIndexIfNotExists(db *sql.DB, indexName string, tableName string, columnName string) {
	var indexExists bool
	err := db.QueryRow(`
		SELECT EXISTS (
			SELECT 1 
			FROM pg_indexes 
			WHERE indexname = $1
		)
	`, indexName).Scan(&indexExists)

	if err != nil {
		log.Printf("Failed to check if index %s exists: %v", indexName, err)
		return
	}

	if !indexExists {
		_, err := db.Exec(fmt.Sprintf("CREATE INDEX %s ON %s(%s)", indexName, tableName, columnName))
		if err != nil {
			log.Printf("Failed to create index %s: %v", indexName, err)
		} else {
			log.Printf("Created index %s on %s(%s)", indexName, tableName, columnName)
		}
	}
}
