package main

import (
	"database/sql" // Add this for string formatting
	"io"
	"log"
	"os" // Add this for string manipulation
	"sync"
	"time"

	"github.com/BachelorThesis/Backend/database"
	"github.com/BachelorThesis/Backend/handlers"
	"github.com/BachelorThesis/Backend/utils"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	// Make sure this RabbitMQ library is imported
)

var (
	db              *sql.DB
	purgeInProgress bool
	purgeMutex      sync.Mutex
	logFile         *os.File
	queuePaused     bool
	queueMutex      sync.Mutex
)

func init() {
	// Load environment variables from .env file
	if err := godotenv.Load(); err != nil {
		log.Println("Warning: .env file not found or could not be loaded")
	}

	// Set up logging
	logFile = utils.SetupLogging()

	// Initialize database
	db = database.InitDB()
}

func cleanup() {
	if logFile != nil {
		log.Println("Closing log file")
		logFile.Close()
	}

	if db != nil {
		log.Println("Closing database connection")
		db.Close()
	}
}

func main() {
	// Set up cleanup on exit
	defer cleanup()

	gin.SetMode(gin.ReleaseMode)

	// Create a custom gin instance with the logger configuration
	// that logs only requests to both console and file
	router := gin.New()

	// Configure Gin to log requests to both console and file
	router.Use(gin.LoggerWithWriter(io.MultiWriter(os.Stdout, logFile)))
	router.Use(gin.Recovery())

	// Add CORS middleware
	router.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	})

	// Create files directory if it doesn't exist
	if err := os.MkdirAll("files", 0755); err != nil {
		log.Fatalf("Failed to create files directory: %v", err)
	}

	// Create Frontend directory if it doesn't exist
	if err := os.MkdirAll("Frontend", 0755); err != nil {
		log.Printf("Note: Frontend directory may not exist: %v", err)
	}

	// Initialize handlers
	jobHandlers := handlers.NewJobHandlers(db, &purgeMutex, &queueMutex, &purgeInProgress, &queuePaused)
	adminHandlers := handlers.NewAdminHandlers(db, &purgeMutex, &queueMutex, &purgeInProgress, &queuePaused)
	ocrHandlers := handlers.NewOCRHandlers(db, &purgeMutex, &queueMutex, &purgeInProgress, &queuePaused)
	processHandlers := handlers.NewProcessHandlers(db, &purgeMutex, &queueMutex, &purgeInProgress, &queuePaused)
	datasetHandlers := handlers.NewDatasetHandlers(db, &purgeMutex, &queueMutex, &purgeInProgress, &queuePaused)
	rabbitmqHandlers := handlers.NewRabbitMQHandlers(db, &purgeMutex, &queueMutex, &purgeInProgress, &queuePaused)
	fileHandlers := handlers.NewFileHandlers(db, &purgeMutex, &queueMutex, &purgeInProgress, &queuePaused)
	websocketHandlers := handlers.NewWebSocketHandlers(db)
	healthHandlers := handlers.NewHealthHandlers(db)
	vectorStoreHandlers := handlers.NewVectorStoreHandlers(db)

	// Start a goroutine to periodically check and update job statuses
	go updateJobStatuses()

	// API routes
	v1 := router.Group("/api/v1")
	{
		v1.POST("/add/job/pdf", ocrHandlers.OCRPDF)
		v1.POST("/add/job/image", ocrHandlers.OCRImage)
		v1.GET("/job/:jobID", jobHandlers.GetJob)

		// File serving routes
		v1.GET("/files/:jobID/:pageFile", fileHandlers.ServeImage)
		v1.GET("/job/:jobID/images", fileHandlers.GetJobImagePaths)
		v1.GET("/job/:jobID/page/:pageNumber/text", fileHandlers.GetJobPageText)

		// Add RabbitMQ routes
		v1.GET("/rabbitmq/overview", rabbitmqHandlers.GetRabbitMQOverview)
		v1.GET("/rabbitmq/queues", rabbitmqHandlers.GetRabbitMQQueues)
		// WebSocket endpoint for real-time updates
		v1.GET("/rabbitmq/ws", rabbitmqHandlers.WebSocketHandler)

		// Add WebSocket endpoint for job statistics
		v1.GET("/jobstats/ws", websocketHandlers.JobStatsWebSocketHandler)
		// Add token stats websocket endpoint
		v1.GET("/tokenstats/ws", websocketHandlers.TokenStatsWebSocketHandler)

		// Add dataset export endpoint without admin prefix
		v1.GET("/dataset/job/:jobID/export", datasetHandlers.ExportJobDataset)

		// Add health WebSocket endpoint for system status monitoring
		v1.GET("/health/ws", healthHandlers.HealthWebSocketHandler)

		// Add new endpoints for OCR processing
		v1.GET("/jobs/ocr-eligible", jobHandlers.ListJobsForOCR)
		v1.GET("/job/:jobID/unprocessed-pages", jobHandlers.GetUnprocessedJobPages)

		// Add new endpoint for jobs with OCR text
		v1.GET("/jobs/with-text", jobHandlers.GetJobsWithText)

		// Add new endpoint for job pages with OCR text
		v1.GET("/job/:jobID/pages-with-text", jobHandlers.GetJobPagesWithText)

		// Add new endpoints to check if a page is in OCR queue or Results queue
		v1.GET("/job/:jobID/page/:pageNumber/ocr-status", jobHandlers.CheckPageInOcrQueue)
		v1.GET("/job/:jobID/page/:pageNumber/results-status", jobHandlers.CheckPageInResultsQueue)

		admin := v1.Group("/admin")
		{
			admin.POST("/purge-queue", adminHandlers.ClearQueue)
			admin.POST("/purge-results-queue", adminHandlers.ClearResultsQueue)
			admin.GET("/job/:jobID/results", jobHandlers.GetJobResults)
			admin.GET("/jobs", jobHandlers.ListJobs)

			// Job management endpoints
			admin.POST("/start", jobHandlers.StartAllJobs)
			admin.POST("/start/job/:jobID", jobHandlers.StartJob)
			admin.POST("/start/job/:jobID/page/:pageNumber", jobHandlers.StartJobPage)
			admin.POST("/stop", adminHandlers.StopQueue)
			admin.DELETE("/delete/job/:jobID/page/:pageNumber", jobHandlers.DeleteJobPage)
			admin.DELETE("/delete/job/:jobID", jobHandlers.DeleteJob)
			admin.DELETE("/delete/jobs", jobHandlers.DeleteAllJobs)

			// New endpoint to update all jobs' completion status
			admin.POST("/update-completion", adminHandlers.UpdateAllJobsStatus)

			// OCR processing endpoints
			admin.POST("/process/start", processHandlers.StartProcess)
			admin.POST("/process/start/job/:jobID", processHandlers.StartJobProcess)
			admin.POST("/process/delete/job/:jobID", processHandlers.DeleteJobFromOCRQueue)

			// Dataset generation endpoints
			admin.POST("/dataset/start", datasetHandlers.StartDatasetGeneration)
			admin.POST("/dataset/start/job/:jobID", datasetHandlers.StartJobDatasetGeneration)
			admin.DELETE("/dataset/delete", datasetHandlers.DeleteAllDatasets)
			admin.DELETE("/dataset/delete/job/:jobID", datasetHandlers.DeleteJobDataset)
			admin.GET("/dataset/get/job/:jobID/page/:pageNumber", datasetHandlers.GetJobPageDataset)
			admin.GET("/dataset/export/json/job/:jobID/page/:pageNumber", datasetHandlers.ExportJobPageDatasetJSON)
			admin.GET("/dataset/export/xlsx/job/:jobID/page/:pageNumber", datasetHandlers.ExportJobPageDatasetXLSX)
			admin.GET("/dataset/job/:jobID/export", datasetHandlers.ExportJobDataset)
			admin.GET("/dataset/export/all", datasetHandlers.ExportAllJobsDataset)

			// New endpoint for token usage statistics
			admin.GET("/dataset/tokens", datasetHandlers.GetTokenUsageStats)

			// Add new endpoints for sending jobs to the results queue
			admin.POST("/send-to-results/job/:jobID/page/:pageNumber", jobHandlers.SendJobPageToResults)
			admin.POST("/send-to-results/job/:jobID", jobHandlers.SendAllJobPagesToResults)

			// Settings endpoints
			admin.GET("/settings", handlers.GetSettings(db))
			admin.POST("/settings", handlers.UpdateSettings(db))

			// Provider settings routes
			admin.GET("/provider-settings", handlers.GetProviderSettings(db))
			admin.POST("/provider-settings", handlers.UpdateProviderSettings(db))

			// OpenAI API key endpoint
			admin.GET("/openai-api-key", handlers.GetOpenAIAPIKey(db))
		}

		// Add vector store routes
		v1.GET("/vectorcost/ws", vectorStoreHandlers.WebSocketHandler)
		v1.POST("/vectorcost", vectorStoreHandlers.GetCostEstimation)
	}

	router.Run(":8080")
}

// updateJobStatuses periodically checks and updates the status of jobs
func updateJobStatuses() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		// Find all jobs that are not marked as completed but have all their tasks completed
		rows, err := db.Query(`
			SELECT job_id, total_tasks, completed_tasks 
			FROM jobs 
			WHERE status != 'completed' 
			AND completed_tasks >= total_tasks
			AND total_tasks > 0
		`)
		if err != nil {
			log.Printf("Error querying jobs for status update: %v", err)
			continue
		}

		completedCount := 0
		for rows.Next() {
			var jobID string
			var totalTasks, completedTasks int
			if err := rows.Scan(&jobID, &totalTasks, &completedTasks); err != nil {
				log.Printf("Error scanning job row: %v", err)
				continue
			}

			_, err := db.Exec("UPDATE jobs SET status = 'completed' WHERE job_id = $1", jobID)
			if err != nil {
				log.Printf("Error updating job %s to completed status: %v", jobID, err)
			} else {
				log.Printf("Job %s marked as completed (%d/%d pages processed)",
					jobID, completedTasks, totalTasks)
				completedCount++
			}
		}
		rows.Close()

		if completedCount > 0 {
			log.Printf("Batch status update: marked %d jobs as completed", completedCount)
		}
	}
}
