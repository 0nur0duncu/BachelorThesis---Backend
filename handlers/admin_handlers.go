package handlers

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/BachelorThesis/Backend/queue"

	"github.com/gin-gonic/gin"
)

// AdminHandlers contains handlers for administrative operations
type AdminHandlers struct {
	db              *sql.DB
	purgeMutex      *sync.Mutex
	queueMutex      *sync.Mutex
	purgeInProgress *bool
	queuePaused     *bool
	queueManager    *queue.QueueManager
}

// NewAdminHandlers creates a new AdminHandlers instance
func NewAdminHandlers(db *sql.DB, purgeMutex *sync.Mutex, queueMutex *sync.Mutex,
	purgeInProgress *bool, queuePaused *bool) *AdminHandlers {
	return &AdminHandlers{
		db:              db,
		purgeMutex:      purgeMutex,
		queueMutex:      queueMutex,
		purgeInProgress: purgeInProgress,
		queuePaused:     queuePaused,
		queueManager:    queue.NewQueueManager(db, purgeMutex, queueMutex, purgeInProgress, queuePaused),
	}
}

// ClearQueue handles the request to clear the OCR queue
func (h *AdminHandlers) ClearQueue(c *gin.Context) {
	log.Println("Purging OCR queue...")

	success, result := h.queueManager.PurgeQueue()
	if success {
		c.JSON(http.StatusOK, gin.H{
			"status":  "success",
			"message": "Queue purged successfully",
			"result":  result,
		})
	} else {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "Failed to purge queue",
			"error":   result,
		})
	}
}

// ClearResultsQueue handles the request to clear the OCR results queue
func (h *AdminHandlers) ClearResultsQueue(c *gin.Context) {
	log.Println("ClearResultsQueue handler called - Purging OCR results queue...")

	// Verify queue manager is initialized
	if h.queueManager == nil {
		log.Println("Error: queueManager is nil!")
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "Queue manager not initialized",
		})
		return
	}

	// Attempt to purge the queue and log detailed info
	log.Println("Calling PurgeResultsQueue method...")
	success, result := h.queueManager.PurgeResultsQueue()
	log.Printf("PurgeResultsQueue returned - success: %v, result: %v", success, result)

	if success {
		// Even if reported as success, verify it worked
		log.Println("Queue purge reported successful. Messages removed:", result)
		c.JSON(http.StatusOK, gin.H{
			"status":  "success",
			"message": "Results queue purged successfully",
			"result":  result,
		})
	} else {
		log.Printf("Error purging results queue: %v", result)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "Failed to purge results queue",
			"error":   result,
		})
	}
}

// StopQueue handles the request to pause the OCR processing
func (h *AdminHandlers) StopQueue(c *gin.Context) {
	h.queueMutex.Lock()
	*h.queuePaused = true
	h.queueMutex.Unlock()

	log.Println("OCR queue processing paused")

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "OCR queue processing paused",
	})
}

// UpdateAllJobsStatus updates the status of all jobs based on their completion
func (h *AdminHandlers) UpdateAllJobsStatus(c *gin.Context) {
	log.Println("Checking all jobs for completion status...")

	// Query jobs that need status checking
	rows, err := h.db.Query(`
		SELECT job_id, total_tasks, completed_tasks 
		FROM jobs 
		WHERE status != 'completed' 
		AND total_tasks > 0
	`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "Failed to query jobs",
			"error":   err.Error(),
		})
		return
	}
	defer rows.Close()

	var updatedJobs []string
	var completedCount int

	// Process each job
	for rows.Next() {
		var jobID string
		var totalTasks, completedTasks int
		if err := rows.Scan(&jobID, &totalTasks, &completedTasks); err != nil {
			log.Printf("Error scanning job row: %v", err)
			continue
		}

		// Get count of pages with text for this job
		var textCount int
		err := h.db.QueryRow(`
			SELECT COUNT(*) 
			FROM worker_results 
			WHERE job_id = $1 AND text IS NOT NULL
		`, jobID).Scan(&textCount)

		if err != nil {
			log.Printf("Error counting text results for job %s: %v", jobID, err)
			continue
		}

		// If all pages have text, mark job as completed
		if textCount >= totalTasks {
			_, err := h.db.Exec("UPDATE jobs SET status = 'completed', completed_tasks = $1 WHERE job_id = $2",
				textCount, jobID)
			if err != nil {
				log.Printf("Error updating job %s status: %v", jobID, err)
				continue
			}

			updatedJobs = append(updatedJobs, jobID)
			completedCount++
			log.Printf("Job %s marked as completed (%d/%d pages processed)",
				jobID, textCount, totalTasks)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"status":        "success",
		"message":       fmt.Sprintf("Updated %d jobs to completed status", completedCount),
		"updated_jobs":  updatedJobs,
		"updated_count": completedCount,
	})
}
