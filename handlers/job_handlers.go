package handlers

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/BachelorThesis/Backend/database"
	"github.com/BachelorThesis/Backend/queue"
	"github.com/BachelorThesis/Backend/utils"

	"github.com/gin-gonic/gin"
	"github.com/lib/pq"
)

// JobHandlers contains handlers for job-related operations
type JobHandlers struct {
	db              *sql.DB
	purgeMutex      *sync.Mutex
	queueMutex      *sync.Mutex
	purgeInProgress *bool
	queuePaused     *bool
	queueManager    *queue.QueueManager
}

// NewJobHandlers creates a new JobHandlers instance
func NewJobHandlers(db *sql.DB, purgeMutex *sync.Mutex, queueMutex *sync.Mutex,
	purgeInProgress *bool, queuePaused *bool) *JobHandlers {
	return &JobHandlers{
		db:              db,
		purgeMutex:      purgeMutex,
		queueMutex:      queueMutex,
		purgeInProgress: purgeInProgress,
		queuePaused:     queuePaused,
		queueManager:    queue.NewQueueManager(db, purgeMutex, queueMutex, purgeInProgress, queuePaused),
	}
}

// UpdateJobStatus updates the status of a job
func (h *JobHandlers) UpdateJobStatus(jobID, status string, totalTasks, completedTasks int) {
	_, err := h.db.Exec(
		"UPDATE jobs SET status = $1, total_tasks = $2, completed_tasks = $3 WHERE job_id = $4",
		status, totalTasks, completedTasks, jobID,
	)
	if err != nil {
		log.Printf("Error updating job status: %v", err)
	} else {
		log.Printf("Updated job %s status: status=%s, completed=%d/%d",
			jobID, status, completedTasks, totalTasks)
	}
}

// GetJob retrieves job status
func (h *JobHandlers) GetJob(c *gin.Context) {
	jobID := c.Param("jobID")

	var job database.Job
	err := h.db.QueryRow(
		"SELECT job_id, filename, status, total_tasks, completed_tasks FROM jobs WHERE job_id = $1",
		jobID,
	).Scan(&job.JobID, &job.Filename, &job.Status, &job.TotalTasks, &job.CompletedTasks)

	if err != nil {
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		}
		return
	}

	progress := utils.CalculateProgress(job.CompletedTasks, job.TotalTasks)

	c.JSON(http.StatusOK, gin.H{
		"job_id":   job.JobID,
		"filename": job.Filename,
		"status":   job.Status,
		"progress": fmt.Sprintf("%.2f%%", progress),
	})
}

// GetJobResults retrieves results for a specific job
func (h *JobHandlers) GetJobResults(c *gin.Context) {
	jobID := c.Param("jobID")

	rows, err := h.db.Query(
		"SELECT job_id, page_number, processed_at, COALESCE(text, '') as text FROM worker_results WHERE job_id = $1 ORDER BY page_number",
		jobID,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	defer rows.Close()

	var results []gin.H
	for rows.Next() {
		var (
			jobID       string
			pageNumber  int
			processedAt time.Time
			text        string
		)
		if err := rows.Scan(&jobID, &pageNumber, &processedAt, &text); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
			return
		}
		results = append(results, gin.H{
			"job_id":       jobID,
			"page_number":  pageNumber,
			"processed_at": processedAt,
			"text":         text,
			"status": func() string {
				if text == "" {
					return "pending"
				} else {
					return "completed"
				}
			}(),
		})
	}

	if err = rows.Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	if len(results) == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "No results found"})
		return
	}

	c.JSON(http.StatusOK, results)
}

// ListJobs retrieves a list of all jobs
func (h *JobHandlers) ListJobs(c *gin.Context) {
	rows, err := h.db.Query(
		"SELECT job_id, filename, status, total_tasks, completed_tasks FROM jobs ORDER BY created_at",
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	defer rows.Close()

	var jobs []gin.H
	for rows.Next() {
		var job database.Job
		if err := rows.Scan(&job.JobID, &job.Filename, &job.Status, &job.TotalTasks, &job.CompletedTasks); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
			return
		}

		progress := utils.CalculateProgress(job.CompletedTasks, job.TotalTasks)
		jobs = append(jobs, gin.H{
			"job_id":   job.JobID,
			"filename": job.Filename,
			"status":   job.Status,
			"progress": fmt.Sprintf("%.2f%%", progress),
		})
	}

	if err = rows.Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	c.JSON(http.StatusOK, jobs)
}

// ListJobsForOCR retrieves a list of jobs that have unprocessed pages
func (h *JobHandlers) ListJobsForOCR(c *gin.Context) {
	// This query finds jobs that have at least one page without OCR text
	rows, err := h.db.Query(`
		SELECT DISTINCT j.job_id, j.filename, j.status, j.total_tasks, j.completed_tasks
		FROM jobs j
		JOIN worker_results wr ON j.job_id = wr.job_id
		WHERE j.status != 'completed'
		AND EXISTS (
			SELECT 1 FROM worker_results
			WHERE job_id = j.job_id
			AND path IS NOT NULL
			AND (text IS NULL OR text = '')
		)
		ORDER BY j.created_at DESC
	`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	defer rows.Close()

	var jobs []gin.H
	for rows.Next() {
		var job database.Job
		if err := rows.Scan(&job.JobID, &job.Filename, &job.Status, &job.TotalTasks, &job.CompletedTasks); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
			return
		}

		progress := utils.CalculateProgress(job.CompletedTasks, job.TotalTasks)
		jobs = append(jobs, gin.H{
			"job_id":   job.JobID,
			"filename": job.Filename,
			"status":   job.Status,
			"progress": fmt.Sprintf("%.2f%%", progress),
		})
	}

	if err = rows.Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	c.JSON(http.StatusOK, jobs)
}

// GetJobsWithText retrieves a list of jobs that have at least one page with OCR text
func (h *JobHandlers) GetJobsWithText(c *gin.Context) {
	// This query finds jobs that have at least one page with OCR text
	rows, err := h.db.Query(`
		SELECT DISTINCT j.job_id, j.filename, j.status, j.total_tasks, j.completed_tasks
		FROM jobs j
		JOIN worker_results wr ON j.job_id = wr.job_id
		WHERE wr.text IS NOT NULL AND wr.text != ''
		ORDER BY j.created_at DESC
	`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	defer rows.Close()

	var jobs []gin.H
	for rows.Next() {
		var job database.Job
		if err := rows.Scan(&job.JobID, &job.Filename, &job.Status, &job.TotalTasks, &job.CompletedTasks); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
			return
		}

		progress := utils.CalculateProgress(job.CompletedTasks, job.TotalTasks)
		jobs = append(jobs, gin.H{
			"job_id":   job.JobID,
			"filename": job.Filename,
			"status":   job.Status,
			"progress": fmt.Sprintf("%.2f%%", progress),
		})
	}

	if err = rows.Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	c.JSON(http.StatusOK, jobs)
}

// GetUnprocessedJobPages retrieves only unprocessed pages for a specific job
func (h *JobHandlers) GetUnprocessedJobPages(c *gin.Context) {
	jobID := c.Param("jobID")

	// First check if the job exists
	var totalTasks int
	var excludedPagesArray pq.Int64Array
	err := h.db.QueryRow("SELECT total_tasks, excluded_pages FROM jobs WHERE job_id = $1", jobID).Scan(&totalTasks, &excludedPagesArray)
	if err != nil {
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		}
		return
	}

	// Convert excluded pages to an integer slice
	excludedPages := make([]int, len(excludedPagesArray))
	for i, v := range excludedPagesArray {
		excludedPages[i] = int(v)
	}

	// Get all worker_results entries for this job that don't have OCR text
	// Expanded query to include pages both with and without paths
	rows, err := h.db.Query(`
		SELECT job_id, page_number, processed_at, path
		FROM worker_results
		WHERE job_id = $1
		AND (text IS NULL OR text = '')
		ORDER BY page_number
	`, jobID)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	defer rows.Close()

	var results []gin.H
	for rows.Next() {
		var (
			jobID       string
			pageNumber  int
			processedAt time.Time
			path        sql.NullString
		)
		if err := rows.Scan(&jobID, &pageNumber, &processedAt, &path); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
			return
		}

		// Skip excluded pages
		if utils.Contains(excludedPages, pageNumber) {
			continue
		}

		// Include page in results regardless of path status
		results = append(results, gin.H{
			"job_id":       jobID,
			"page_number":  pageNumber,
			"processed_at": processedAt,
			"has_path":     path.Valid && path.String != "",
			"status":       "pending",
		})
	}

	if err = rows.Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	// For jobs with status 'extracted', 'interrupted' or 'failed',
	// we want to make sure pages that don't have worker_results entries yet are still included
	var jobStatus string
	err = h.db.QueryRow("SELECT status FROM jobs WHERE job_id = $1", jobID).Scan(&jobStatus)
	if err == nil && (jobStatus == "extracted" || jobStatus == "interrupted" || jobStatus == "failed" || jobStatus == "ready") {
		// Also include "ready" status to ensure more pages are included

		// Find all the page numbers we already have
		var existingPages []int
		for _, result := range results {
			if pageNum, ok := result["page_number"].(int); ok {
				existingPages = append(existingPages, pageNum)
			}
		}

		// Make sure we account for all possible pages from 1 to total_tasks
		// This ensures we don't miss any pages that should exist
		for pageNum := 1; pageNum <= totalTasks; pageNum++ {
			if utils.Contains(excludedPages, pageNum) {
				continue // Skip excluded pages
			}

			if !utils.Contains(existingPages, pageNum) {
				// Entry doesn't exist, create it and add to results
				// _, err = h.db.Exec(
				// 	"INSERT INTO worker_results (job_id, page_number) VALUES ($1, $2) ON CONFLICT DO NOTHING",
				// 	jobID, pageNum,
				// )
				// if err != nil {
				// 	log.Printf("Error creating worker_result entry for job %s page %d: %v", jobID, pageNum, err)
				// 	continue
				// }

				results = append(results, gin.H{
					"job_id":       jobID,
					"page_number":  pageNum,
					"processed_at": time.Now(),
					"has_path":     false,
					"status":       "pending",
				})
			}
		}
	}

	log.Printf("Returning %d unprocessed pages for job %s", len(results), jobID)

	// Return empty array instead of 404 if no results
	if len(results) == 0 {
		c.JSON(http.StatusOK, []gin.H{})
		return
	}

	c.JSON(http.StatusOK, results)
}

// GetJobPagesWithText retrieves only pages with OCR text for a specific job
func (h *JobHandlers) GetJobPagesWithText(c *gin.Context) {
	jobID := c.Param("jobID")

	// Query only pages that have text (OCR completed)
	rows, err := h.db.Query(`
		SELECT job_id, page_number, processed_at, text
		FROM worker_results
		WHERE job_id = $1
		AND text IS NOT NULL 
		AND text != ''
		ORDER BY page_number
	`, jobID)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	defer rows.Close()

	var results []gin.H
	for rows.Next() {
		var (
			jobID       string
			pageNumber  int
			processedAt time.Time
			text        string
		)
		if err := rows.Scan(&jobID, &pageNumber, &processedAt, &text); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
			return
		}
		results = append(results, gin.H{
			"job_id":       jobID,
			"page_number":  pageNumber,
			"processed_at": processedAt,
			"text":         text,
			"status":       "completed",
		})
	}

	if err = rows.Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	// Return empty array instead of 404 if no results
	if len(results) == 0 {
		c.JSON(http.StatusOK, []gin.H{})
		return
	}

	c.JSON(http.StatusOK, results)
}

// StartAllJobs starts all jobs that are in extracted or interrupted status
func (h *JobHandlers) StartAllJobs(c *gin.Context) {
	h.queueMutex.Lock()
	*h.queuePaused = false
	h.queueMutex.Unlock()

	rows, err := h.db.Query("SELECT job_id FROM jobs WHERE status = 'extracted' OR status = 'interrupted'")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	defer rows.Close()

	var jobIDs []string
	for rows.Next() {
		var jobID string
		if err := rows.Scan(&jobID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
			return
		}
		jobIDs = append(jobIDs, jobID)
	}

	if err = rows.Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	if len(jobIDs) == 0 {
		c.JSON(http.StatusOK, gin.H{
			"status":  "success",
			"message": "No jobs to start",
		})
		return
	}

	go func() {
		for _, jobID := range jobIDs {
			// Get job info
			var totalTasks int
			var excludedPagesArray pq.Int64Array // Change to pq.Int64Array
			err := h.db.QueryRow("SELECT total_tasks, excluded_pages FROM jobs WHERE job_id = $1",
				jobID).Scan(&totalTasks, &excludedPagesArray) // Remove pq.Array()

			if err != nil {
				log.Printf("Error getting job info for %s: %v", jobID, err)
				continue
			}

			// Convert int64 array to int array if needed
			excludedPages := make([]int, len(excludedPagesArray))
			for i, v := range excludedPagesArray {
				excludedPages[i] = int(v)
			}

			// Get all pages for the job
			rows, err := h.db.Query("SELECT page_number FROM worker_results WHERE job_id = $1 AND path IS NOT NULL", jobID)
			if err != nil {
				log.Printf("Error querying pages for job %s: %v", jobID, err)
				continue
			}

			var pageNumbers []int
			for rows.Next() {
				var pageNum int
				if err := rows.Scan(&pageNum); err != nil {
					log.Printf("Error scanning page number: %v", err)
					continue
				}
				pageNumbers = append(pageNumbers, pageNum)
			}
			rows.Close()

			// Update job status before sending to queue
			h.UpdateJobStatus(jobID, "queuing", totalTasks, 0)

			// Send each page to queue in batches to prevent overwhelming the queue
			batchSize := 10
			sentCount := 0
			for i := 0; i < len(pageNumbers); i += batchSize {
				end := i + batchSize
				if end > len(pageNumbers) {
					end = len(pageNumbers)
				}

				for j := i; j < end; j++ {
					pageNum := pageNumbers[j]
					if h.queueManager.SendPageToQueue(jobID, pageNum) {
						sentCount++
					} else {
						log.Printf("Failed to send page %d of job %s", pageNum, jobID)
					}
				}

				// Small delay between batches to allow queue processing
				time.Sleep(100 * time.Millisecond)
			}

			// Update job status after queuing
			if sentCount > 0 {
				h.UpdateJobStatus(jobID, "queued", totalTasks, 0)
				log.Printf("Successfully queued %d/%d pages for job %s", sentCount, len(pageNumbers), jobID)
			} else {
				h.UpdateJobStatus(jobID, "failed", totalTasks, 0)
				log.Printf("Failed to queue any pages for job %s", jobID)
			}
		}
	}()

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": fmt.Sprintf("Starting %d jobs", len(jobIDs)),
		"jobs":    jobIDs,
	})
}

// StartJob starts a specific job
func (h *JobHandlers) StartJob(c *gin.Context) {
	jobID := c.Param("jobID")

	h.queueMutex.Lock()
	*h.queuePaused = false
	h.queueMutex.Unlock()

	// Check if job exists and is in correct status
	var status string
	var totalTasks int
	err := h.db.QueryRow("SELECT status, total_tasks FROM jobs WHERE job_id = $1", jobID).Scan(&status, &totalTasks)
	if err != nil {
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		}
		return
	}

	if status != "extracted" && status != "interrupted" && status != "failed" {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Job is in %s status and cannot be started", status)})
		return
	}

	// Update job status to indicate queuing in progress
	h.UpdateJobStatus(jobID, "queuing", totalTasks, 0)

	go func() {
		// Get all pages for the job with non-null paths
		rows, err := h.db.Query("SELECT page_number FROM worker_results WHERE job_id = $1 AND path IS NOT NULL", jobID)
		if err != nil {
			log.Printf("Error querying pages for job %s: %v", jobID, err)
			h.UpdateJobStatus(jobID, "failed", totalTasks, 0)
			return
		}
		defer rows.Close()

		var pageNumbers []int
		for rows.Next() {
			var pageNum int
			if err := rows.Scan(&pageNum); err != nil {
				log.Printf("Error scanning page number: %v", err)
				continue
			}
			pageNumbers = append(pageNumbers, pageNum)
		}

		if err = rows.Err(); err != nil {
			log.Printf("Error iterating rows: %v", err)
			h.UpdateJobStatus(jobID, "failed", totalTasks, 0)
			return
		}

		if len(pageNumbers) == 0 {
			log.Printf("No valid pages found for job %s", jobID)
			h.UpdateJobStatus(jobID, "failed", totalTasks, 0)
			return
		}

		// Send pages to queue in batches
		batchSize := 10
		sentCount := 0
		for i := 0; i < len(pageNumbers); i += batchSize {
			end := i + batchSize
			if end > len(pageNumbers) {
				end = len(pageNumbers)
			}

			for j := i; j < end; j++ {
				pageNum := pageNumbers[j]
				if h.queueManager.SendPageToQueue(jobID, pageNum) {
					sentCount++
				} else {
					log.Printf("Failed to send page %d of job %s", pageNum, jobID)
				}
			}

			// Small delay between batches
			time.Sleep(100 * time.Millisecond)
		}

		if sentCount > 0 {
			h.UpdateJobStatus(jobID, "queued", totalTasks, 0)
			log.Printf("Successfully queued %d/%d pages for job %s", sentCount, len(pageNumbers), jobID)
		} else {
			h.UpdateJobStatus(jobID, "failed", totalTasks, 0)
			log.Printf("Failed to queue any pages for job %s", jobID)
		}
	}()

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": fmt.Sprintf("Starting job %s", jobID),
	})
}

// Add this new function to check if a page is in queue
func (h *JobHandlers) isPageInQueue(jobID string, pageNumber int, queueType string) (bool, error) {
	// Check if a worker_results record exists for this page
	var exists bool
	err := h.db.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM worker_results 
			WHERE job_id = $1 AND page_number = $2
		)`, jobID, pageNumber).Scan(&exists)

	if err != nil {
		return false, fmt.Errorf("database error checking record existence: %v", err)
	}

	// If no record exists, page is definitely not in any queue
	if !exists {
		return false, nil
	}

	// Now check specific queue type
	if queueType == "ocr" {
		// For OCR queue, check processing status directly
		var processingStatus sql.NullString
		err := h.db.QueryRow(`
			SELECT processing_status
			FROM worker_results
			WHERE job_id = $1 AND page_number = $2
		`, jobID, pageNumber).Scan(&processingStatus)

		if err != nil {
			if err == sql.ErrNoRows {
				// If row doesn't exist, it means the page isn't in the queue
				return false, nil
			}
			return false, fmt.Errorf("database error: %v", err)
		}

		// Check if the page is currently in the OCR processing pipeline
		if processingStatus.Valid {
			inQueue := processingStatus.String == "processing" ||
				processingStatus.String == "queued" ||
				processingStatus.String == "queuing"
			return inQueue, nil
		}
	} else if queueType == "results" {
		// For results queue, check if the page has been marked as in results queue
		var inQueue bool
		err := h.db.QueryRow(`
			SELECT EXISTS (
				SELECT 1 FROM worker_results 
				WHERE job_id = $1 AND page_number = $2 AND processing_status = 'results_queue'
			)`, jobID, pageNumber).Scan(&inQueue)

		if err != nil {
			return false, fmt.Errorf("database error checking results queue: %v", err)
		}

		return inQueue, nil
	}

	return false, nil
}

// Update StartJobPage function
func (h *JobHandlers) StartJobPage(c *gin.Context) {
	jobID := c.Param("jobID")
	pageNumber, err := strconv.Atoi(c.Param("pageNumber"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid page number"})
		return
	}

	// Check if page is already in OCR queue
	inQueue, err := h.isPageInQueue(jobID, pageNumber, "ocr")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to check queue status"})
		return
	}
	if inQueue {
		c.JSON(http.StatusConflict, gin.H{"error": "Page is already in OCR queue"})
		return
	}

	h.queueMutex.Lock()
	*h.queuePaused = false
	h.queueMutex.Unlock()

	// Check if job exists
	var status string
	var totalTasks int
	err = h.db.QueryRow("SELECT status, total_tasks FROM jobs WHERE job_id = $1", jobID).Scan(&status, &totalTasks)
	if err != nil {
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		}
		return
	}

	// Check if page exists for job
	var path string
	err = h.db.QueryRow("SELECT path FROM worker_results WHERE job_id = $1 AND page_number = $2",
		jobID, pageNumber).Scan(&path)
	if err != nil {
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "Page not found for this job"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		}
		return
	}

	// Send page to queue
	if h.queueManager.SendPageToQueue(jobID, pageNumber) {
		c.JSON(http.StatusOK, gin.H{
			"status":  "success",
			"message": fmt.Sprintf("Page %d of job %s sent to queue", pageNumber, jobID),
		})
	} else {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "Failed to send page to queue",
		})
	}
}

// Update SendJobPageToResults function
func (h *JobHandlers) SendJobPageToResults(c *gin.Context) {
	jobID := c.Param("jobID")
	pageNumber, err := strconv.Atoi(c.Param("pageNumber"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid page number"})
		return
	}

	// Check if page is already in results queue
	inQueue, err := h.isPageInQueue(jobID, pageNumber, "results")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to check queue status"})
		return
	}
	if inQueue {
		c.JSON(http.StatusConflict, gin.H{"error": "Page is already in results queue"})
		return
	}

	// Check if job exists
	var exists bool
	err = h.db.QueryRow("SELECT EXISTS(SELECT 1 FROM jobs WHERE job_id = $1)", jobID).Scan(&exists)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("Database error: %v", err),
		})
		return
	}

	if !exists {
		c.JSON(http.StatusNotFound, gin.H{
			"status":  "error",
			"message": "Job not found",
		})
		return
	}

	// Send the page directly to results queue
	if h.queueManager.SendJobPageToResultsQueue(jobID, pageNumber) {
		c.JSON(http.StatusOK, gin.H{
			"status":  "success",
			"message": fmt.Sprintf("Page %d of job %s sent to results queue", pageNumber, jobID),
		})
	} else {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "Failed to send page to results queue. Text may not be available.",
		})
	}
}

// SendAllJobPagesToResults sends all pages of a job directly to the results queue
func (h *JobHandlers) SendAllJobPagesToResults(c *gin.Context) {
	jobID := c.Param("jobID")

	// Check if job exists
	var exists bool
	err := h.db.QueryRow("SELECT EXISTS(SELECT 1 FROM jobs WHERE job_id = $1)", jobID).Scan(&exists)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("Database error: %v", err),
		})
		return
	}

	if !exists {
		c.JSON(http.StatusNotFound, gin.H{
			"status":  "error",
			"message": "Job not found",
		})
		return
	}

	// Send all pages to results queue
	success, count := h.queueManager.SendAllJobPagesToResultsQueue(jobID)
	if success {
		c.JSON(http.StatusOK, gin.H{
			"status":  "success",
			"message": fmt.Sprintf("Sent %d pages of job %s to results queue", count, jobID),
			"count":   count,
		})
	} else {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "Failed to send pages to results queue. No text data may be available.",
		})
	}
}

// DeleteJobPage deletes a specific page of a job
func (h *JobHandlers) DeleteJobPage(c *gin.Context) {
	jobID := c.Param("jobID")
	pageNumberStr := c.Param("pageNumber")
	pageNumber, err := strconv.Atoi(pageNumberStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid page number"})
		return
	}

	// Get file path from database
	var path string
	err = h.db.QueryRow("SELECT path FROM worker_results WHERE job_id = $1 AND page_number = $2",
		jobID, pageNumber).Scan(&path)
	if err != nil {
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "Page not found"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		}
		return
	}

	// Delete file if it exists
	if path != "" {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			log.Printf("Error deleting file %s: %v", path, err)
		}
	}

	// Delete record from database
	_, err = h.db.Exec("DELETE FROM worker_results WHERE job_id = $1 AND page_number = $2",
		jobID, pageNumber)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	// Update job's total tasks
	_, err = h.db.Exec("UPDATE jobs SET total_tasks = total_tasks - 1 WHERE job_id = $1", jobID)
	if err != nil {
		log.Printf("Error updating job total tasks: %v", err)
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": fmt.Sprintf("Page %d of job %s deleted", pageNumber, jobID),
	})
}

// DeleteJob deletes a specific job and all its associated data
func (h *JobHandlers) DeleteJob(c *gin.Context) {
	jobID := c.Param("jobID")

	// Get all file paths for the job
	rows, err := h.db.Query("SELECT path FROM worker_results WHERE job_id = $1", jobID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	defer rows.Close()

	var paths []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
			return
		}
		if path != "" {
			paths = append(paths, path)
		}
	}

	// Delete all files for the job
	for _, path := range paths {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			log.Printf("Error deleting file %s: %v", path, err)
		}
	}

	// Delete job directory
	jobDir := fmt.Sprintf("files/%s", jobID)
	if err := os.RemoveAll(jobDir); err != nil && !os.IsNotExist(err) {
		log.Printf("Error deleting job directory %s: %v", jobDir, err)
	}

	// Begin transaction
	tx, err := h.db.Begin()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	// Delete worker results first (foreign key constraint)
	_, err = tx.Exec("DELETE FROM worker_results WHERE job_id = $1", jobID)
	if err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	// Delete job record
	result, err := tx.Exec("DELETE FROM jobs WHERE job_id = $1", jobID)
	if err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	// Commit transaction
	if err = tx.Commit(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": fmt.Sprintf("Job %s deleted", jobID),
	})
}

// DeleteAllJobs deletes all jobs and associated data
// Note: This is distinct from queue purging and should not be called after purging the queue
// if you want to preserve files and database records
func (h *JobHandlers) DeleteAllJobs(c *gin.Context) {
	// Delete all files in the files directory
	entries, err := os.ReadDir("files")
	if err != nil {
		log.Printf("Error reading files directory: %v", err)
	} else {
		for _, entry := range entries {
			if entry.IsDir() {
				path := filepath.Join("files", entry.Name())
				if err := os.RemoveAll(path); err != nil {
					log.Printf("Error deleting directory %s: %v", path, err)
				}
			}
		}
	}

	// Begin transaction
	tx, err := h.db.Begin()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	// Delete worker results first (foreign key constraint)
	_, err = tx.Exec("DELETE FROM worker_results")
	if err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	// Delete all jobs
	result, err := tx.Exec("DELETE FROM jobs")
	if err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	// Commit transaction
	if err = tx.Commit(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	rowsAffected, _ := result.RowsAffected()

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": fmt.Sprintf("Deleted %d jobs and all associated files", rowsAffected),
	})
}

// CheckPageInOcrQueue checks if a page is in OCR queue or already processed
func (h *JobHandlers) CheckPageInOcrQueue(c *gin.Context) {
	jobID := c.Param("jobID")
	pageNumber, err := strconv.Atoi(c.Param("pageNumber"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid page number"})
		return
	}

	// Check if job exists
	var jobStatus string
	err = h.db.QueryRow("SELECT status FROM jobs WHERE job_id = $1", jobID).Scan(&jobStatus)
	if err != nil {
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		}
		return
	}

	// Check if the page exists in worker_results and get its status
	var textExists bool
	var processingStatus sql.NullString
	var path sql.NullString
	err = h.db.QueryRow(`
		SELECT 
			(text IS NOT NULL AND text != ''),
			processing_status,
			path
		FROM worker_results 
		WHERE job_id = $1 AND page_number = $2
	`, jobID, pageNumber).Scan(&textExists, &processingStatus, &path)

	if err != nil {
		if err == sql.ErrNoRows {
			// If page doesn't exist in worker_results yet, consider it as not in queue
			c.JSON(http.StatusOK, gin.H{
				"inQueue": false,
				"hasText": false,
				"status":  jobStatus,
				"hasPath": false,
			})
			return
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
			return
		}
	}

	// Determine if the page is in queue:
	// 1. If it has text, it's not in OCR queue (it's completed)
	// 2. If processing_status indicates it's being processed, it's in queue
	// 3. We need to also check if the page has a valid path, as pages without
	//    paths can't be sent to OCR queue
	inQueue := false
	if !textExists {
		if processingStatus.Valid {
			inQueue = processingStatus.String == "processing" ||
				processingStatus.String == "queued" ||
				processingStatus.String == "queuing"
		}
	}

	hasPath := path.Valid && path.String != ""

	// Return the status information with more detailed context
	c.JSON(http.StatusOK, gin.H{
		"inQueue":  inQueue,
		"hasText":  textExists,
		"hasPath":  hasPath,
		"status":   jobStatus,
		"canQueue": hasPath && !textExists && !inQueue,
	})
}

// CheckPageInResultsQueue checks if a page has been sent to results queue
func (h *JobHandlers) CheckPageInResultsQueue(c *gin.Context) {
	jobID := c.Param("jobID")
	pageNumber, err := strconv.Atoi(c.Param("pageNumber"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid page number"})
		return
	}

	// Check if job exists first
	var jobExists bool
	err = h.db.QueryRow("SELECT EXISTS(SELECT 1 FROM jobs WHERE job_id = $1)", jobID).Scan(&jobExists)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	if !jobExists {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}

	// Check if the page exists and get text status and processing status
	var textExists bool
	var processingStatus sql.NullString
	var path sql.NullString

	err = h.db.QueryRow(`
		SELECT 
			(text IS NOT NULL AND text != ''), 
			processing_status,
			path
		FROM worker_results 
		WHERE job_id = $1 AND page_number = $2
	`, jobID, pageNumber).Scan(&textExists, &processingStatus, &path)

	if err != nil {
		if err == sql.ErrNoRows {
			// If page doesn't exist in worker_results yet, it can't be in the results queue
			c.JSON(http.StatusOK, gin.H{
				"inQueue":   false,
				"hasText":   false,
				"eligible":  false,
				"hasPath":   false,
				"statusMsg": "Page record not found",
			})
			return
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
			return
		}
	}

	// Check if page is already in results queue
	inResultsQueue := processingStatus.Valid && processingStatus.String == "results_queue"
	hasPath := path.Valid && path.String != ""

	// If no text exists, it can't be in results queue
	if !textExists {
		c.JSON(http.StatusOK, gin.H{
			"inQueue":   inResultsQueue,
			"hasText":   false,
			"eligible":  false,
			"hasPath":   hasPath,
			"statusMsg": "Page has no OCR text",
		})
		return
	}

	// Return the page status
	statusMsg := "Page is eligible for results queue"
	if inResultsQueue {
		statusMsg = "Page is already in results queue"
	}

	c.JSON(http.StatusOK, gin.H{
		"inQueue":   inResultsQueue,
		"hasText":   true,
		"eligible":  !inResultsQueue, // Only eligible if not already in queue
		"hasPath":   hasPath,
		"statusMsg": statusMsg,
	})
}
