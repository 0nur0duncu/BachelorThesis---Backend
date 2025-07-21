package handlers

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"image/png"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath" // Add this import
	"sync"
	"time"

	"github.com/BachelorThesis/Backend/queue"
	"github.com/BachelorThesis/Backend/utils"

	"github.com/gen2brain/go-fitz"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

// OCRHandlers contains handlers for OCR operations
type OCRHandlers struct {
	db              *sql.DB
	purgeMutex      *sync.Mutex
	queueMutex      *sync.Mutex
	purgeInProgress *bool
	queuePaused     *bool
	queueManager    *queue.QueueManager
}

// NewOCRHandlers creates a new OCRHandlers instance
func NewOCRHandlers(db *sql.DB, purgeMutex *sync.Mutex, queueMutex *sync.Mutex,
	purgeInProgress *bool, queuePaused *bool) *OCRHandlers {
	return &OCRHandlers{
		db:              db,
		purgeMutex:      purgeMutex,
		queueMutex:      queueMutex,
		purgeInProgress: purgeInProgress,
		queuePaused:     queuePaused,
		queueManager:    queue.NewQueueManager(db, purgeMutex, queueMutex, purgeInProgress, queuePaused),
	}
}

// UpdateJobStatus updates the status of a job
func (h *OCRHandlers) UpdateJobStatus(jobID, status string, totalTasks, completedTasks int) {
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

// OCRImage handles OCR processing for individual images
func (h *OCRHandlers) OCRImage(c *gin.Context) {
	file, header, err := c.Request.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No file uploaded"})
		return
	}
	defer file.Close()

	imageBytes, err := io.ReadAll(file)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read file"})
		return
	}

	jobID := uuid.New().String()
	filename := header.Filename

	// Create job record
	_, err = h.db.Exec(
		"INSERT INTO jobs (job_id, status, total_tasks, completed_tasks, filename) VALUES ($1, $2, $3, $4, $5)",
		jobID, "pending", 1, 0, filename,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create job"})
		return
	}
	log.Printf("Created new image job in database: ID=%s, filename=%s", jobID, filename)

	// Create directory for this job
	jobDir := fmt.Sprintf("files/%s", jobID)
	if err := os.MkdirAll(jobDir, 0755); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create job directory"})
		return
	}

	// Save the image file
	pagePath := fmt.Sprintf("%s/1.png", jobDir)
	imageFile, err := os.Create(pagePath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create image file"})
		return
	}

	if _, err := imageFile.Write(imageBytes); err != nil {
		imageFile.Close()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to write image file"})
		return
	}
	imageFile.Close()

	// Create a worker_results record
	relativePath := fmt.Sprintf("files/%s/1.png", jobID)
	_, err = h.db.Exec(
		"INSERT INTO worker_results (job_id, page_number, path, processing_status) VALUES ($1, $2, $3, NULL)",
		jobID, 1, relativePath,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create worker_results record"})
		return
	}

	// We don't automatically send to queue anymore
	// User will explicitly start OCR processing

	c.JSON(http.StatusAccepted, gin.H{
		"job_id": jobID,
		"status": "pending",
	})
}

// extractPDFPages extracts pages from a PDF and saves them as PNG images
func (h *OCRHandlers) extractPDFPages(jobID string, pdfBytes []byte, excludedPages []int) (int, error) {
	// Create directory for this job
	jobDir := fmt.Sprintf("files/%s", jobID)
	if err := os.MkdirAll(jobDir, 0755); err != nil {
		return 0, fmt.Errorf("failed to create job directory: %v", err)
	}

	// Ensure temp directory exists
	tmpDir := "./temp"
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return 0, fmt.Errorf("failed to create temp directory: %v", err)
	}

	// Create temp file for the PDF with the original filename
	filename := filepath.Base(jobID) + ".pdf" // Use jobID as filename base to ensure uniqueness
	tempFilePath := filepath.Join(tmpDir, filename)
	tempFile, err := os.Create(tempFilePath)
	if err != nil {
		return 0, fmt.Errorf("failed to create temporary file: %v", err)
	}

	if _, err := tempFile.Write(pdfBytes); err != nil {
		tempFile.Close()
		return 0, fmt.Errorf("failed to write temporary file: %v", err)
	}
	tempFile.Close()

	// Open PDF with fitz with retry mechanism
	var doc *fitz.Document
	var openErr error

	// Try up to 3 times to open the PDF
	for attempts := 0; attempts < 3; attempts++ {
		doc, openErr = fitz.New(tempFilePath)
		if openErr == nil {
			break
		}
		log.Printf("Failed to open PDF (attempt %d): %v", attempts+1, openErr)
		time.Sleep(100 * time.Millisecond)
	}

	if openErr != nil {
		return 0, fmt.Errorf("failed to open PDF with fitz after retries: %v", openErr)
	}
	defer doc.Close()

	totalPages := doc.NumPage()
	processedPages := 0

	// Process in batches of 10 pages
	batchSize := 10
	var processingErrors []error
	var successfulPages []int

	for i := 0; i < totalPages; i += batchSize {
		var wg sync.WaitGroup
		end := i + batchSize
		if end > totalPages {
			end = totalPages
		}

		// Process batch
		for pageIdx := i; pageIdx < end; pageIdx++ {
			pageNum := pageIdx + 1
			if utils.Contains(excludedPages, pageNum) {
				continue
			}

			wg.Add(1)
			go func(idx, num int) {
				defer wg.Done()

				// Extract page as image
				img, err := doc.Image(idx)
				if err != nil {
					log.Printf("Error extracting page %d: %v", num, err)
					processingErrors = append(processingErrors, fmt.Errorf("page %d: %v", num, err))
					return
				}

				// Save as PNG
				pagePath := fmt.Sprintf("%s/%d.png", jobDir, num)
				file, err := os.Create(pagePath)
				if err != nil {
					log.Printf("Error creating file for page %d: %v", num, err)
					processingErrors = append(processingErrors, fmt.Errorf("page %d: %v", num, err))
					return
				}
				if err := png.Encode(file, img); err != nil {
					file.Close()
					log.Printf("Error saving PNG for page %d: %v", num, err)
					processingErrors = append(processingErrors, fmt.Errorf("page %d: %v", num, err))
					return
				}
				file.Close()

				// Update path in database
				relativePath := fmt.Sprintf("files/%s/%d.png", jobID, num)
				_, err = h.db.Exec(
					"UPDATE worker_results SET path = $1 WHERE job_id = $2 AND page_number = $3",
					relativePath, jobID, num,
				)
				if err != nil {
					log.Printf("Error updating path in database for page %d: %v", num, err)
					processingErrors = append(processingErrors, fmt.Errorf("database update page %d: %v", num, err))
					return
				}

				// Track successful processing
				successfulPages = append(successfulPages, num)
			}(pageIdx, pageNum)
		}

		wg.Wait()
	}

	processedPages = len(successfulPages)

	if len(processingErrors) > 0 {
		// If we have some processed pages but also some errors, continue but log errors
		if processedPages > 0 {
			log.Printf("Processed %d pages with %d errors for job %s",
				processedPages, len(processingErrors), jobID)
			for _, err := range processingErrors {
				log.Printf("- %v", err)
			}
		} else {
			// If no pages were processed, return an error
			return 0, fmt.Errorf("failed to process any pages: %v", processingErrors[0])
		}
	}

	return processedPages, nil
}

// cleanupTempFiles removes older temporary files
func (h *OCRHandlers) cleanupTempFiles() {
	tmpDir := "./temp"
	files, err := os.ReadDir(tmpDir)
	if err != nil {
		log.Printf("Failed to read temp directory: %v", err)
		return
	}

	currentTime := time.Now()
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filePath := filepath.Join(tmpDir, file.Name())
		fileInfo, err := file.Info()
		if err != nil {
			log.Printf("Failed to get file info for %s: %v", file.Name(), err)
			continue
		}

		// If file is older than 1 hour, delete it
		if currentTime.Sub(fileInfo.ModTime()) > 1*time.Hour {
			if err := os.Remove(filePath); err != nil {
				log.Printf("Failed to remove old temp file %s: %v", filePath, err)
			} else {
				log.Printf("Removed old temp file: %s", filePath)
			}
		}
	}
}

// OCRPDF processes a PDF file for OCR
func (h *OCRHandlers) OCRPDF(c *gin.Context) {
	file, header, err := c.Request.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No file uploaded"})
		return
	}
	defer file.Close()

	// Parse excluded pages from JSON array
	excludedPagesJSON := c.PostForm("excluded_pages")
	var excludedPages []int
	if excludedPagesJSON != "" {
		if err := json.Unmarshal([]byte(excludedPagesJSON), &excludedPages); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid excluded_pages format"})
			return
		}
	}

	pdfBytes, err := io.ReadAll(file)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read file"})
		return
	}

	filename := header.Filename

	// Create temp directory and file for initial PDF processing
	tmpDir := "./temp"
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create temp directory"})
		return
	}

	tempFilePath := fmt.Sprintf("%s/pdf-%s.pdf", tmpDir, uuid.New().String())
	tempFile, err := os.Create(tempFilePath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create temporary file"})
		return
	}
	defer func() {
		tempFile.Close()
		os.Remove(tempFilePath)
		go h.cleanupTempFiles() // Clean up older temp files
	}()

	if _, err := tempFile.Write(pdfBytes); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to write temporary file"})
		return
	}

	// Just get the total number of pages for now
	doc, err := fitz.New(tempFilePath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to open PDF: %v", err)})
		return
	}
	totalPages := doc.NumPage()
	doc.Close()

	// Calculate actual number of pages to process (excluding the excluded ones)
	tasksCount := totalPages - len(excludedPages)

	if tasksCount <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "No pages to process. All pages are excluded.",
		})
		return
	}

	// Check if a PDF with the same filename and total page count (tasks + excluded) already exists
	var existingJobID string
	var existingStatus string
	err = h.db.QueryRow(`
		SELECT job_id, status FROM jobs 
		WHERE filename = $1 
		AND (total_tasks + array_length(excluded_pages, 1)) = $2
		ORDER BY created_at DESC LIMIT 1`,
		filename, totalPages).Scan(&existingJobID, &existingStatus)

	// If we found a matching job, return that job's ID instead of creating a new one
	if err == nil && existingJobID != "" {
		log.Printf("Found existing job %s with same filename and page count", existingJobID)
		c.JSON(http.StatusAccepted, gin.H{
			"job_id":           existingJobID,
			"status":           existingStatus,
			"excluded_pages":   excludedPages,
			"total_pages":      totalPages,
			"processing_pages": tasksCount,
			"message":          "Using existing job with same filename and page count",
		})
		return
	}

	// If no existing job found or there was an error, create a new one
	jobID := uuid.New().String()

	// Create job record with excluded pages
	_, err = h.db.Exec(
		"INSERT INTO jobs (job_id, status, total_tasks, completed_tasks, filename, excluded_pages) VALUES ($1, $2, $3, $4, $5, $6)",
		jobID, "pending", tasksCount, 0, filename, pq.Array(excludedPages),
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create job"})
		return
	}

	// Pre-create worker_results records for all non-excluded pages
	for i := 0; i < totalPages; i++ {
		pageNum := i + 1
		if utils.Contains(excludedPages, pageNum) {
			continue
		}
		_, err = h.db.Exec(
			"INSERT INTO worker_results (job_id, page_number, processing_status) VALUES ($1, $2, NULL)",
			jobID, pageNum,
		)
		if err != nil {
			log.Printf("Failed to create worker result record for page %d: %v", pageNum, err)
			// Continue despite errors
		}
	}

	// Process PDF pages in background
	go func() {
		processedPages, err := h.extractPDFPages(jobID, pdfBytes, excludedPages)
		if err != nil {
			log.Printf("Error extracting PDF pages for job %s: %v", jobID, err)
			h.UpdateJobStatus(jobID, "failed", tasksCount, 0)
		} else {
			log.Printf("Successfully extracted %d pages for job %s", processedPages, jobID)
			h.UpdateJobStatus(jobID, "extracted", tasksCount, 0)
		}
	}()

	c.JSON(http.StatusAccepted, gin.H{
		"job_id":           jobID,
		"status":           "pending",
		"excluded_pages":   excludedPages,
		"total_pages":      totalPages,
		"processing_pages": tasksCount,
	})
}
