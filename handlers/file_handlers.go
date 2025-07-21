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

	"github.com/gin-gonic/gin"
)

// FileHandlers contains handlers for file-related operations
type FileHandlers struct {
	db              *sql.DB
	purgeMutex      *sync.Mutex
	queueMutex      *sync.Mutex
	purgeInProgress *bool
	queuePaused     *bool
}

// NewFileHandlers creates a new FileHandlers instance
func NewFileHandlers(db *sql.DB, purgeMutex *sync.Mutex, queueMutex *sync.Mutex,
	purgeInProgress *bool, queuePaused *bool) *FileHandlers {
	return &FileHandlers{
		db:              db,
		purgeMutex:      purgeMutex,
		queueMutex:      queueMutex,
		purgeInProgress: purgeInProgress,
		queuePaused:     queuePaused,
	}
}

// ServeImage serves an image file for a specific job and page
func (h *FileHandlers) ServeImage(c *gin.Context) {
	jobID := c.Param("jobID")
	pageFile := c.Param("pageFile") // This will be like "1.png"

	// Extract page number from filename
	pageNumberStr := pageFile
	if extension := filepath.Ext(pageFile); extension != "" {
		pageNumberStr = pageFile[:len(pageFile)-len(extension)]
	}

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
			c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("Image for job %s page %d not found", jobID, pageNumber)})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		}
		return
	}

	// Check if file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		c.JSON(http.StatusNotFound, gin.H{"error": "Image file not found on server"})
		return
	}

	// Set cache control headers
	c.Header("Cache-Control", "public, max-age=86400") // Cache for 24 hours

	// Serve the file
	c.File(path)

	log.Printf("Served image for job %s page %d from path %s", jobID, pageNumber, path)
}

// GetJobPageText serves the OCR text for a specific job page
func (h *FileHandlers) GetJobPageText(c *gin.Context) {
	jobID := c.Param("jobID")
	pageNumberStr := c.Param("pageNumber")
	pageNumber, err := strconv.Atoi(pageNumberStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid page number"})
		return
	}

	// Get text from database
	var text sql.NullString
	err = h.db.QueryRow("SELECT text FROM worker_results WHERE job_id = $1 AND page_number = $2",
		jobID, pageNumber).Scan(&text)

	if err != nil {
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("Text for job %s page %d not found", jobID, pageNumber)})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		}
		return
	}

	// Return the text content
	content := ""
	if text.Valid {
		content = text.String
	}

	c.JSON(http.StatusOK, gin.H{
		"job_id":      jobID,
		"page_number": pageNumber,
		"text":        content,
	})
}

// GetJobImagePaths returns a list of all image paths for a specific job
func (h *FileHandlers) GetJobImagePaths(c *gin.Context) {
	jobID := c.Param("jobID")

	// Check if job exists
	var exists bool
	err := h.db.QueryRow("SELECT EXISTS(SELECT 1 FROM jobs WHERE job_id = $1)", jobID).Scan(&exists)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}

	// Query database for file paths
	rows, err := h.db.Query(`
		SELECT page_number, path 
		FROM worker_results 
		WHERE job_id = $1 AND path IS NOT NULL 
		ORDER BY page_number`,
		jobID)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	defer rows.Close()

	var results []gin.H
	for rows.Next() {
		var pageNumber int
		var path string

		if err := rows.Scan(&pageNumber, &path); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Error scanning results"})
			return
		}

		// Check if file exists
		_, err := os.Stat(path)
		fileExists := !os.IsNotExist(err)

		// Construct image URL
		imageURL := fmt.Sprintf("/api/v1/files/%s/%d.png", jobID, pageNumber)

		results = append(results, gin.H{
			"job_id":      jobID,
			"page_number": pageNumber,
			"path":        path,
			"image_url":   imageURL,
			"file_exists": fileExists,
		})
	}

	if err = rows.Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Error reading results"})
		return
	}

	if len(results) == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("No images found for job %s", jobID)})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"job_id": jobID,
		"count":  len(results),
		"images": results,
	})
}
