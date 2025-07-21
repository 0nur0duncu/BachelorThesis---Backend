package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/BachelorThesis/Backend/config"

	vision "cloud.google.com/go/vision/v2/apiv1"
	visionpb "cloud.google.com/go/vision/v2/apiv1/visionpb"
	"github.com/gin-gonic/gin"
	"github.com/streadway/amqp"
)

var (
	workerCtx      context.Context
	workerCancelFn context.CancelFunc
)

// ProcessedResult represents an OCR processing result
type ProcessedResult struct {
	JobID      string `json:"job_id"`
	PageNumber int    `json:"page_number"`
	Text       string `json:"text"`
}

// ProcessHandlers contains handlers for OCR processing operations
type ProcessHandlers struct {
	db              *sql.DB
	purgeMutex      *sync.Mutex
	queueMutex      *sync.Mutex
	purgeInProgress *bool
	queuePaused     *bool
}

// NewProcessHandlers creates a new ProcessHandlers instance
func NewProcessHandlers(db *sql.DB, purgeMutex *sync.Mutex, queueMutex *sync.Mutex,
	purgeInProgress *bool, queuePaused *bool) *ProcessHandlers {
	return &ProcessHandlers{
		db:              db,
		purgeMutex:      purgeMutex,
		queueMutex:      queueMutex,
		purgeInProgress: purgeInProgress,
		queuePaused:     queuePaused,
	}
}

// StartProcess starts processing the current OCR jobs in the queue
func (h *ProcessHandlers) StartProcess(c *gin.Context) {
	// Get number of workers from query parameter (default to 1)
	workersStr := c.DefaultQuery("workers", "1")
	workers, err := strconv.Atoi(workersStr)
	if err != nil || workers < 1 {
		workers = 1
	}

	// Create a new context for this batch of processing
	workerCtx, workerCancelFn = context.WithCancel(context.Background())

	log.Printf("Starting OCR processing with %d workers for current queue items", workers)

	go h.startWorkers(workers)

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": fmt.Sprintf("Started processing current OCR queue items with %d workers", workers),
	})
}

// StartJobProcess starts OCR processing for a specific job
func (h *ProcessHandlers) StartJobProcess(c *gin.Context) {
	jobID := c.Param("jobID")
	log.Printf("StartJobProcess called for job ID: %s", jobID)

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

	// Start processing with a new context
	workerCtx, workerCancelFn = context.WithCancel(context.Background())

	// Get number of workers (default to 1)
	workers := 1

	// Start workers
	go h.startWorkers(workers)

	log.Printf("OCR processing started for job %s with %d worker(s)", jobID, workers)

	// Update job status to indicate it's being prepared for processing
	_, err = h.db.Exec(
		"UPDATE jobs SET status = 'processing' WHERE job_id = $1 AND status NOT IN ('completed', 'processing')",
		jobID)
	if err != nil {
		log.Printf("Error updating job status for %s: %v", jobID, err)
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": fmt.Sprintf("OCR processing started for job %s", jobID),
	})
}

// DeleteJobFromOCRQueue removes a specific job from the OCR queue
func (h *ProcessHandlers) DeleteJobFromOCRQueue(c *gin.Context) {
	jobID := c.Param("jobID")
	log.Printf("DeleteJobFromOCRQueue called for job ID: %s", jobID)

	// Connect to RabbitMQ
	conn, err := amqp.Dial(config.GetRabbitMQURL())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("Failed to connect to RabbitMQ: %v", err),
		})
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("Failed to open channel: %v", err),
		})
		return
	}
	defer ch.Close()

	// Note: RabbitMQ doesn't have a direct way to remove specific messages by job ID
	// The approach is to purge the queue and requeue everything except the job we want to delete

	// First, lock the queue
	h.queueMutex.Lock()
	*h.queuePaused = true
	h.queueMutex.Unlock()

	// Declare the OCR queue
	queue, err := ch.QueueDeclare(
		"ocr_queue", // name
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		h.queueMutex.Lock()
		*h.queuePaused = false
		h.queueMutex.Unlock()

		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("Failed to declare queue: %v", err),
		})
		return
	}

	// Get queue count before
	countBefore := queue.Messages

	// Update job status to indicate it's being removed from queue
	_, err = h.db.Exec(
		"UPDATE jobs SET status = 'interrupted' WHERE job_id = $1 AND status = 'queued'",
		jobID)
	if err != nil {
		log.Printf("Error updating job status for %s: %v", jobID, err)
	}

	h.queueMutex.Lock()
	*h.queuePaused = false
	h.queueMutex.Unlock()

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": fmt.Sprintf("Job %s marked as interrupted and will not be processed from OCR queue", jobID),
		"queue": gin.H{
			"count_before": countBefore,
		},
	})
}

// startWorkers starts the specified number of worker goroutines to process OCR tasks
func (h *ProcessHandlers) startWorkers(numWorkers int) {
	var wg sync.WaitGroup
	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go h.processOCRWorker(workerCtx, i, &wg)
	}

	// Wait for all workers to complete
	wg.Wait()
	log.Println("All OCR workers have completed processing the current queue items")
}

// processOCRWorker runs a worker that processes images from the queue
func (h *ProcessHandlers) processOCRWorker(ctx context.Context, workerID int, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("Worker %d starting", workerID)

	// Initialize Vision client
	client, err := vision.NewImageAnnotatorClient(ctx)
	if err != nil {
		log.Printf("Worker %d: Failed to create Vision client: %v", workerID, err)
		return
	}
	defer client.Close()

	// Connect to RabbitMQ
	conn, err := amqp.Dial(config.GetRabbitMQURL())
	if err != nil {
		log.Printf("Worker %d: Failed to connect to RabbitMQ: %v", workerID, err)
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Printf("Worker %d: Failed to open channel: %v", workerID, err)
		return
	}
	defer ch.Close()

	// Declare input queue
	inputQ, err := ch.QueueDeclare(
		"ocr_queue", // name
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		log.Printf("Worker %d: Failed to declare input queue: %v", workerID, err)
		return
	}

	// Check if there are any messages in the queue
	queueInfo, err := ch.QueueInspect(inputQ.Name)
	if err != nil {
		log.Printf("Worker %d: Failed to inspect queue: %v", workerID, err)
		return
	}

	// If queue is empty, worker can stop immediately
	if queueInfo.Messages == 0 {
		log.Printf("Worker %d: Queue is empty, no jobs to process", workerID)
		return
	}

	// Store the current number of messages to process
	initialMessageCount := queueInfo.Messages
	log.Printf("Worker %d: Queue has %d messages to process", workerID, initialMessageCount)

	// Track how many messages we've processed from the initial batch
	processedCount := 0

	// Declare output queue for processed results
	outputQ, err := ch.QueueDeclare(
		"ocr_results", // name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		log.Printf("Worker %d: Failed to declare output queue: %v", workerID, err)
		return
	}

	// Set prefetch count to 1 to distribute work fairly
	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		log.Printf("Worker %d: Failed to set QoS: %v", workerID, err)
		return
	}

	// Create a context with timeout to automatically stop if we get stuck
	workerCtxWithTimeout, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	msgs, err := ch.Consume(
		inputQ.Name, // queue
		"",          // consumer
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		log.Printf("Worker %d: Failed to register consumer: %v", workerID, err)
		return
	}

	log.Printf("Worker %d is now consuming from queue", workerID)

	for {
		select {
		case <-workerCtxWithTimeout.Done():
			log.Printf("Worker %d stopping due to timeout or cancellation", workerID)
			return
		case msg, ok := <-msgs:
			if !ok {
				log.Printf("Worker %d: Channel closed", workerID)
				return
			}

			// Process message
			jobID, ok := msg.Headers["job_id"].(string)
			if !ok {
				log.Printf("Worker %d: Missing job_id in message headers", workerID)
				msg.Nack(false, false)
				continue
			}

			pageNumberVal, ok := msg.Headers["page_number"]
			if !ok {
				log.Printf("Worker %d: Missing page_number in message headers", workerID)
				msg.Nack(false, false)
				continue
			}
			pageNumber, ok := pageNumberVal.(int32)
			if !ok {
				log.Printf("Worker %d: page_number is not an integer", workerID)
				msg.Nack(false, false)
				continue
			}

			log.Printf("Worker %d processing job %s page %d", workerID, jobID, pageNumber)

			// Process OCR with Google Cloud Vision
			text, err := h.processImageWithVision(ctx, client, msg.Body)
			if err != nil {
				log.Printf("Worker %d: OCR processing failed for job %s page %d: %v",
					workerID, jobID, pageNumber, err)
				msg.Nack(false, true) // Requeue for retry
				continue
			}

			// Update database with OCR results
			_, err = h.db.ExecContext(ctx,
				"UPDATE worker_results SET text = $1, processing_status = 'processed' WHERE job_id = $2 AND page_number = $3",
				text, jobID, pageNumber)
			if err != nil {
				log.Printf("Worker %d: Failed to update database for job %s page %d: %v",
					workerID, jobID, pageNumber, err)
				msg.Nack(false, true) // Requeue for retry
				continue
			}

			// Update completed tasks count for this job
			_, err = h.db.ExecContext(ctx,
				"UPDATE jobs SET completed_tasks = completed_tasks + 1 WHERE job_id = $1",
				jobID)
			if err != nil {
				log.Printf("Worker %d: Failed to update job completion count for %s: %v",
					workerID, jobID, err)
				// Continue processing even if this update fails
			}

			// Check if job is now complete
			h.checkAndUpdateJobCompletion(jobID)

			// Send to output queue
			result := ProcessedResult{
				JobID:      jobID,
				PageNumber: int(pageNumber),
				Text:       text,
			}

			resultBytes, err := json.Marshal(result)
			if err != nil {
				log.Printf("Worker %d: Failed to marshal result for job %s page %d: %v",
					workerID, jobID, pageNumber, err)
				// Continue even if sending to output queue fails
			} else {
				err = ch.Publish(
					"",           // exchange
					outputQ.Name, // routing key
					false,        // mandatory
					false,        // immediate
					amqp.Publishing{
						Headers: amqp.Table{
							"job_id":      jobID,
							"page_number": pageNumber,
						},
						ContentType:  "application/json",
						Body:         resultBytes,
						DeliveryMode: amqp.Persistent,
					})
				if err != nil {
					log.Printf("Worker %d: Failed to publish result to output queue: %v", workerID, err)
					// Continue even if sending to output queue fails
				} else {
					log.Printf("Worker %d: Published result to output queue for job %s page %d",
						workerID, jobID, pageNumber)
				}
			}

			// Acknowledge message only after all processing is complete
			msg.Ack(false)
			log.Printf("Worker %d completed processing job %s page %d", workerID, jobID, pageNumber)

			// Increment processed count and check if we've processed all initial messages
			processedCount++
			if processedCount >= int(initialMessageCount) {
				log.Printf("Worker %d: Finished processing all %d initial messages in queue", workerID, initialMessageCount)
				return
			}
		}
	}
}

// checkAndUpdateJobCompletion checks if all pages of a job have been processed
// and updates the job status to "completed" if so
func (h *ProcessHandlers) checkAndUpdateJobCompletion(jobID string) {
	var totalTasks, completedTasks int
	var status string

	err := h.db.QueryRow(
		"SELECT total_tasks, completed_tasks, status FROM jobs WHERE job_id = $1",
		jobID).Scan(&totalTasks, &completedTasks, &status)

	if err != nil {
		log.Printf("Error checking job completion status for %s: %v", jobID, err)
		return
	}

	// If all tasks are completed but status isn't "completed" yet, update it
	if completedTasks >= totalTasks && status != "completed" {
		_, err := h.db.Exec(
			"UPDATE jobs SET status = 'completed' WHERE job_id = $1",
			jobID)
		if err != nil {
			log.Printf("Error updating job %s to completed status: %v", jobID, err)
		} else {
			log.Printf("Job %s marked as completed (%d/%d pages processed)",
				jobID, completedTasks, totalTasks)
		}
	}
}

// processImageWithVision processes an image with Google Cloud Vision API
// with improved layout preservation
func (h *ProcessHandlers) processImageWithVision(ctx context.Context, client *vision.ImageAnnotatorClient, imageData []byte) (string, error) {
	// Create the request with the image data
	req := &visionpb.AnnotateImageRequest{
		Image: &visionpb.Image{
			Content: imageData,
		},
		Features: []*visionpb.Feature{
			{
				Type:       visionpb.Feature_DOCUMENT_TEXT_DETECTION,
				MaxResults: 1,
			},
		},
		ImageContext: &visionpb.ImageContext{
			LanguageHints: []string{"tr", "en"}, // Add English and Turkish language hints
		},
	}

	// Call the BatchAnnotateImages method
	resp, err := client.BatchAnnotateImages(ctx, &visionpb.BatchAnnotateImagesRequest{
		Requests: []*visionpb.AnnotateImageRequest{req},
	})
	if err != nil {
		return "", fmt.Errorf("failed to detect text: %v", err)
	}

	// Check if we got a response
	if len(resp.Responses) == 0 {
		return "", fmt.Errorf("no response from Vision API")
	}

	// Check for API errors
	if resp.Responses[0].Error != nil {
		return "", fmt.Errorf("API error: %v", resp.Responses[0].Error.Message)
	}

	fullTextAnnotation := resp.Responses[0].FullTextAnnotation
	if fullTextAnnotation == nil {
		log.Println("No text found in the image")
		return "", nil
	}

	// Process the document structure to preserve layout with enhanced method
	return enhancedProcessStructuredText(fullTextAnnotation), nil
}

// enhancedProcessStructuredText handles the full text annotation with improved layout preservation
func enhancedProcessStructuredText(fullTextAnnotation *visionpb.TextAnnotation) string {
	var result strings.Builder

	// Process the text to preserve line breaks with \n character
	processedText := strings.ReplaceAll(fullTextAnnotation.Text, ".", ".\n")
	processedText2 := strings.ReplaceAll(processedText, "-", "")

	// Filter out non-alphanumeric characters, but preserve spaces, newlines, and Turkish characters
	// reg := regexp.MustCompile("[^a-zA-ZçÇğĞıİöÖşŞüÜ0-9.?! \n]")
	// filteredText := reg.ReplaceAllString(processedText2, "")

	// result.WriteString(filteredText)
	result.WriteString(processedText2)

	return result.String()
}
