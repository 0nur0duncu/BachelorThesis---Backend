package queue

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time" // Adding missing time import

	"github.com/BachelorThesis/Backend/config"

	"github.com/streadway/amqp"
)

// QueueManager handles operations related to the RabbitMQ queue
type QueueManager struct {
	db              *sql.DB
	purgeMutex      *sync.Mutex
	queueMutex      *sync.Mutex
	purgeInProgress *bool
	queuePaused     *bool
}

// NewQueueManager creates a new QueueManager
func NewQueueManager(db *sql.DB, purgeMutex *sync.Mutex, queueMutex *sync.Mutex,
	purgeInProgress *bool, queuePaused *bool) *QueueManager {
	return &QueueManager{
		db:              db,
		purgeMutex:      purgeMutex,
		queueMutex:      queueMutex,
		purgeInProgress: purgeInProgress,
		queuePaused:     queuePaused,
	}
}

// SendToQueue sends image data to RabbitMQ queue
func (qm *QueueManager) SendToQueue(jobID string, imageBytes []byte, pageNumber int) bool {
	qm.purgeMutex.Lock()
	if *qm.purgeInProgress {
		qm.purgeMutex.Unlock()
		log.Printf("Queue purge in progress, not sending job %s page %d", jobID, pageNumber)
		return false
	}
	qm.purgeMutex.Unlock()

	conn, err := amqp.Dial(config.GetRabbitMQURL())
	if err != nil {
		log.Printf("Failed to connect to RabbitMQ: %v", err)
		return false
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Printf("Failed to open a channel: %v", err)
		return false
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"ocr_queue", // name
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		log.Printf("Failed to declare a queue: %v", err)
		return false
	}

	headers := amqp.Table{
		"job_id":      jobID,
		"page_number": pageNumber,
	}

	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			Headers:      headers,
			ContentType:  "application/octet-stream",
			Body:         imageBytes,
			DeliveryMode: amqp.Persistent,
		})
	if err != nil {
		log.Printf("Failed to publish a message: %v", err)
		return false
	}

	// Update job status after successfully sending to queue
	_, err = qm.db.Exec(
		"UPDATE jobs SET status = $1 WHERE job_id = $2",
		"queued", jobID,
	)
	if err != nil {
		log.Printf("Error updating job status: %v", err)
	}

	return true
}

// SendPageToQueue sends a specific page to the queue
func (qm *QueueManager) SendPageToQueue(jobID string, pageNumber int) bool {
	qm.queueMutex.Lock()
	if *qm.queuePaused {
		qm.queueMutex.Unlock()
		log.Printf("Queue is paused, not sending job %s page %d", jobID, pageNumber)
		return false
	}
	qm.queueMutex.Unlock()

	qm.purgeMutex.Lock()
	if *qm.purgeInProgress {
		qm.purgeMutex.Unlock()
		log.Printf("Queue purge in progress, not sending job %s page %d", jobID, pageNumber)
		return false
	}
	qm.purgeMutex.Unlock()

	// Önce worker_results tablosunda kayıt var mı kontrol et
	var recordExists bool
	err := qm.db.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM worker_results 
			WHERE job_id = $1 AND page_number = $2
		)`,
		jobID, pageNumber).Scan(&recordExists)

	if err != nil {
		log.Printf("Failed to check if record exists in worker_results: %v", err)
		return false
	}

	// Get the path and check if text already exists
	var path string
	var text sql.NullString
	var processingStatus sql.NullString
	err = qm.db.QueryRow("SELECT path, text, processing_status FROM worker_results WHERE job_id = $1 AND page_number = $2",
		jobID, pageNumber).Scan(&path, &text, &processingStatus)

	if err != nil {
		log.Printf("Failed to get path for job %s page %d: %v", jobID, pageNumber, err)
		return false
	}

	// Check if page has already been processed and is in results queue
	if processingStatus.Valid && processingStatus.String == "results_queue" {
		log.Printf("Page %d of job %s is already in results queue, not sending to OCR queue", pageNumber, jobID)
		return false
	}

	// Check if page already has text content
	if text.Valid && text.String != "" {
		log.Printf("Page %d of job %s already has OCR text content, not sending to OCR queue unless explicitly requested to re-process",
			pageNumber, jobID)
		// We don't automatically clear existing text anymore unless explicitly requested to reprocess
		return false
	}

	if path == "" {
		log.Printf("Path is empty for job %s page %d", jobID, pageNumber)
		return false
	}

	// Check if file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		log.Printf("File does not exist: %s", path)
		return false
	}

	// Check if this page is already being processed
	if processingStatus.Valid && (processingStatus.String == "processing" ||
		processingStatus.String == "queued" || processingStatus.String == "queuing") {
		log.Printf("Page %d of job %s is already in queue (status: %s)",
			pageNumber, jobID, processingStatus.String)
		return false
	}

	// Update status to indicate we're about to queue this page
	_, err = qm.db.Exec(
		"UPDATE worker_results SET processing_status = 'queuing' WHERE job_id = $1 AND page_number = $2",
		jobID, pageNumber)
	if err != nil {
		log.Printf("Error updating page status to queuing: %v", err)
	}

	// Read the image file
	imageBytes, err := os.ReadFile(path)
	if err != nil {
		log.Printf("Failed to read image file %s: %v", path, err)
		// Update status to indicate the error
		_, dbErr := qm.db.Exec(
			"UPDATE worker_results SET processing_status = 'read_error' WHERE job_id = $1 AND page_number = $2",
			jobID, pageNumber)
		if dbErr != nil {
			log.Printf("Error updating page status: %v", dbErr)
		}
		return false
	}

	// Connect to RabbitMQ with retry mechanism
	var conn *amqp.Connection
	var ch *amqp.Channel
	var q amqp.Queue
	var connectErr error

	// Try to connect up to 3 times
	for attempts := 0; attempts < 3; attempts++ {
		conn, connectErr = amqp.Dial(config.GetRabbitMQURL())
		if connectErr == nil {
			break
		}
		log.Printf("Failed to connect to RabbitMQ (attempt %d): %v", attempts+1, connectErr)
		time.Sleep(100 * time.Millisecond)
	}

	if connectErr != nil {
		log.Printf("Failed to connect to RabbitMQ after retries: %v", connectErr)
		// Update status to indicate the error
		_, err = qm.db.Exec(
			"UPDATE worker_results SET processing_status = 'connection_error' WHERE job_id = $1 AND page_number = $2",
			jobID, pageNumber)
		if err != nil {
			log.Printf("Error updating page status: %v", err)
		}
		return false
	}
	defer conn.Close()

	ch, err = conn.Channel()
	if err != nil {
		log.Printf("Failed to open a channel: %v", err)
		// Update status to indicate the error
		_, dbErr := qm.db.Exec(
			"UPDATE worker_results SET processing_status = 'channel_error' WHERE job_id = $1 AND page_number = $2",
			jobID, pageNumber)
		if dbErr != nil {
			log.Printf("Error updating page status: %v", dbErr)
		}
		return false
	}
	defer ch.Close()

	q, err = ch.QueueDeclare(
		"ocr_queue", // name
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		log.Printf("Failed to declare a queue: %v", err)
		// Update status to indicate the error
		_, dbErr := qm.db.Exec(
			"UPDATE worker_results SET processing_status = 'queue_error' WHERE job_id = $1 AND page_number = $2",
			jobID, pageNumber)
		if dbErr != nil {
			log.Printf("Error updating page status: %v", dbErr)
		}
		return false
	}

	headers := amqp.Table{
		"job_id":      jobID,
		"page_number": pageNumber,
		"path":        path,
	}

	// Update status to queued before actually publishing
	_, err = qm.db.Exec(
		"UPDATE worker_results SET processing_status = 'queued' WHERE job_id = $1 AND page_number = $2",
		jobID, pageNumber)
	if err != nil {
		log.Printf("Error updating page status to queued: %v", err)
	}

	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			Headers:      headers,
			ContentType:  "application/octet-stream",
			Body:         imageBytes,
			DeliveryMode: amqp.Persistent,
		})
	if err != nil {
		log.Printf("Failed to publish a message: %v", err)
		// Update status to indicate the error
		_, dbErr := qm.db.Exec(
			"UPDATE worker_results SET processing_status = 'publish_error' WHERE job_id = $1 AND page_number = $2",
			jobID, pageNumber)
		if dbErr != nil {
			log.Printf("Error updating page status: %v", dbErr)
		}
		return false
	}

	log.Printf("Successfully sent page %d of job %s to OCR queue", pageNumber, jobID)

	// The processing_status will be updated to 'processing' by the worker
	// when it picks up the message from the queue

	return true
}

// SendToResultsQueue sends an existing OCR result directly to the results queue
func (qm *QueueManager) SendToResultsQueue(jobID string, pageNumber int, text string) bool {
	// İlk olarak, bu sayfa için zaten worker_results tablosunda bir kayıt var mı kontrol et
	var recordExists bool
	err := qm.db.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM worker_results 
			WHERE job_id = $1 AND page_number = $2
		)`,
		jobID, pageNumber).Scan(&recordExists)

	if err != nil {
		log.Printf("Failed to check if record exists: %v", err)
	}

	// Eğer kayıt yoksa, önce yeni bir kayıt oluştur
	// if !recordExists {
	// 	log.Printf("Creating new record for job %s page %d in worker_results", jobID, pageNumber)
	// 	_, err = qm.db.Exec(`
	// 		INSERT INTO worker_results (job_id, page_number, processed_at)
	// 		VALUES ($1, $2, CURRENT_TIMESTAMP)
	// 	`, jobID, pageNumber)

	// 	if err != nil {
	// 		log.Printf("Failed to create worker_results record: %v", err)
	// 		return false
	// 	}
	// }

	// Sonra, bu sayfa için zaten bir results queue mesajı gönderilip gönderilmediğini kontrol et
	var inQueue bool
	err = qm.db.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM worker_results 
			WHERE job_id = $1 AND page_number = $2 AND processing_status = 'results_queue'
		)`,
		jobID, pageNumber).Scan(&inQueue)

	if err != nil {
		log.Printf("Failed to check if page is already in results queue: %v", err)
	} else if inQueue {
		log.Printf("Page %d of job %s is already in results queue, skipping", pageNumber, jobID)
		return true // Zaten kuyruktaysa başarılı kabul et
	}

	// Get existing path and current status to ensure we don't lose information
	var existingPath sql.NullString
	var existingText sql.NullString
	var currentStatus sql.NullString
	err = qm.db.QueryRow(`
		SELECT path, text, processing_status FROM worker_results 
		WHERE job_id = $1 AND page_number = $2
	`, jobID, pageNumber).Scan(&existingPath, &existingText, &currentStatus)

	if err != nil && err != sql.ErrNoRows {
		log.Printf("Failed to get existing record info for job %s page %d: %v", jobID, pageNumber, err)
	}

	// Log the current state for debugging
	if existingPath.Valid && existingPath.String != "" {
		log.Printf("Path for job %s page %d: %s", jobID, pageNumber, existingPath.String)
	} else {
		log.Printf("Warning: No path found for job %s page %d", jobID, pageNumber)
	}

	if existingText.Valid && existingText.String != "" {
		log.Printf("Text already exists for job %s page %d", jobID, pageNumber)
	}

	if currentStatus.Valid {
		log.Printf("Current status for job %s page %d: %s", jobID, pageNumber, currentStatus.String)
	}

	conn, err := amqp.Dial(config.GetRabbitMQURL())
	if err != nil {
		log.Printf("Failed to connect to RabbitMQ: %v", err)
		return false
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Printf("Failed to open a channel: %v", err)
		return false
	}
	defer ch.Close()

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
		log.Printf("Failed to declare output queue: %v", err)
		return false
	}

	// Create result object
	type ProcessedResult struct {
		JobID      string `json:"job_id"`
		PageNumber int    `json:"page_number"`
		Text       string `json:"text"`
	}

	// Eğer yeni metin boş ama var olan metin doluysa, var olan metni kullan
	finalText := text
	if finalText == "" && existingText.Valid && existingText.String != "" {
		finalText = existingText.String
		log.Printf("Using existing text for job %s page %d", jobID, pageNumber)
	}

	if finalText == "" {
		log.Printf("No text available for job %s page %d, cannot send to results queue", jobID, pageNumber)
		return false
	}

	result := ProcessedResult{
		JobID:      jobID,
		PageNumber: pageNumber,
		Text:       finalText,
	}

	resultBytes, err := json.Marshal(result)
	if err != nil {
		log.Printf("Failed to marshal result for job %s page %d: %v", jobID, pageNumber, err)
		return false
	}

	// Önce mevcut kaydı logla
	log.Printf("Before update for job %s page %d: path=%v, text=%v, status=%v",
		jobID, pageNumber,
		existingPath.Valid && existingPath.String != "",
		existingText.Valid && existingText.String != "",
		currentStatus)

	// SQL sorgusunu ve argümanları hazırla
	var updateQuery string
	var queryArgs []interface{}

	// Var olan path'i koru (path varsa)
	if existingPath.Valid && existingPath.String != "" {
		// Path varsa, path'i koruyarak text ve status'u güncelle
		updateQuery = `
			UPDATE worker_results 
			SET text = $1, processing_status = 'results_queue', path = $2
			WHERE job_id = $3 AND page_number = $4
		`
		queryArgs = []interface{}{finalText, existingPath.String, jobID, pageNumber}
	} else {
		// Path yoksa, sadece text ve status'u güncelle
		updateQuery = `
			UPDATE worker_results 
			SET text = $1, processing_status = 'results_queue'
			WHERE job_id = $2 AND page_number = $3
		`
		queryArgs = []interface{}{finalText, jobID, pageNumber}
	}

	// Kaydı güncelle
	_, err = qm.db.Exec(updateQuery, queryArgs...)

	if err != nil {
		log.Printf("Failed to update processing status for job %s page %d: %v", jobID, pageNumber, err)
	} else {
		// Double-check that the update was successful
		var pathAfterUpdate sql.NullString
		var textAfterUpdate sql.NullString
		var statusAfterUpdate sql.NullString
		err = qm.db.QueryRow(`
			SELECT path, text, processing_status FROM worker_results 
			WHERE job_id = $1 AND page_number = $2
		`, jobID, pageNumber).Scan(&pathAfterUpdate, &textAfterUpdate, &statusAfterUpdate)

		if err == nil {
			log.Printf("Updated record for job %s page %d: path=%v, text=%v, status=%v",
				jobID, pageNumber,
				pathAfterUpdate.String,
				textAfterUpdate.String != "",
				statusAfterUpdate.String)
		}
	}

	err = ch.Publish(
		"",           // exchange
		outputQ.Name, // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			Headers: amqp.Table{
				"job_id":      jobID,
				"page_number": int32(pageNumber),
			},
			ContentType:  "application/json",
			Body:         resultBytes,
			DeliveryMode: amqp.Persistent,
		})
	if err != nil {
		log.Printf("Failed to publish result to output queue: %v", err)
		return false
	}

	log.Printf("Successfully sent existing result for job %s page %d directly to results queue",
		jobID, pageNumber)

	// Update the completed tasks count for this job
	_, err = qm.db.Exec(
		"UPDATE jobs SET completed_tasks = completed_tasks + 1 WHERE job_id = $1 AND "+
			"(SELECT COUNT(*) FROM worker_results WHERE job_id = $1 AND text IS NOT NULL) > completed_tasks",
		jobID)
	if err != nil {
		log.Printf("Failed to update job completion count for %s: %v", jobID, err)
	}

	// Check if all pages are completed for this job and update status if needed
	qm.checkAndUpdateJobCompletion(jobID)

	return true
}

// SendJobPageToResultsQueue attempts to send a job page to the results queue
// by first retrieving the text from the database
func (qm *QueueManager) SendJobPageToResultsQueue(jobID string, pageNumber int) bool {
	// First check if this page is already in the results queue
	var inResultsQueue bool
	err := qm.db.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM worker_results 
			WHERE job_id = $1 AND page_number = $2 AND processing_status = 'results_queue'
		)`,
		jobID, pageNumber).Scan(&inResultsQueue)

	if err != nil {
		log.Printf("Failed to check if page is already in results queue: %v", err)
	} else if inResultsQueue {
		log.Printf("Page %d of job %s is already in results queue, skipping", pageNumber, jobID)
		return true // Already in the queue is considered success
	}

	// Önce bu sayfa için worker_results tablosunda kayıt olup olmadığını kontrol et
	var workerResultsExists bool
	err = qm.db.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM worker_results 
			WHERE job_id = $1 AND page_number = $2
		)
	`, jobID, pageNumber).Scan(&workerResultsExists)

	if err != nil {
		log.Printf("Failed to check worker_results existence for job %s page %d: %v", jobID, pageNumber, err)
		return false
	}

	// if !workerResultsExists {
	// 	log.Printf("No worker_results record found for job %s page %d, creating one", jobID, pageNumber)
	// 	_, err = qm.db.Exec(`
	// 		INSERT INTO worker_results (job_id, page_number, processed_at)
	// 		VALUES ($1, $2, CURRENT_TIMESTAMP)
	// 	`, jobID, pageNumber)

	// 	if err != nil {
	// 		log.Printf("Failed to create worker_results record: %v", err)
	// 		return false
	// 	}
	// }

	// Next, check if this page is currently in the OCR queue
	var inOcrQueue bool
	var processingStatus sql.NullString
	err = qm.db.QueryRow(`
		SELECT processing_status
		FROM worker_results
		WHERE job_id = $1 AND page_number = $2
	`, jobID, pageNumber).Scan(&processingStatus)

	if err == nil && processingStatus.Valid {
		inOcrQueue = processingStatus.String == "processing" ||
			processingStatus.String == "queued" ||
			processingStatus.String == "queuing"
	}

	if inOcrQueue {
		log.Printf("Page %d of job %s is currently in OCR queue (status: %s), cannot send to results queue",
			pageNumber, jobID, processingStatus.String)
		return false
	}

	// Get the text and path for the page
	var text sql.NullString
	var path sql.NullString
	err = qm.db.QueryRow("SELECT text, path FROM worker_results WHERE job_id = $1 AND page_number = $2",
		jobID, pageNumber).Scan(&text, &path)

	if err != nil {
		log.Printf("Failed to get text and path for job %s page %d: %v", jobID, pageNumber, err)
		return false
	}

	// Log existing values for debugging
	if path.Valid && path.String != "" {
		log.Printf("Found path for job %s page %d: %s", jobID, pageNumber, path.String)
	} else {
		log.Printf("Warning: No path found for job %s page %d", jobID, pageNumber)
	}

	// Ensure we have valid text to send
	if !text.Valid || text.String == "" {
		log.Printf("No text found for job %s page %d, cannot send to results queue", jobID, pageNumber)
		return false
	}

	log.Printf("Found existing text for job %s page %d with length: %d", jobID, pageNumber, len(text.String))

	// Send to results queue
	return qm.SendToResultsQueue(jobID, pageNumber, text.String)
}

// SendAllJobPagesToResultsQueue sends all pages of a job to the results queue
func (qm *QueueManager) SendAllJobPagesToResultsQueue(jobID string) (bool, int) {
	// İlk olarak, iş için worker_results kayıtlarının durumunu logla
	rows, err := qm.db.Query(`
		SELECT page_number, path, text, processing_status 
		FROM worker_results 
		WHERE job_id = $1
	`, jobID)

	if err != nil {
		log.Printf("Failed to query current worker_results for job %s: %v", jobID, err)
	} else {
		log.Printf("Current worker_results status for job %s:", jobID)
		for rows.Next() {
			var pageNumber int
			var path, text, status sql.NullString
			if err := rows.Scan(&pageNumber, &path, &text, &status); err != nil {
				log.Printf("  Error scanning row: %v", err)
				continue
			}
			log.Printf("  Page %d: path=%v, text=%v, status=%v",
				pageNumber,
				path.Valid && path.String != "",
				text.Valid && text.String != "",
				status)
		}
		rows.Close()
	}

	// Get all pages with text for this job that are not already in the results queue
	// and also not currently in the OCR queue
	rows, err = qm.db.Query(`
		SELECT page_number, text 
		FROM worker_results 
		WHERE job_id = $1 
		  AND text IS NOT NULL 
		  AND text != ''
		  AND (processing_status IS NULL OR 
		       processing_status != 'results_queue') 
		  AND (processing_status IS NULL OR 
		       processing_status NOT IN ('processing', 'queued', 'queuing'))
		ORDER BY page_number
	`, jobID)
	if err != nil {
		log.Printf("Failed to query pages for job %s: %v", jobID, err)
		return false, 0
	}
	defer rows.Close()

	sentCount := 0
	for rows.Next() {
		var pageNumber int
		var text string
		if err := rows.Scan(&pageNumber, &text); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		log.Printf("Sending page %d of job %s to results queue with text length: %d",
			pageNumber, jobID, len(text))

		if qm.SendToResultsQueue(jobID, pageNumber, text) {
			sentCount++
		}
	}

	if sentCount > 0 {
		log.Printf("Sent %d pages to results queue for job %s", sentCount, jobID)
		return true, sentCount
	}

	log.Printf("No pages sent to results queue for job %s", jobID)
	return false, 0
}

// checkAndUpdateJobCompletion checks if all pages of a job have been processed
// and updates the job status to "completed" if so
func (qm *QueueManager) checkAndUpdateJobCompletion(jobID string) {
	var totalTasks, completedTasks int
	var status string

	err := qm.db.QueryRow(
		"SELECT total_tasks, completed_tasks, status FROM jobs WHERE job_id = $1",
		jobID).Scan(&totalTasks, &completedTasks, &status)

	if err != nil {
		log.Printf("Error checking job completion status for %s: %v", jobID, err)
		return
	}

	// If all tasks are completed but status isn't "completed" yet, update it
	if completedTasks >= totalTasks && status != "completed" {
		_, err := qm.db.Exec(
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

// PurgeQueue purges the RabbitMQ queue without deleting files or database records
func (qm *QueueManager) PurgeQueue() (bool, interface{}) {
	qm.purgeMutex.Lock()
	*qm.purgeInProgress = true
	qm.purgeMutex.Unlock()

	// Give processing routines time to notice the flag
	time.Sleep(1 * time.Second)

	conn, err := amqp.Dial(config.GetRabbitMQURL())
	if err != nil {
		qm.purgeMutex.Lock()
		*qm.purgeInProgress = false
		qm.purgeMutex.Unlock()
		return false, err.Error()
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		qm.purgeMutex.Lock()
		*qm.purgeInProgress = false
		qm.purgeMutex.Unlock()
		return false, err.Error()
	}
	defer ch.Close()

	// Purge the queue - this only removes messages from RabbitMQ, not files or database records
	purgeCount, err := ch.QueuePurge("ocr_queue", false)
	if err != nil {
		qm.purgeMutex.Lock()
		*qm.purgeInProgress = false
		qm.purgeMutex.Unlock()
		return false, err.Error()
	}

	// Update database records for pages that were in the OCR queue
	log.Println("Updating database records for cleared queue pages...")
	result, err := qm.db.Exec(`
		UPDATE worker_results 
		SET processing_status = NULL 
		WHERE processing_status IN ('queued', 'queuing', 'processing')
	`)
	if err != nil {
		log.Printf("Failed to update database records: %v", err)
	} else {
		rowsAffected, _ := result.RowsAffected()
		log.Printf("Updated processing status for %d records from OCR queue statuses to NULL", rowsAffected)
	}

	// Check queue status after purge
	queue, err := ch.QueueDeclare(
		"ocr_queue", // name
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)

	qm.purgeMutex.Lock()
	*qm.purgeInProgress = false
	qm.purgeMutex.Unlock()

	if err != nil {
		return true, purgeCount
	}

	return true, queue.Messages
}

// PurgeResultsQueue purges the OCR results queue
func (qm *QueueManager) PurgeResultsQueue() (bool, interface{}) {
	log.Println("Starting PurgeResultsQueue operation...")

	qm.purgeMutex.Lock()
	*qm.purgeInProgress = true
	qm.purgeMutex.Unlock()

	// Give processing routines time to notice the flag
	time.Sleep(1 * time.Second)

	// Log the RabbitMQ URL (without sensitive info)
	rabbitURL := config.GetRabbitMQURL()
	urlParts := strings.Split(rabbitURL, "@")
	maskedURL := "amqp://***:***@"
	if len(urlParts) > 1 {
		maskedURL += urlParts[1]
	}
	log.Printf("Connecting to RabbitMQ at %s...", maskedURL)

	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Printf("Failed to connect to RabbitMQ: %v", err)
		qm.purgeMutex.Lock()
		*qm.purgeInProgress = false
		qm.purgeMutex.Unlock()
		return false, fmt.Sprintf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	log.Println("Successfully connected to RabbitMQ")

	log.Println("Opening channel to RabbitMQ...")
	ch, err := conn.Channel()
	if err != nil {
		log.Printf("Failed to open channel: %v", err)
		qm.purgeMutex.Lock()
		*qm.purgeInProgress = false
		qm.purgeMutex.Unlock()
		return false, fmt.Sprintf("Failed to open channel: %v", err)
	}
	defer ch.Close()
	log.Println("Channel opened successfully")

	// Check if queue exists first
	log.Println("Checking if ocr_results queue exists...")
	queue, err := ch.QueueInspect("ocr_results")
	if err != nil {
		log.Printf("Failed to inspect ocr_results queue: %v", err)

		// Try to declare the queue if it doesn't exist
		log.Println("Attempting to declare ocr_results queue...")
		_, err = ch.QueueDeclare(
			"ocr_results", // name
			true,          // durable
			false,         // delete when unused
			false,         // exclusive
			false,         // no-wait
			nil,           // arguments
		)

		if err != nil {
			log.Printf("Failed to declare queue: %v", err)
			qm.purgeMutex.Lock()
			*qm.purgeInProgress = false
			qm.purgeMutex.Unlock()
			return false, fmt.Sprintf("Queue does not exist and could not be created: %v", err)
		}

		log.Println("Queue declared successfully")
		queue, _ = ch.QueueInspect("ocr_results")
	}
	log.Printf("Queue exists with %d messages", queue.Messages)

	// Purge the results queue
	log.Println("Purging ocr_results queue...")
	purgeCount, err := ch.QueuePurge("ocr_results", false)
	if err != nil {
		log.Printf("Failed to purge queue: %v", err)
		qm.purgeMutex.Lock()
		*qm.purgeInProgress = false
		qm.purgeMutex.Unlock()
		return false, fmt.Sprintf("Failed to purge queue: %v", err)
	}
	log.Printf("Purged %d messages from queue", purgeCount)

	// Verify the queue is empty after purging
	queue, err = ch.QueueInspect("ocr_results")
	if err != nil {
		log.Printf("Failed to inspect queue after purge: %v", err)
		qm.purgeMutex.Lock()
		*qm.purgeInProgress = false
		qm.purgeMutex.Unlock()
		return false, fmt.Sprintf("Failed to verify queue is empty: %v", err)
	}

	// Update database records for pages that were in the results queue
	log.Println("Updating database records for cleared queue pages...")
	result, err := qm.db.Exec(`
		UPDATE worker_results 
		SET processing_status = 'processed' 
		WHERE processing_status = 'results_queue'
	`)
	if err != nil {
		log.Printf("Failed to update database records: %v", err)
	} else {
		rowsAffected, _ := result.RowsAffected()
		log.Printf("Updated processing status for %d records from 'results_queue' to 'processed'", rowsAffected)
	}

	qm.purgeMutex.Lock()
	*qm.purgeInProgress = false
	qm.purgeMutex.Unlock()

	log.Printf("Successfully purged OCR results queue, removed %d messages, queue now has %d messages", purgeCount, queue.Messages)
	return true, purgeCount
}
