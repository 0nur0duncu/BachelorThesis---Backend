package handlers

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/BachelorThesis/Backend/config"
	"github.com/gin-gonic/gin"
	"github.com/google/generative-ai-go/genai"
	openai "github.com/openai/openai-go"
	openaiOption "github.com/openai/openai-go/option"
	shared "github.com/openai/openai-go/shared"
	"github.com/streadway/amqp"
	"github.com/xuri/excelize/v2"
	"google.golang.org/api/option"
)

// DatasetEntry represents a single entry in the dataset
type DatasetEntry struct {
	Input  string `json:"input"`
	Output string `json:"output"`
	Chunk  string `json:"chunk"`
}

// TokenUsage tracks token usage for Gemini API calls
type TokenUsage struct {
	JobID           string
	PageNumber      int
	PromptTokens    int32
	ResponseTokens  int32
	TotalTokens     int32
	EstimatedTokens int32 // From CountTokens call
	Timestamp       time.Time
	Provider        string
	ModelName       string
}

// DatasetEntryFormatted defines the format for dataset exports
type DatasetEntryFormatted struct {
	Chunk     string    `json:"chunk"`
	CreatedAt time.Time `json:"created_at"`
	ID        int       `json:"id"`
	Input     string    `json:"input"`
	Output    string    `json:"output"`
}

// DatasetHandlers contains handlers for dataset generation operations
type DatasetHandlers struct {
	db              *sql.DB
	purgeMutex      *sync.Mutex
	queueMutex      *sync.Mutex
	purgeInProgress *bool
	queuePaused     *bool
	tokenUsage      []TokenUsage // Track token usage across requests
	tokenMutex      sync.Mutex   // Mutex for concurrent token usage updates
}

// NewDatasetHandlers creates a new DatasetHandlers instance
func NewDatasetHandlers(db *sql.DB, purgeMutex *sync.Mutex, queueMutex *sync.Mutex,
	purgeInProgress *bool, queuePaused *bool) *DatasetHandlers {
	return &DatasetHandlers{
		db:              db,
		purgeMutex:      purgeMutex,
		queueMutex:      queueMutex,
		purgeInProgress: purgeInProgress,
		queuePaused:     queuePaused,
		tokenUsage:      make([]TokenUsage, 0),
	}
}

// StartDatasetGeneration starts generating dataset from all jobs in ocr_results queue
func (h *DatasetHandlers) StartDatasetGeneration(c *gin.Context) {
	log.Println("StartDatasetGeneration called - Generating datasets for current ocr_results queue snapshot")

	// Ensure database table exists
	if err := h.ensureDatasetTable(); err != nil {
		log.Printf("Failed to ensure dataset table: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("Failed to ensure dataset table: %v", err),
		})
		return
	}

	// Ensure token usage table exists
	if err := h.ensureTokenUsageTable(); err != nil {
		log.Printf("Failed to ensure token usage table: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("Failed to ensure token usage table: %v", err),
		})
		return
	}

	// Get worker count from settings
	var workerCount int
	err := h.db.QueryRow("SELECT dataset_worker_count FROM settings WHERE id = 1").Scan(&workerCount)
	if err != nil {
		log.Printf("Failed to get dataset worker count from settings: %v. Using default value of 1.", err)
		workerCount = 1
	}

	if workerCount < 1 {
		workerCount = 1
	}

	log.Printf("Starting dataset generation with %d workers", workerCount)

	// Start processing with multiple workers
	go h.processAllJobsWithWorkers(workerCount)

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": fmt.Sprintf("Dataset generation started with %d workers", workerCount),
	})
}

// StartJobDatasetGeneration starts generating dataset for a specific job
func (h *DatasetHandlers) StartJobDatasetGeneration(c *gin.Context) {
	jobID := c.Param("jobID")
	log.Printf("StartJobDatasetGeneration called for job ID: %s", jobID)

	// Check if job exists
	var exists bool
	err := h.db.QueryRow("SELECT EXISTS(SELECT 1 FROM jobs WHERE job_id = $1)", jobID).Scan(&exists)
	if err != nil {
		log.Printf("Database error checking job existence: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("Database error: %v", err),
		})
		return
	}

	if !exists {
		log.Printf("Job not found: %s", jobID)
		c.JSON(http.StatusNotFound, gin.H{
			"status":  "error",
			"message": "Job not found",
		})
		return
	}

	// Ensure database tables exist
	if err := h.ensureDatasetTable(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("Failed to ensure dataset table: %v", err),
		})
		return
	}

	if err := h.ensureTokenUsageTable(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("Failed to ensure token usage table: %v", err),
		})
		return
	}

	// Get worker count from settings
	var workerCount int
	err = h.db.QueryRow("SELECT dataset_worker_count FROM settings WHERE id = 1").Scan(&workerCount)
	if err != nil {
		log.Printf("Failed to get dataset worker count from settings: %v. Using default value of 1.", err)
		workerCount = 1
	}

	if workerCount < 1 {
		workerCount = 1
	}

	log.Printf("Starting dataset generation for job %s with %d workers", jobID, workerCount)

	// Start processing with multiple workers for this specific job
	go h.processJobDatasetWithWorkers(jobID, workerCount)

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": fmt.Sprintf("Dataset generation started for job %s with %d workers", jobID, workerCount),
	})
}

// DeleteAllDatasets deletes all datasets
func (h *DatasetHandlers) DeleteAllDatasets(c *gin.Context) {
	_, err := h.db.Exec("DELETE FROM dataset")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("Failed to delete datasets: %v", err),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "All datasets deleted",
	})
}

// DeleteJobDataset deletes dataset for a specific job
func (h *DatasetHandlers) DeleteJobDataset(c *gin.Context) {
	jobID := c.Param("jobID")

	result, err := h.db.Exec("DELETE FROM dataset WHERE job_id = $1", jobID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("Failed to delete job dataset: %v", err),
		})
		return
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{
			"status":  "error",
			"message": "No dataset found for this job",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": fmt.Sprintf("Dataset for job %s deleted", jobID),
		"count":   rowsAffected,
	})
}

// ensureDatasetTable creates the dataset table if it doesn't exist
func (h *DatasetHandlers) ensureDatasetTable() error {
	_, err := h.db.Exec(`
		CREATE TABLE IF NOT EXISTS dataset (
			id SERIAL PRIMARY KEY,
			job_id TEXT NOT NULL,
			page_number INTEGER NOT NULL,
			input TEXT NOT NULL,
			output TEXT NOT NULL,
			chunk TEXT NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			CONSTRAINT fk_job FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
		)
	`)
	return err
}

// ensureTokenUsageTable creates the token_usage table if it doesn't exist
func (h *DatasetHandlers) ensureTokenUsageTable() error {
	_, err := h.db.Exec(`
		CREATE TABLE IF NOT EXISTS token_usage (
			id SERIAL PRIMARY KEY,
			job_id VARCHAR(36) NOT NULL,
			page_number INTEGER NOT NULL,
			prompt_tokens INTEGER NOT NULL,
			response_tokens INTEGER NOT NULL,
			total_tokens INTEGER NOT NULL,
			estimated_tokens INTEGER NOT NULL,
			provider TEXT,
			model_name TEXT, 
			timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			CONSTRAINT fk_job_token FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
		)
	`)
	return err
}

// processAllJobs processes all jobs to generate datasets
func (h *DatasetHandlers) processAllJobs() {
	// Get worker count from settings
	var workerCount int
	err := h.db.QueryRow("SELECT dataset_worker_count FROM settings WHERE id = 1").Scan(&workerCount)
	if err != nil {
		log.Printf("Failed to get dataset worker count from settings: %v. Using default value of 1.", err)
		workerCount = 1
	}

	if workerCount < 1 {
		workerCount = 1
	}

	log.Printf("Starting dataset generation for all jobs with %d workers", workerCount)

	// Start parallel processing with multiple workers
	h.processAllJobsWithWorkers(workerCount)
}

// processAllJobsWithWorkers processes all jobs using multiple workers
func (h *DatasetHandlers) processAllJobsWithWorkers(workerCount int) {
	log.Printf("Starting dataset generation with %d workers", workerCount)

	// Connect to RabbitMQ
	conn, err := amqp.Dial(config.GetRabbitMQURL())
	if err != nil {
		log.Printf("Failed to connect to RabbitMQ: %v", err)
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Printf("Failed to open channel: %v", err)
		return
	}
	defer ch.Close()

	// Declare the ocr_results queue
	resultsQueue, err := ch.QueueDeclare(
		"ocr_results", // name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		log.Printf("Failed to declare queue: %v", err)
		return
	}

	// Check if there are any messages in the queue
	queueInfo, err := ch.QueueInspect(resultsQueue.Name)
	if err != nil {
		log.Printf("Failed to inspect queue: %v", err)
		return
	}

	// If queue is empty, no need to start workers
	if queueInfo.Messages == 0 {
		log.Printf("Queue is empty, no jobs to process")
		return
	}

	log.Printf("Queue has %d messages to process", queueInfo.Messages)

	// Start workers using a WaitGroup to wait for all workers to complete
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			h.datasetWorker(workerID)
		}(i)
	}

	// Wait for all workers to complete
	wg.Wait()
	log.Println("All dataset workers have completed processing")
}

// processJobDataset processes a specific job to generate dataset
func (h *DatasetHandlers) processJobDataset(jobID string) {
	// Connect to RabbitMQ
	conn, err := amqp.Dial(config.GetRabbitMQURL())
	if err != nil {
		log.Printf("Failed to connect to RabbitMQ: %v", err)
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Printf("Failed to open channel: %v", err)
		return
	}
	defer ch.Close()

	// Declare the ocr_results queue
	resultsQueue, err := ch.QueueDeclare(
		"ocr_results", // name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		log.Printf("Failed to declare queue: %v", err)
		return
	}

	// Initialize AI client based on selected provider and model
	ctx := context.Background()
	var provider, apiKey, modelName string
	if err := h.db.QueryRow(
		"SELECT model_provider, model_api_key, model_name FROM settings WHERE id = 1",
	).Scan(&provider, &apiKey, &modelName); err != nil {
		log.Printf("Error loading model settings: %v, defaulting to Gemini", err)
		provider = "gemini"
		apiKey = os.Getenv("GEMINI_API_KEY")
		modelName = "gemini-2.5-pro-preview-03-25"
	}
	if provider != "gemini" {
		log.Printf("Provider %s not supported, using Gemini", provider)
		provider = "gemini"
	}
	client, err := genai.NewClient(ctx, option.WithAPIKey(apiKey))
	if err != nil {
		log.Printf("Failed to create AI client: %v", err)
		return
	}
	defer client.Close()
	// Configure model from settings
	model := client.GenerativeModel(modelName)
	model.ResponseMIMEType = "application/json"

	// Define the dataset entry schema
	model.ResponseSchema = &genai.Schema{
		Type: genai.TypeArray,
		Items: &genai.Schema{
			Type: genai.TypeObject,
			Properties: map[string]*genai.Schema{
				"input":  {Type: genai.TypeString},
				"output": {Type: genai.TypeString},
				"chunk":  {Type: genai.TypeString},
			},
			Required: []string{"input", "output", "chunk"},
		},
	}

	// System instruction for Gemini
	model.SystemInstruction = &genai.Content{
		Parts: []genai.Part{genai.Text(`
			Sen OCR metin işleme için bir veri seti oluşturucusun. Sana vereceğim OCR metninden, dil modelini eğitmek için kullanılabilecek anlamlı girdi ve çıktı çiftleri çıkaracaksın. Her bir çift, 'input', 'output' ve 'chunk' alanlarını içermelidir. 'Input' alanı, OCR metninden türetilen ve dil modeli tarafından cevaplanabilecek bir soru veya talimat olmalıdır. 'Output' alanı, bu girdiye verilmesi beklenen doğru yanıt olmalıdır. 'Chunk' alanı, bu çiftin türetildiği anlamlı (sematik) bir OCR metin segmentini içermelidir. OCR çıktı metnindeki anlamsız karakterler kullanılmamalıdır. Her bir çift, OCR metninin farklı yönlerini ele almalı ve tekrarlamamalıdır. 'Input' ve 'output' çiftleri, basit soru-cevap yerine daha karmaşık ve ayrıntılı sorgular ve yanıtlar içermelidir. Veri seti, metnin tüm detaylarını ve bağlamlarını kapsayacak kadar çeşitli ve uzun olmalı. Verileri, her biri 'input', 'output' ve 'chunk' alanlarına sahip birden fazla giriş içeren bir JSON dizisi olarak döndüreceksin. Lütfen OCR metnindeki tüm detayları ve bağlamları kapsayacak şekilde veri setini oluştur. Ürettiğin her 'input' ve 'output' çifti yeterince uzun ve kapsayıcı olmalı, kısa olmamalıdır. Veri setindeki her örnek, OCR metninin farklı yönlerini veya senaryolarını kapsamalı ve çeşitlilik göstermelidir. Yaratıcı ol, ancak verilerin yüksek kaliteli olduğundan emin ol. Ürettiğin veri setinde asla orijinal OCR metnine atıfta bulunulmamalıdır; her şey bağımsız ve kendinden yeterli olmalıdır. Her zaman belirtilen şemaya sıkı sıkıya uyan geçerli JSON döndür.
			---
			OCR çıktı metni:
			---
		`)},
	}

	// Get messages for this job from the queue
	msgs, err := ch.Consume(
		resultsQueue.Name, // queue
		"",                // consumer
		false,             // auto-ack (we'll manually ack)
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // arguments
	)
	if err != nil {
		log.Printf("Failed to register a consumer: %v", err)
		return
	}

	// Process messages from the channel
	for msg := range msgs {
		// Check if message belongs to our job
		msgJobID, ok := msg.Headers["job_id"].(string)
		if !ok || msgJobID != jobID {
			// This message is for a different job, put it back in the queue
			msg.Nack(false, true)
			continue
		}

		// Extract page number
		pageNumberVal, ok := msg.Headers["page_number"]
		if !ok {
			log.Printf("Missing page_number in message headers")
			msg.Nack(false, false) // Don't requeue, this message is broken
			continue
		}

		// Convert page number to int
		var pageNumber int
		switch v := pageNumberVal.(type) {
		case int32:
			pageNumber = int(v)
		case int:
			pageNumber = v
		case int64:
			pageNumber = int(v)
		case float64:
			pageNumber = int(v)
		default:
			log.Printf("Invalid page_number type: %T", pageNumberVal)
			msg.Nack(false, false)
			continue
		}

		// Parse the message body
		var result struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(msg.Body, &result); err != nil {
			log.Printf("Error unmarshaling message: %v", err)
			msg.Nack(false, false) // Don't requeue
			continue
		}

		if len(result.Text) < 50 {
			log.Printf("Text for job %s page %d is too short, skipping", jobID, pageNumber)
			// Acknowledge message even though we're skipping it - we've evaluated it
			msg.Ack(false)
			continue
		}

		// Generate dataset entries using Gemini with retry mechanism and token tracking
		datasetEntries, tokenUsage, err := h.generateDatasetWithRetry(ctx, model, result.Text, jobID, pageNumber)
		if err != nil {
			log.Printf("Failed to generate dataset for job %s page %d after retries: %v", jobID, pageNumber, err)
			// We tried our best, ack the message anyway to remove it from queue
			msg.Ack(false)
			continue
		}

		// Store token usage in database
		if tokenUsage != nil {
			_, err := h.db.Exec(`
				INSERT INTO token_usage (job_id, page_number, prompt_tokens, response_tokens, total_tokens, estimated_tokens, provider, model_name)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			`, tokenUsage.JobID, tokenUsage.PageNumber, tokenUsage.PromptTokens,
				tokenUsage.ResponseTokens, tokenUsage.TotalTokens, tokenUsage.EstimatedTokens,
				tokenUsage.Provider, tokenUsage.ModelName)

			if err != nil {
				log.Printf("Error storing token usage for job %s page %d: %v", jobID, pageNumber, err)
			}
		}

		// Store dataset entries in database
		for _, entry := range datasetEntries {
			_, err := h.db.Exec(`
				INSERT INTO dataset (job_id, page_number, input, output, chunk)
				VALUES ($1, $2, $3, $4, $5)
			`, jobID, pageNumber, entry.Input, entry.Output, entry.Chunk)
			if err != nil {
				log.Printf("Error storing dataset entry for job %s page %d: %v", jobID, pageNumber, err)
			}
		}

		log.Printf("Generated %d dataset entries for job %s page %d", len(datasetEntries), jobID, pageNumber)

		// Acknowledge the message to remove it from the queue
		msg.Ack(false)
	}
}

// generateDatasetWithRetry attempts to generate a dataset with retries if JSON parsing fails
func (h *DatasetHandlers) generateDatasetWithRetry(ctx context.Context, model *genai.GenerativeModel, text string, jobID string, pageNumber int) ([]DatasetEntry, *TokenUsage, error) {
	maxRetries := 3
	retryDelay := 2 * time.Second

	// Fetch the prompt template from settings
	var promptTemplate string
	err := h.db.QueryRow("SELECT dataset_generation_prompt FROM settings WHERE id = 1").Scan(&promptTemplate)
	if err != nil {
		log.Printf("Error fetching dataset generation prompt from settings: %v, using default prompt", err)
		promptTemplate = "Generate a synthetic dataset from this OCR text:\n\n{{text}}"
	}

	// Replace placeholder with actual text
	promptTemplate = strings.Replace(promptTemplate, "{{text}}", text, -1)

	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Generate dataset entries using Gemini
		prompt := promptTemplate
		if attempt > 1 {
			prompt += "\n\nIMPORTANT: Please ensure your response is valid JSON that strictly follows the required schema. Previous attempts failed to parse the JSON."
		}

		// Estimate token count before sending
		promptContent := genai.Text(prompt)
		tokenCountResp, err := model.CountTokens(ctx, promptContent)
		var estimatedTokens int32 = 0
		if err != nil {
			log.Printf("Warning: Failed to count tokens for job %s page %d: %v", jobID, pageNumber, err)
		} else {
			estimatedTokens = tokenCountResp.TotalTokens
			log.Printf("Estimated token count for job %s page %d: %d tokens", jobID, pageNumber, estimatedTokens)
		}

		// Generate content
		resp, err := model.GenerateContent(ctx, promptContent)
		if err != nil {
			log.Printf("Error generating dataset for job %s page %d (attempt %d/%d): %v",
				jobID, pageNumber, attempt, maxRetries, err)
			time.Sleep(retryDelay)
			continue
		}

		// Track token usage from response
		tokenUsage := &TokenUsage{
			JobID:           jobID,
			PageNumber:      pageNumber,
			EstimatedTokens: estimatedTokens,
			Timestamp:       time.Now(),
		}

		// Extract token usage metadata if available
		if resp.UsageMetadata != nil {
			tokenUsage.PromptTokens = resp.UsageMetadata.PromptTokenCount
			tokenUsage.ResponseTokens = resp.UsageMetadata.CandidatesTokenCount
			tokenUsage.TotalTokens = resp.UsageMetadata.TotalTokenCount

			// Add provider and model information
			tokenUsage.Provider = "gemini"
			// Get the model name from the Gemini model
			var settingsModelName string
			err := h.db.QueryRow("SELECT model_name FROM settings WHERE id = 1").Scan(&settingsModelName)
			if err == nil && settingsModelName != "" {
				tokenUsage.ModelName = settingsModelName
			} else {
				tokenUsage.ModelName = "gemini-model" // Fallback if can't get from settings
			}

			log.Printf("Token usage for job %s page %d: prompt=%d, response=%d, total=%d",
				jobID, pageNumber, tokenUsage.PromptTokens, tokenUsage.ResponseTokens, tokenUsage.TotalTokens)
		} else {
			log.Printf("No token usage metadata available for job %s page %d", jobID, pageNumber)
		}

		// Store token usage in memory
		h.tokenMutex.Lock()
		h.tokenUsage = append(h.tokenUsage, *tokenUsage)
		h.tokenMutex.Unlock()

		// Parse response
		datasetEntries, err := h.parseGeminiResponse(resp)
		if err != nil {
			log.Printf("Error parsing Gemini response for job %s page %d (attempt %d/%d): %v",
				jobID, pageNumber, attempt, maxRetries, err)

			// Log the raw response to help diagnose issues
			for _, candidate := range resp.Candidates {
				for _, part := range candidate.Content.Parts {
					if txt, ok := part.(genai.Text); ok {
						log.Printf("Raw response (first 200 chars): %s", txt[:min(200, len(txt))])
					}
				}
			}

			// Wait before retrying
			time.Sleep(retryDelay)
			continue
		}

		// If we got here, we successfully parsed the response
		return datasetEntries, tokenUsage, nil
	}

	return nil, nil, fmt.Errorf("failed to generate dataset after %d attempts", maxRetries)
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// parseGeminiResponse parses the Gemini model response into dataset entries
func (h *DatasetHandlers) parseGeminiResponse(resp *genai.GenerateContentResponse) ([]DatasetEntry, error) {
	if resp == nil || len(resp.Candidates) == 0 || resp.Candidates[0].Content == nil {
		return nil, fmt.Errorf("empty response from Gemini")
	}

	// Extract the text content from the response
	var textContent string
	for _, part := range resp.Candidates[0].Content.Parts {
		if txt, ok := part.(genai.Text); ok {
			textContent = string(txt)
			break
		}
	}

	if textContent == "" {
		return nil, fmt.Errorf("no text content in Gemini response")
	}

	// Clean up JSON content - sometimes the model adds markdown code blocks
	textContent = cleanJSONString(textContent)

	// Parse the JSON response
	var entries []DatasetEntry
	if err := json.Unmarshal([]byte(textContent), &entries); err != nil {
		return nil, fmt.Errorf("failed to parse JSON response: %v", err)
	}

	// Validate entries
	var validEntries []DatasetEntry
	for _, entry := range entries {
		if entry.Input != "" && entry.Output != "" {
			validEntries = append(validEntries, entry)
		}
	}

	if len(validEntries) == 0 {
		return nil, fmt.Errorf("no valid entries found in response")
	}

	return validEntries, nil
}

// GetTokenUsageStats returns token usage statistics for a handler
func (h *DatasetHandlers) GetTokenUsageStats(c *gin.Context) {
	// Query from database
	rows, err := h.db.Query(`
		SELECT 
			t.job_id, 
			j.filename,
			COUNT(*) as request_count,
			SUM(prompt_tokens) as total_prompt_tokens,
			SUM(response_tokens) as total_response_tokens,
			SUM(total_tokens) as grand_total_tokens,
			AVG(total_tokens) as avg_tokens_per_request,
			provider,
			model_name
		FROM token_usage t
		JOIN jobs j ON t.job_id = j.job_id
		GROUP BY t.job_id, j.filename, provider, model_name
		ORDER BY grand_total_tokens DESC
	`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("Failed to query token usage: %v", err),
		})
		return
	}
	defer rows.Close()

	var stats []gin.H
	var totalRequests int64
	var totalPromptTokens, totalResponseTokens, grandTotalTokens int64

	for rows.Next() {
		var jobID, filename string
		var requestCount, promptTokens, responseTokens, tokenTotal int64
		var avgTokens float64
		var provider, modelName string

		if err := rows.Scan(&jobID, &filename, &requestCount, &promptTokens, &responseTokens, &tokenTotal, &avgTokens, &provider, &modelName); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"status":  "error",
				"message": fmt.Sprintf("Error scanning results: %v", err),
			})
			return
		}

		totalRequests += requestCount
		totalPromptTokens += promptTokens
		totalResponseTokens += responseTokens
		grandTotalTokens += tokenTotal

		stats = append(stats, gin.H{
			"job_id":                 jobID,
			"filename":               filename,
			"requests":               requestCount,
			"prompt_tokens":          promptTokens,
			"response_tokens":        responseTokens,
			"total_tokens":           tokenTotal,
			"avg_tokens_per_request": avgTokens,
			"provider":               provider,
			"model_name":             modelName,
		})
	}

	// Return the statistics
	c.JSON(http.StatusOK, gin.H{
		"status":    "success",
		"job_stats": stats,
		"summary": gin.H{
			"total_requests":        totalRequests,
			"total_prompt_tokens":   totalPromptTokens,
			"total_response_tokens": totalResponseTokens,
			"grand_total_tokens":    grandTotalTokens,
			"estimated_cost_usd":    fmt.Sprintf("$%.4f", float64(grandTotalTokens)*0.000003), // Example rate: $0.000003 per token
		},
	})
}

// GetJobPageDataset retrieves dataset entries for a specific job and page number
func (h *DatasetHandlers) GetJobPageDataset(c *gin.Context) {
	jobID := c.Param("jobID")
	pageNumberStr := c.Param("pageNumber")

	// Convert page number to int
	pageNumber, err := strconv.Atoi(pageNumberStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "Invalid page number",
		})
		return
	}

	// Check if job exists
	var exists bool
	err = h.db.QueryRow("SELECT EXISTS(SELECT 1 FROM jobs WHERE job_id = $1)", jobID).Scan(&exists)
	if err != nil {
		log.Printf("Database error checking job existence: %v", err)
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

	// Query dataset entries for the specific job and page
	rows, err := h.db.Query(
		"SELECT id, input, output, chunk, created_at FROM dataset WHERE job_id = $1 AND page_number = $2 ORDER BY id",
		jobID, pageNumber,
	)
	if err != nil {
		log.Printf("Error querying dataset entries: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("Database error: %v", err),
		})
		return
	}
	defer rows.Close()

	// Parse results
	var entries []gin.H
	for rows.Next() {
		var id int
		var input, output, chunk string
		var createdAt time.Time

		if err := rows.Scan(&id, &input, &output, &chunk, &createdAt); err != nil {
			log.Printf("Error scanning dataset row: %v", err)
			continue
		}

		entries = append(entries, gin.H{
			"id":         id,
			"input":      input,
			"output":     output,
			"chunk":      chunk,
			"created_at": createdAt,
		})
	}

	// Check if any entries were found
	if len(entries) == 0 {
		c.JSON(http.StatusNotFound, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("No dataset entries found for job %s page %d", jobID, pageNumber),
		})
		return
	}

	// Return the dataset entries
	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"job_id":  jobID,
		"page":    pageNumber,
		"count":   len(entries),
		"entries": entries,
	})
}

// ExportJobPageDatasetJSON exports dataset entries for a specific job and page as a JSON file
func (h *DatasetHandlers) ExportJobPageDatasetJSON(c *gin.Context) {
	jobID := c.Param("jobID")
	pageNumberStr := c.Param("pageNumber")

	// Convert page number to int
	pageNumber, err := strconv.Atoi(pageNumberStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "Invalid page number",
		})
		return
	}

	// Check if job exists
	var exists bool
	err = h.db.QueryRow("SELECT EXISTS(SELECT 1 FROM jobs WHERE job_id = $1)", jobID).Scan(&exists)
	if err != nil {
		log.Printf("Database error checking job existence: %v", err)
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

	// Query dataset entries for the specific job and page
	rows, err := h.db.Query(
		"SELECT id, input, output, chunk, created_at FROM dataset WHERE job_id = $1 AND page_number = $2 ORDER BY id",
		jobID, pageNumber,
	)
	if err != nil {
		log.Printf("Error querying dataset entries: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("Database error: %v", err),
		})
		return
	}
	defer rows.Close()

	// Parse results
	var entries []map[string]interface{}
	for rows.Next() {
		var id int
		var input, output, chunk string
		var createdAt time.Time

		if err := rows.Scan(&id, &input, &output, &chunk, &createdAt); err != nil {
			log.Printf("Error scanning dataset row: %v", err)
			continue
		}

		entries = append(entries, map[string]interface{}{
			"id":         id,
			"input":      input,
			"output":     output,
			"chunk":      chunk,
			"created_at": createdAt,
		})
	}

	// Check if any entries were found
	if len(entries) == 0 {
		c.JSON(http.StatusNotFound, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("No dataset entries found for job %s page %d", jobID, pageNumber),
		})
		return
	}

	// Convert to JSON
	jsonData, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("Failed to generate JSON: %v", err),
		})
		return
	}

	// Get job filename
	var filename string
	err = h.db.QueryRow("SELECT filename FROM jobs WHERE job_id = $1", jobID).Scan(&filename)
	if err != nil {
		filename = jobID // Fallback to job ID if filename can't be retrieved
	}

	// Set headers for file download
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=%s_page%d_dataset.json", filename, pageNumber))
	c.Header("Content-Type", "application/json")
	c.Data(http.StatusOK, "application/json", jsonData)
}

// ExportJobPageDatasetXLSX exports dataset entries for a specific job and page as an XLSX file
func (h *DatasetHandlers) ExportJobPageDatasetXLSX(c *gin.Context) {
	jobID := c.Param("jobid")
	if jobID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Job ID is required"})
		return
	}

	// Veritabanından verileri çek
	rows, err := h.db.Query(`
		SELECT input, output, chunk, created_at 
		FROM dataset_entries 
		WHERE job_id = $1 
		ORDER BY created_at DESC`, jobID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer rows.Close()

	var entries []DatasetEntryFormatted
	for rows.Next() {
		var entry DatasetEntryFormatted
		if err := rows.Scan(&entry.Input, &entry.Output, &entry.Chunk, &entry.CreatedAt); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		entries = append(entries, entry)
	}

	if len(entries) == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "No dataset entries found for this job"})
		return
	}

	// UTF-8 BOM ekleyerek XLSX oluştur
	c.Header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=dataset_%s.xlsx", jobID))

	// Excel dosyasını oluştur
	file := excelize.NewFile()
	sheet := "Dataset"
	file.NewSheet(sheet)
	file.DeleteSheet("Sheet1")

	// Başlık satırını ekle
	headers := []string{"Input", "Output", "Chunk", "Created At"}
	for i, header := range headers {
		cell, _ := excelize.CoordinatesToCellName(i+1, 1)
		file.SetCellValue(sheet, cell, header)
	}

	// Verileri ekle
	for i, entry := range entries {
		row := i + 2
		file.SetCellValue(sheet, fmt.Sprintf("A%d", row), entry.Input)
		file.SetCellValue(sheet, fmt.Sprintf("B%d", row), entry.Output)
		file.SetCellValue(sheet, fmt.Sprintf("C%d", row), entry.Chunk)
		file.SetCellValue(sheet, fmt.Sprintf("D%d", row), entry.CreatedAt.Format(time.RFC3339))
	}

	// Dosyayı yaz
	if err := file.Write(c.Writer); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
}

// cleanJSONString removes markdown code blocks and other potential wrapping from JSON strings
func cleanJSONString(input string) string {
	// Remove markdown JSON code block if present
	input = trimPrefix(input, "```json")
	input = trimPrefix(input, "```")
	input = trimSuffix(input, "```")

	// Trim whitespace
	return input
}

// trimPrefix removes a prefix from a string if present
func trimPrefix(s, prefix string) string {
	if len(s) >= len(prefix) && s[:len(prefix)] == prefix {
		s = s[len(prefix):]
	}
	return s
}

// trimSuffix removes a suffix from a string if present
func trimSuffix(s, suffix string) string {
	if len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix {
		s = s[:len(s)-len(suffix)]
	}
	return s
}

// datasetWorker processes messages from the ocr_results queue
func (h *DatasetHandlers) datasetWorker(workerID int) {
	log.Printf("Dataset worker %d starting", workerID)

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

	// Declare the ocr_results queue
	resultsQueue, err := ch.QueueDeclare(
		"ocr_results", // name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		log.Printf("Worker %d: Failed to declare queue: %v", workerID, err)
		return
	}

	// Set prefetch count to 1 to distribute work fairly among workers
	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		log.Printf("Worker %d: Failed to set QoS: %v", workerID, err)
		return
	}

	// Initialize AI client based on selected provider and model
	ctx := context.Background()
	var provider, apiKey, modelName string
	if err := h.db.QueryRow(
		"SELECT model_provider, model_api_key, model_name FROM settings WHERE id = 1",
	).Scan(&provider, &apiKey, &modelName); err != nil {
		log.Printf("Worker %d: Error loading model settings: %v, defaulting to Gemini", workerID, err)
		provider = "gemini"
		apiKey = os.Getenv("GEMINI_API_KEY")
		modelName = "gemini-2.5-pro-preview-03-25"
	}
	if provider != "gemini" && provider != "openai" && provider != "xai" {
		log.Printf("Worker %d: Provider %s not recognized, falling back to Gemini", workerID, provider)
		provider = "gemini"
	}

	var client *genai.Client
	var model *genai.GenerativeModel

	// Only create Gemini client if provider is Gemini
	if provider == "gemini" {
		client, err = genai.NewClient(ctx, option.WithAPIKey(apiKey))
		if err != nil {
			log.Printf("Worker %d: Failed to create AI client: %v", workerID, err)
			return
		}
		defer client.Close()
		// Configure model from settings
		model = client.GenerativeModel(modelName)
		model.ResponseMIMEType = "application/json"

		// Define the dataset entry schema
		model.ResponseSchema = &genai.Schema{
			Type: genai.TypeArray,
			Items: &genai.Schema{
				Type: genai.TypeObject,
				Properties: map[string]*genai.Schema{
					"input":  {Type: genai.TypeString},
					"output": {Type: genai.TypeString},
					"chunk":  {Type: genai.TypeString},
				},
				Required: []string{"input", "output", "chunk"},
			},
		}

		// System instruction for Gemini
		model.SystemInstruction = &genai.Content{
			Parts: []genai.Part{genai.Text(`
				Sen OCR metin işleme için bir veri seti oluşturucusun. Sana vereceğim OCR metninden, dil modelini eğitmek için kullanılabilecek anlamlı girdi ve çıktı çiftleri çıkaracaksın. Her bir çift, 'input', 'output' ve 'chunk' alanlarını içermelidir. 'Input' alanı, OCR metninden türetilen ve dil modeli tarafından cevaplanabilecek bir soru veya talimat olmalıdır. 'Output' alanı, bu girdiye verilmesi beklenen doğru yanıt olmalıdır. 'Chunk' alanı, bu çiftin türetildiği anlamlı (sematik) bir OCR metin segmentini içermelidir. OCR çıktı metnindeki anlamsız karakterler kullanılmamalıdır. Her bir çift, OCR metninin farklı yönlerini ele almalı ve tekrarlamamalıdır. 'Input' ve 'output' çiftleri, basit soru-cevap yerine daha karmaşık ve ayrıntılı sorgular ve yanıtlar içermelidir. Veri seti, metnin tüm detaylarını ve bağlamlarını kapsayacak kadar çeşitli ve uzun olmalı. Verileri, her biri 'input', 'output' ve 'chunk' alanlarına sahip birden fazla giriş içeren bir JSON dizisi olarak döndüreceksin. Lütfen OCR metnindeki tüm detayları ve bağlamları kapsayacak şekilde veri setini oluştur. Ürettiğin her 'input' ve 'output' çifti yeterince uzun ve kapsayıcı olmalı, kısa olmamalıdır. Veri setindeki her örnek, OCR metninin farklı yönlerini veya senaryolarını kapsamalı ve çeşitlilik göstermelidir. Yaratıcı ol, ancak verilerin yüksek kaliteli olduğundan emin ol. Ürettiğin veri setinde asla orijinal OCR metnine atıfta bulunulmamalıdır; her şey bağımsız ve kendinden yeterli olmalıdır. Her zaman belirtilen şemaya sıkı sıkıya uyan geçerli JSON döndür.
				---
				OCR çıktı metni:
				---
			`)},
		}
	}

	// Get messages from the queue
	msgs, err := ch.Consume(
		resultsQueue.Name, // queue
		"",                // consumer
		false,             // auto-ack (we'll manually ack)
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // arguments
	)
	if err != nil {
		log.Printf("Worker %d: Failed to register a consumer: %v", workerID, err)
		return
	}

	log.Printf("Worker %d is now consuming from queue", workerID)

	// Process messages from the channel
	for msg := range msgs {
		// Extract job ID
		msgJobID, ok := msg.Headers["job_id"].(string)
		if !ok {
			log.Printf("Worker %d: Missing job_id in message headers", workerID)
			msg.Nack(false, false) // Don't requeue, this message is broken
			continue
		}

		// Check if job is marked as interrupted
		var status string
		err := h.db.QueryRow("SELECT status FROM jobs WHERE job_id = $1", msgJobID).Scan(&status)
		if err != nil {
			log.Printf("Worker %d: Error checking job status for %s: %v", workerID, msgJobID, err)
			msg.Nack(false, true) // Requeue the message
			continue
		}

		// Skip processing if job status is 'interrupted'
		if status == "interrupted" {
			log.Printf("Worker %d: Job %s is marked as interrupted, skipping", workerID, msgJobID)
			msg.Ack(false) // Acknowledge the message to remove it from the queue
			continue
		}

		// Extract page number
		pageNumberVal, ok := msg.Headers["page_number"]
		if !ok {
			log.Printf("Worker %d: Missing page_number in message headers", workerID)
			msg.Nack(false, false) // Don't requeue, this message is broken
			continue
		}

		// Convert page number to int
		var pageNumber int
		switch v := pageNumberVal.(type) {
		case int32:
			pageNumber = int(v)
		case int:
			pageNumber = v
		case int64:
			pageNumber = int(v)
		case float64:
			pageNumber = int(v)
		default:
			log.Printf("Worker %d: Invalid page_number type: %T", workerID, pageNumberVal)
			msg.Nack(false, false)
			continue
		}

		log.Printf("Worker %d processing job %s page %d", workerID, msgJobID, pageNumber)

		// Parse the message body
		var result struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(msg.Body, &result); err != nil {
			log.Printf("Worker %d: Error unmarshaling message: %v", workerID, err)
			msg.Nack(false, false) // Don't requeue
			continue
		}

		if len(result.Text) < 50 {
			log.Printf("Worker %d: Text for job %s page %d is too short, skipping", workerID, msgJobID, pageNumber)
			// Acknowledge message even though we're skipping it - we've evaluated it
			msg.Ack(false)
			continue
		}

		// Generate dataset entries using model with retry mechanism and token tracking
		var datasetEntries []DatasetEntry
		var tokenUsage *TokenUsage
		var genErr error

		if provider == "openai" {
			datasetEntries, tokenUsage, genErr = h.generateOpenAIDatasetWithRetry(
				ctx,
				openai.NewClient(openaiOption.WithAPIKey(apiKey)),
				modelName,
				`Sen OCR metin işleme için bir veri seti oluşturucusun. Sana vereceğim OCR metninden, dil modelini eğitmek için kullanılabilecek anlamlı girdi ve çıktı çiftleri çıkaracaksın. Her bir çift, 'input', 'output' ve 'chunk' alanlarını içermelidir. 'Input' alanı, OCR metninden türetilen ve dil modeli tarafından cevaplanabilecek bir soru veya talimat olmalıdır. 'Output' alanı, bu girdiye verilmesi beklenen doğru yanıt olmalıdır. 'Chunk' alanı, bu çiftin türetildiği anlamlı (sematik) bir OCR metin segmentini içermelidir. OCR çıktı metnindeki anlamsız karakterler kullanılmamalıdır. Her bir çift, OCR metninin farklı yönlerini ele almalı ve tekrarlamamalıdır. 'Input' ve 'output' çiftleri, basit soru-cevap yerine daha karmaşık ve ayrıntılı sorgular ve yanıtlar içermelidir. Veri seti, metnin tüm detaylarını ve bağlamlarını kapsayacak kadar çeşitli ve uzun olmalı. Verileri, her biri 'input', 'output' ve 'chunk' alanlarına sahip birden fazla giriş içeren bir JSON dizisi olarak döndüreceksin. Lütfen OCR metnindeki tüm detayları ve bağlamları kapsayacak şekilde veri setini oluştur. Ürettiğin her 'input' ve 'output' çifti yeterince uzun ve kapsayıcı olmalı, kısa olmamalıdır. Veri setindeki her örnek, OCR metninin farklı yönlerini veya senaryolarını kapsamalı ve çeşitlilik göstermelidir. Yaratıcı ol, ancak verilerin yüksek kaliteli olduğundan emin ol. Ürettiğin veri setinde asla orijinal OCR metnine atıfta bulunulmamalıdır; her şey bağımsız ve kendinden yeterli olmalıdır. Her zaman belirtilen şemaya sıkı sıkıya uyan geçerli JSON döndür.`,
				result.Text,
				msgJobID,
				pageNumber,
			)
		} else if provider == "xai" {
			datasetEntries, tokenUsage, genErr = h.generateGrokDatasetWithRetry(
				ctx,
				apiKey,
				modelName,
				`Sen OCR metin işleme için bir veri seti oluşturucusun. Sana vereceğim OCR metninden, dil modelini eğitmek için kullanılabilecek anlamlı girdi ve çıktı çiftleri çıkaracaksın. Her bir çift, 'input', 'output' ve 'chunk' alanlarını içermelidir. 'Input' alanı, OCR metninden türetilen ve dil modeli tarafından cevaplanabilecek bir soru veya talimat olmalıdır. 'Output' alanı, bu girdiye verilmesi beklenen doğru yanıt olmalıdır. 'Chunk' alanı, bu çiftin türetildiği anlamlı (sematik) bir OCR metin segmentini içermelidir. OCR çıktı metnindeki anlamsız karakterler kullanılmamalıdır. Her bir çift, OCR metninin farklı yönlerini ele almalı ve tekrarlamamalıdır. 'Input' ve 'output' çiftleri, basit soru-cevap yerine daha karmaşık ve ayrıntılı sorgular ve yanıtlar içermelidir. Veri seti, metnin tüm detaylarını ve bağlamlarını kapsayacak kadar çeşitli ve uzun olmalı. Verileri, her biri 'input', 'output' ve 'chunk' alanlarına sahip birden fazla giriş içeren bir JSON dizisi olarak döndüreceksin. Lütfen OCR metnindeki tüm detayları ve bağlamları kapsayacak şekilde veri setini oluştur. Ürettiğin her 'input' ve 'output' çifti yeterince uzun ve kapsayıcı olmalı, kısa olmamalıdır. Veri setindeki her örnek, OCR metninin farklı yönlerini veya senaryolarını kapsamalı ve çeşitlilik göstermelidir. Yaratıcı ol, ancak verilerin yüksek kaliteli olduğundan emin ol. Ürettiğin veri setinde asla orijinal OCR metnine atıfta bulunulmamalıdır; her şey bağımsız ve kendinden yeterli olmalıdır. Her zaman belirtilen şemaya sıkı sıkıya uyan geçerli JSON döndür.`,
				result.Text,
				msgJobID,
				pageNumber,
			)
		} else {
			datasetEntries, tokenUsage, genErr = h.generateDatasetWithRetry(ctx, model, result.Text, msgJobID, pageNumber)
		}
		if genErr != nil {
			log.Printf("Failed to generate dataset for job %s page %d: %v", msgJobID, pageNumber, genErr)
			msg.Ack(false)
			continue
		}

		// Store token usage in database
		if tokenUsage != nil {
			_, err := h.db.Exec(`
				INSERT INTO token_usage (job_id, page_number, prompt_tokens, response_tokens, total_tokens, estimated_tokens, provider, model_name)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			`, tokenUsage.JobID, tokenUsage.PageNumber, tokenUsage.PromptTokens,
				tokenUsage.ResponseTokens, tokenUsage.TotalTokens, tokenUsage.EstimatedTokens,
				tokenUsage.Provider, tokenUsage.ModelName)

			if err != nil {
				log.Printf("Error storing token usage for job %s page %d: %v", msgJobID, pageNumber, err)
			}
		}

		// Store dataset entries in database
		for _, entry := range datasetEntries {
			_, err := h.db.Exec(`
				INSERT INTO dataset (job_id, page_number, input, output, chunk)
				VALUES ($1, $2, $3, $4, $5)
			`, msgJobID, pageNumber, entry.Input, entry.Output, entry.Chunk)
			if err != nil {
				log.Printf("Error storing dataset entry for job %s page %d: %v", msgJobID, pageNumber, err)
			}
		}

		// Update worker_results to mark this page as processed for dataset generation
		_, err = h.db.Exec(`
			UPDATE worker_results 
			SET processing_status = 'processed' 
			WHERE job_id = $1 AND page_number = $2
		`, msgJobID, pageNumber)
		if err != nil {
			log.Printf("Error updating worker_results status for job %s page %d: %v", msgJobID, pageNumber, err)
		} else {
			log.Printf("Updated worker_results status to 'processed' for job %s page %d", msgJobID, pageNumber)
		}

		log.Printf("Generated %d dataset entries for job %s page %d", len(datasetEntries), msgJobID, pageNumber)

		// Acknowledge the message to remove it from the queue
		msg.Ack(false)
	}
}

// processJobDatasetWithWorkers processes a specific job using multiple workers
func (h *DatasetHandlers) processJobDatasetWithWorkers(jobID string, workerCount int) {
	log.Printf("Starting dataset generation for job %s with %d workers", jobID, workerCount)

	// Connect to RabbitMQ
	conn, err := amqp.Dial(config.GetRabbitMQURL())
	if err != nil {
		log.Printf("Failed to connect to RabbitMQ: %v", err)
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Printf("Failed to open channel: %v", err)
		return
	}
	defer ch.Close()

	// Declare the ocr_results queue
	resultsQueue, err := ch.QueueDeclare(
		"ocr_results", // name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		log.Printf("Failed to declare queue: %v", err)
		return
	}

	// Check if there are any messages in the queue
	queueInfo, err := ch.QueueInspect(resultsQueue.Name)
	if err != nil {
		log.Printf("Failed to inspect queue: %v", err)
		return
	}

	// If queue is empty, no need to start workers
	if queueInfo.Messages == 0 {
		log.Printf("Queue is empty, no OCR results to process for job %s", jobID)
		return
	}

	log.Printf("Queue has %d messages to potentially process for job %s", queueInfo.Messages, jobID)

	// Start workers using a WaitGroup to wait for all workers to complete
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			h.jobSpecificDatasetWorker(workerID, jobID)
		}(i)
	}

	// Wait for all workers to complete
	wg.Wait()
	log.Printf("All dataset workers have completed processing for job %s", jobID)
}

// jobSpecificDatasetWorker processes messages from the ocr_results queue for a specific job
func (h *DatasetHandlers) jobSpecificDatasetWorker(workerID int, jobID string) {
	log.Printf("Dataset worker %d starting for job %s", workerID, jobID)

	// Check if job is marked as interrupted
	var status string
	err := h.db.QueryRow("SELECT status FROM jobs WHERE job_id = $1", jobID).Scan(&status)
	if err != nil {
		log.Printf("Worker %d: Error checking job status for %s: %v", workerID, jobID, err)
		return
	}

	// Exit if job status is 'interrupted'
	if status == "interrupted" {
		log.Printf("Worker %d: Job %s is marked as interrupted, not processing", workerID, jobID)
		return
	}

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

	// Declare the ocr_results queue
	resultsQueue, err := ch.QueueDeclare(
		"ocr_results", // name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		log.Printf("Worker %d: Failed to declare queue: %v", workerID, err)
		return
	}

	// Set prefetch count to 1 to distribute work fairly among workers
	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		log.Printf("Worker %d: Failed to set QoS: %v", workerID, err)
		return
	}

	// Initialize AI client based on selected provider and model
	ctx := context.Background()
	var provider, apiKey, modelName string
	if err := h.db.QueryRow(
		"SELECT model_provider, model_api_key, model_name FROM settings WHERE id = 1",
	).Scan(&provider, &apiKey, &modelName); err != nil {
		log.Printf("Worker %d: Error loading model settings: %v, defaulting to Gemini", workerID, err)
		provider = "gemini"
		apiKey = os.Getenv("GEMINI_API_KEY")
		modelName = "gemini-2.5-pro-preview-03-25"
	}
	if provider != "gemini" && provider != "openai" && provider != "xai" {
		log.Printf("Worker %d: Provider %s not recognized, falling back to Gemini", workerID, provider)
		provider = "gemini"
	}

	var client *genai.Client
	var model *genai.GenerativeModel

	// Only create Gemini client if provider is Gemini
	if provider == "gemini" {
		client, err = genai.NewClient(ctx, option.WithAPIKey(apiKey))
		if err != nil {
			log.Printf("Worker %d: Failed to create AI client: %v", workerID, err)
			return
		}
		defer client.Close()
		// Configure model from settings
		model = client.GenerativeModel(modelName)
		model.ResponseMIMEType = "application/json"

		// Define the dataset entry schema
		model.ResponseSchema = &genai.Schema{
			Type: genai.TypeArray,
			Items: &genai.Schema{
				Type: genai.TypeObject,
				Properties: map[string]*genai.Schema{
					"input":  {Type: genai.TypeString},
					"output": {Type: genai.TypeString},
					"chunk":  {Type: genai.TypeString},
				},
				Required: []string{"input", "output", "chunk"},
			},
		}

		// System instruction for Gemini
		model.SystemInstruction = &genai.Content{
			Parts: []genai.Part{genai.Text(`
				Sen OCR metin işleme için bir veri seti oluşturucusun. Sana vereceğim OCR metninden, dil modelini eğitmek için kullanılabilecek anlamlı girdi ve çıktı çiftleri çıkaracaksın. Her bir çift, 'input', 'output' ve 'chunk' alanlarını içermelidir. 'Input' alanı, OCR metninden türetilen ve dil modeli tarafından cevaplanabilecek bir soru veya talimat olmalıdır. 'Output' alanı, bu girdiye verilmesi beklenen doğru yanıt olmalıdır. 'Chunk' alanı, bu çiftin türetildiği anlamlı (sematik) bir OCR metin segmentini içermelidir. OCR çıktı metnindeki anlamsız karakterler kullanılmamalıdır. Her bir çift, OCR metninin farklı yönlerini ele almalı ve tekrarlamamalıdır. 'Input' ve 'output' çiftleri, basit soru-cevap yerine daha karmaşık ve ayrıntılı sorgular ve yanıtlar içermelidir. Veri seti, metnin tüm detaylarını ve bağlamlarını kapsayacak kadar çeşitli ve uzun metinler içermelidir. Verileri, her biri 'input', 'output' ve 'chunk' alanlarına sahip birden fazla giriş içeren bir JSON dizisi olarak döndüreceksin. Lütfen OCR metnindeki tüm detayları ve bağlamları kapsayacak şekilde veri setini oluştur. Ürettiğin her 'input' ve 'output' çifti yeterince uzun ve kapsayıcı olmalı, kısa olmamalıdır. Veri setindeki her örnek, OCR metninin farklı yönlerini veya senaryolarını kapsamalı ve çeşitlilik göstermelidir. Yaratıcı ol, ancak verilerin yüksek kaliteli olduğundan emin ol. Ürettiğin veri setinde asla orijinal OCR metnine atıfta bulunulmamalıdır; her şey bağımsız ve kendinden yeterli olmalıdır. Her zaman belirtilen şemaya sıkı sıkıya uyan geçerli JSON döndür.
				---
				OCR çıktı metni:
				---
			`)},
		}
	}

	// Get messages from the queue
	msgs, err := ch.Consume(
		resultsQueue.Name, // queue
		"",                // consumer
		false,             // auto-ack (we'll manually ack)
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // arguments
	)
	if err != nil {
		log.Printf("Worker %d: Failed to register a consumer: %v", workerID, err)
		return
	}

	log.Printf("Worker %d is now consuming from queue", workerID)

	// Process messages from the channel
	for msg := range msgs {
		// Extract job ID
		msgJobID, ok := msg.Headers["job_id"].(string)
		if !ok || msgJobID != jobID {
			log.Printf("Worker %d: This message is for a different job, putting it back in the queue", workerID)
			msg.Nack(false, true) // Requeue for another consumer
			continue
		}

		// Periodically check if job status has changed to interrupted
		var currentStatus string
		err := h.db.QueryRow("SELECT status FROM jobs WHERE job_id = $1", jobID).Scan(&currentStatus)
		if err != nil {
			log.Printf("Worker %d: Error checking job status for %s: %v", workerID, jobID, err)
		} else if currentStatus == "interrupted" {
			log.Printf("Worker %d: Job %s has been marked as interrupted, stopping processing", workerID, jobID)
			msg.Nack(false, true) // Put the message back in the queue
			return
		}

		// Extract page number
		pageNumberVal, ok := msg.Headers["page_number"]
		if !ok {
			log.Printf("Worker %d: Missing page_number in message headers", workerID)
			msg.Nack(false, false) // Don't requeue, this message is broken
			continue
		}

		// Convert page number to int
		var pageNumber int
		switch v := pageNumberVal.(type) {
		case int32:
			pageNumber = int(v)
		case int:
			pageNumber = v
		case int64:
			pageNumber = int(v)
		case float64:
			pageNumber = int(v)
		default:
			log.Printf("Worker %d: Invalid page_number type: %T", workerID, pageNumberVal)
			msg.Nack(false, false)
			continue
		}

		log.Printf("Worker %d processing job %s page %d", workerID, msgJobID, pageNumber)

		// Parse the message body
		var result struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(msg.Body, &result); err != nil {
			log.Printf("Worker %d: Error unmarshaling message: %v", workerID, err)
			msg.Nack(false, false) // Don't requeue
			continue
		}

		if len(result.Text) < 50 {
			log.Printf("Worker %d: Text for job %s page %d is too short, skipping", workerID, msgJobID, pageNumber)
			// Acknowledge message even though we're skipping it - we've evaluated it
			msg.Ack(false)
			continue
		}

		// Generate entries based on provider
		var datasetEntries []DatasetEntry
		var tokenUsage *TokenUsage
		var genErr error

		if provider == "openai" {
			datasetEntries, tokenUsage, genErr = h.generateOpenAIDatasetWithRetry(
				ctx,
				openai.NewClient(openaiOption.WithAPIKey(apiKey)),
				modelName,
				`Sen OCR metin işleme için bir veri seti oluşturucusun. Sana vereceğim OCR metninden, dil modelini eğitmek için kullanılabilecek anlamlı girdi ve çıktı çiftleri çıkaracaksın. Her bir çift, 'input', 'output' ve 'chunk' alanlarını içermelidir. 'Input' alanı, OCR metninden türetilen ve dil modeli tarafından cevaplanabilecek bir soru veya talimat olmalıdır. 'Output' alanı, bu girdiye verilmesi beklenen doğru yanıt olmalıdır. 'Chunk' alanı, bu çiftin türetildiği anlamlı (sematik) bir OCR metin segmentini içermelidir. OCR çıktı metnindeki anlamsız karakterler kullanılmamalıdır. Her bir çift, OCR metninin farklı yönlerini ele almalı ve tekrarlamamalıdır. 'Input' ve 'output' çiftleri, basit soru-cevap yerine daha karmaşık ve ayrıntılı sorgular ve yanıtlar içermelidir. Veri seti, metnin tüm detaylarını ve bağlamlarını kapsayacak kadar çeşitli ve uzun olmalı. Verileri, her biri 'input', 'output' ve 'chunk' alanlarına sahip birden fazla giriş içeren bir JSON dizisi olarak döndüreceksin. Lütfen OCR metnindeki tüm detayları ve bağlamları kapsayacak şekilde veri setini oluştur. Ürettiğin her 'input' ve 'output' çifti yeterince uzun ve kapsayıcı olmalı, kısa olmamalıdır. Veri setindeki her örnek, OCR metninin farklı yönlerini veya senaryolarını kapsamalı ve çeşitlilik göstermelidir. Yaratıcı ol, ancak verilerin yüksek kaliteli olduğundan emin ol. Ürettiğin veri setinde asla orijinal OCR metnine atıfta bulunulmamalıdır; her şey bağımsız ve kendinden yeterli olmalıdır. Her zaman belirtilen şemaya sıkı sıkıya uyan geçerli JSON döndür.`,
				result.Text,
				msgJobID,
				pageNumber,
			)
		} else if provider == "xai" {
			datasetEntries, tokenUsage, genErr = h.generateGrokDatasetWithRetry(
				ctx,
				apiKey,
				modelName,
				`Sen OCR metin işleme için bir veri seti oluşturucusun. Sana vereceğim OCR metninden, dil modelini eğitmek için kullanılabilecek anlamlı girdi ve çıktı çiftleri çıkaracaksın. Her bir çift, 'input', 'output' ve 'chunk' alanlarını içermelidir. 'Input' alanı, OCR metninden türetilen ve dil modeli tarafından cevaplanabilecek bir soru veya talimat olmalıdır. 'Output' alanı, bu girdiye verilmesi beklenen doğru yanıt olmalıdır. 'Chunk' alanı, bu çiftin türetildiği anlamlı (sematik) bir OCR metin segmentini içermelidir. OCR çıktı metnindeki anlamsız karakterler kullanılmamalıdır. Her bir çift, OCR metninin farklı yönlerini ele almalı ve tekrarlamamalıdır. 'Input' ve 'output' çiftleri, basit soru-cevap yerine daha karmaşık ve ayrıntılı sorgular ve yanıtlar içermelidir. Veri seti, metnin tüm detaylarını ve bağlamlarını kapsayacak kadar çeşitli ve uzun olmalı. Verileri, her biri 'input', 'output' ve 'chunk' alanlarına sahip birden fazla giriş içeren bir JSON dizisi olarak döndüreceksin. Lütfen OCR metnindeki tüm detayları ve bağlamları kapsayacak şekilde veri setini oluştur. Ürettiğin her 'input' ve 'output' çifti yeterince uzun ve kapsayıcı olmalı, kısa olmamalıdır. Veri setindeki her örnek, OCR metninin farklı yönlerini veya senaryolarını kapsamalı ve çeşitlilik göstermelidir. Yaratıcı ol, ancak verilerin yüksek kaliteli olduğundan emin ol. Ürettiğin veri setinde asla orijinal OCR metnine atıfta bulunulmamalıdır; her şey bağımsız ve kendinden yeterli olmalıdır. Her zaman belirtilen şemaya sıkı sıkıya uyan geçerli JSON döndür.`,
				result.Text,
				msgJobID,
				pageNumber,
			)
		} else {
			datasetEntries, tokenUsage, genErr = h.generateDatasetWithRetry(ctx, model, result.Text, msgJobID, pageNumber)
		}
		if genErr != nil {
			log.Printf("Failed to generate dataset for job %s page %d: %v", msgJobID, pageNumber, genErr)
			msg.Ack(false)
			continue
		}

		// Store token usage in database
		if tokenUsage != nil {
			_, err := h.db.Exec(`
				INSERT INTO token_usage (job_id, page_number, prompt_tokens, response_tokens, total_tokens, estimated_tokens, provider, model_name)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			`, tokenUsage.JobID, tokenUsage.PageNumber, tokenUsage.PromptTokens,
				tokenUsage.ResponseTokens, tokenUsage.TotalTokens, tokenUsage.EstimatedTokens,
				tokenUsage.Provider, tokenUsage.ModelName)

			if err != nil {
				log.Printf("Error storing token usage for job %s page %d: %v", msgJobID, pageNumber, err)
			}
		}

		// Store dataset entries in database
		for _, entry := range datasetEntries {
			_, err := h.db.Exec(`
				INSERT INTO dataset (job_id, page_number, input, output, chunk)
				VALUES ($1, $2, $3, $4, $5)
			`, msgJobID, pageNumber, entry.Input, entry.Output, entry.Chunk)
			if err != nil {
				log.Printf("Error storing dataset entry for job %s page %d: %v", msgJobID, pageNumber, err)
			}
		}

		// Update worker_results to mark this page as processed for dataset generation
		_, err = h.db.Exec(`
			UPDATE worker_results 
			SET processing_status = 'processed' 
			WHERE job_id = $1 AND page_number = $2
		`, msgJobID, pageNumber)
		if err != nil {
			log.Printf("Error updating worker_results status for job %s page %d: %v", msgJobID, pageNumber, err)
		} else {
			log.Printf("Updated worker_results status to 'processed' for job %s page %d", msgJobID, pageNumber)
		}

		log.Printf("Generated %d dataset entries for job %s page %d", len(datasetEntries), msgJobID, pageNumber)

		// Acknowledge the message to remove it from the queue
		msg.Ack(false)
	}
}

// generateOpenAIDatasetWithRetry attempts to generate a dataset using OpenAI API with retries
func (h *DatasetHandlers) generateOpenAIDatasetWithRetry(ctx context.Context, client openai.Client, modelName, systemPrompt, text, jobID string, pageNumber int) ([]DatasetEntry, *TokenUsage, error) {
	maxRetries := 3
	retryDelay := 2 * time.Second

	// Load prompt template from settings
	var promptTemplate string
	if err := h.db.QueryRow("SELECT dataset_generation_prompt FROM settings WHERE id = 1").Scan(&promptTemplate); err != nil {
		log.Printf("Error fetching dataset generation prompt: %v, using default", err)
		promptTemplate = "Generate a synthetic dataset from this OCR text:\n\n{{text}}"
	}
	promptTemplate = strings.Replace(promptTemplate, "{{text}}", text, -1)

	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Build user prompt
		userPrompt := promptTemplate
		if attempt > 1 {
			userPrompt += "\n\nIMPORTANT: Please ensure your response is valid JSON that strictly follows the required schema. Previous attempts failed to parse the JSON."
		}

		// Prepare OpenAI messages
		messages := []openai.ChatCompletionMessageParamUnion{
			{OfSystem: &openai.ChatCompletionSystemMessageParam{Content: openai.ChatCompletionSystemMessageParamContentUnion{OfString: openai.String(systemPrompt)}}},
			{OfUser: &openai.ChatCompletionUserMessageParam{Content: openai.ChatCompletionUserMessageParamContentUnion{OfString: openai.String(userPrompt)}}},
		}
		params := openai.ChatCompletionNewParams{
			Model:    shared.ChatModel(modelName),
			Messages: messages,
		}

		// Call OpenAI
		resp, err := client.Chat.Completions.New(ctx, params)
		if err != nil {
			log.Printf("Error generating dataset with OpenAI for job %s page %d (attempt %d/%d): %v", jobID, pageNumber, attempt, maxRetries, err)
			time.Sleep(retryDelay)
			continue
		}

		// Build token usage
		u := resp.Usage
		tokenUsage := &TokenUsage{
			JobID:           jobID,
			PageNumber:      pageNumber,
			PromptTokens:    int32(u.PromptTokens),
			ResponseTokens:  int32(u.CompletionTokens),
			TotalTokens:     int32(u.TotalTokens),
			EstimatedTokens: int32(u.TotalTokens),
			Timestamp:       time.Now(),
			Provider:        "openai",
			ModelName:       modelName,
		}
		log.Printf("OpenAI token usage for job %s page %d: prompt=%d, response=%d, total=%d",
			jobID, pageNumber, tokenUsage.PromptTokens, tokenUsage.ResponseTokens, tokenUsage.TotalTokens)

		// Extract and clean content
		textContent := resp.Choices[0].Message.Content
		clean := cleanJSONString(textContent)

		// Parse JSON
		var entries []DatasetEntry
		if err := json.Unmarshal([]byte(clean), &entries); err != nil {
			log.Printf("Error parsing OpenAI JSON for job %s page %d (attempt %d/%d): %v", jobID, pageNumber, attempt, maxRetries, err)
			time.Sleep(retryDelay)
			continue
		}
		// Validate entries
		var validEntries []DatasetEntry
		for _, e := range entries {
			if e.Input != "" && e.Output != "" {
				validEntries = append(validEntries, e)
			}
		}
		if len(validEntries) == 0 {
			log.Printf("No valid entries from OpenAI for job %s page %d (attempt %d/%d)", jobID, pageNumber, attempt, maxRetries)
			time.Sleep(retryDelay)
			continue
		}

		return validEntries, tokenUsage, nil
	}

	return nil, nil, fmt.Errorf("failed to generate dataset after %d attempts", maxRetries)
}

// generateGrokDatasetWithRetry attempts to generate a dataset using Grok API with retries
func (h *DatasetHandlers) generateGrokDatasetWithRetry(ctx context.Context, apiKey, modelName, systemPrompt, text, jobID string, pageNumber int) ([]DatasetEntry, *TokenUsage, error) {
	maxRetries := 3
	retryDelay := 2 * time.Second

	// Load prompt template from settings
	var promptTemplate string
	if err := h.db.QueryRow("SELECT dataset_generation_prompt FROM settings WHERE id = 1").Scan(&promptTemplate); err != nil {
		log.Printf("Error fetching dataset generation prompt: %v, using default", err)
		promptTemplate = "Generate a synthetic dataset from this OCR text:\n\n{{text}}"
	}
	promptTemplate = strings.Replace(promptTemplate, "{{text}}", text, -1)

	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Build user prompt
		userPrompt := promptTemplate
		if attempt > 1 {
			userPrompt += "\n\nIMPORTANT: Please ensure your response is valid JSON that strictly follows the required schema. Previous attempts failed to parse the JSON."
		}

		// Prepare request to X.AI API
		reqBody := map[string]interface{}{
			"model": modelName,
			"messages": []map[string]interface{}{
				{
					"role":    "system",
					"content": systemPrompt,
				},
				{
					"role":    "user",
					"content": userPrompt,
				},
			},
			"temperature": 0.3,
		}

		reqJSON, err := json.Marshal(reqBody)
		if err != nil {
			return nil, nil, fmt.Errorf("error marshalling request: %v", err)
		}

		// Create HTTP request
		req, err := http.NewRequestWithContext(ctx, "POST", "https://api.x.ai/v1/chat/completions", bytes.NewBuffer(reqJSON))
		if err != nil {
			return nil, nil, fmt.Errorf("error creating request: %v", err)
		}

		// Set headers
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", apiKey))

		// Send request
		client := &http.Client{Timeout: 120 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("Error calling Grok API for job %s page %d (attempt %d/%d): %v", jobID, pageNumber, attempt, maxRetries, err)
			time.Sleep(retryDelay)
			continue
		}
		defer resp.Body.Close()

		// Check response status
		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			log.Printf("Error from Grok API for job %s page %d: status %d, body: %s", jobID, pageNumber, resp.StatusCode, string(bodyBytes))
			time.Sleep(retryDelay)
			continue
		}

		// Parse response
		var grokResp struct {
			ID      string `json:"id"`
			Object  string `json:"object"`
			Created int64  `json:"created"`
			Model   string `json:"model"`
			Choices []struct {
				Index   int `json:"index"`
				Message struct {
					Role    string `json:"role"`
					Content string `json:"content"`
				} `json:"message"`
				FinishReason string `json:"finish_reason"`
			} `json:"choices"`
			Usage struct {
				PromptTokens     int `json:"prompt_tokens"`
				CompletionTokens int `json:"completion_tokens"`
				TotalTokens      int `json:"total_tokens"`
			} `json:"usage"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&grokResp); err != nil {
			log.Printf("Error decoding Grok API response for job %s page %d (attempt %d/%d): %v", jobID, pageNumber, attempt, maxRetries, err)
			time.Sleep(retryDelay)
			continue
		}

		// Build token usage
		tokenUsage := &TokenUsage{
			JobID:           jobID,
			PageNumber:      pageNumber,
			PromptTokens:    int32(grokResp.Usage.PromptTokens),
			ResponseTokens:  int32(grokResp.Usage.CompletionTokens),
			TotalTokens:     int32(grokResp.Usage.TotalTokens),
			EstimatedTokens: int32(grokResp.Usage.TotalTokens),
			Timestamp:       time.Now(),
			Provider:        "xai",
			ModelName:       modelName,
		}
		log.Printf("Grok token usage for job %s page %d: prompt=%d, response=%d, total=%d",
			jobID, pageNumber, tokenUsage.PromptTokens, tokenUsage.ResponseTokens, tokenUsage.TotalTokens)

		// Make sure we have a valid response
		if len(grokResp.Choices) == 0 {
			log.Printf("Empty choices from Grok API for job %s page %d (attempt %d/%d)", jobID, pageNumber, attempt, maxRetries)
			time.Sleep(retryDelay)
			continue
		}

		// Extract and clean content
		textContent := grokResp.Choices[0].Message.Content
		clean := cleanJSONString(textContent)

		// Parse JSON
		var entries []DatasetEntry
		if err := json.Unmarshal([]byte(clean), &entries); err != nil {
			log.Printf("Error parsing Grok JSON for job %s page %d (attempt %d/%d): %v", jobID, pageNumber, attempt, maxRetries, err)
			time.Sleep(retryDelay)
			continue
		}

		// Validate entries
		var validEntries []DatasetEntry
		for _, e := range entries {
			if e.Input != "" && e.Output != "" {
				validEntries = append(validEntries, e)
			}
		}
		if len(validEntries) == 0 {
			log.Printf("No valid entries from Grok for job %s page %d (attempt %d/%d)", jobID, pageNumber, attempt, maxRetries)
			time.Sleep(retryDelay)
			continue
		}

		return validEntries, tokenUsage, nil
	}

	return nil, nil, fmt.Errorf("failed to generate dataset after %d attempts", maxRetries)
}

// Helper to process a single ocr_results message (extracted from datasetWorker)
func (h *DatasetHandlers) processOcrResultMessage(msg amqp.Delivery) {
	// Extract job ID
	msgJobID, ok := msg.Headers["job_id"].(string)
	if !ok {
		log.Printf("Missing job_id in message headers")
		msg.Nack(false, false)
		return
	}

	// Check if job is marked as interrupted
	var status string
	err := h.db.QueryRow("SELECT status FROM jobs WHERE job_id = $1", msgJobID).Scan(&status)
	if err != nil {
		log.Printf("Error checking job status for %s: %v", msgJobID, err)
		msg.Nack(false, true)
		return
	}
	if status == "interrupted" {
		log.Printf("Job %s is marked as interrupted, skipping", msgJobID)
		msg.Ack(false)
		return
	}

	// Extract page number
	pageNumberVal, ok := msg.Headers["page_number"]
	if !ok {
		log.Printf("Missing page_number in message headers")
		msg.Nack(false, false)
		return
	}
	var pageNumber int
	switch v := pageNumberVal.(type) {
	case int32:
		pageNumber = int(v)
	case int:
		pageNumber = v
	case int64:
		pageNumber = int(v)
	case float64:
		pageNumber = int(v)
	default:
		log.Printf("Invalid page_number type: %T", pageNumberVal)
		msg.Nack(false, false)
		return
	}

	// Parse the message body
	var result struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(msg.Body, &result); err != nil {
		log.Printf("Error unmarshaling message: %v", err)
		msg.Nack(false, false)
		return
	}

	if len(result.Text) < 50 {
		log.Printf("Text for job %s page %d is too short, skipping", msgJobID, pageNumber)
		msg.Ack(false)
		return
	}

	// TODO: Add your dataset generation logic here (call Gemini/OpenAI/XAI, store results, etc.)
	// For now, just log and acknowledge
	log.Printf("Would process dataset for job %s page %d", msgJobID, pageNumber)
	msg.Ack(false)
}

// ExportJobDataset exports dataset entries for a specific job in the requested format
func (h *DatasetHandlers) ExportJobDataset(c *gin.Context) {
	jobID := c.Param("jobID")
	format := c.DefaultQuery("format", "json")

	// Check if format is valid
	if format != "json" && format != "xlsx" && format != "csv" {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "Invalid format specified. Supported formats: json, xlsx, csv",
		})
		return
	}

	// Check if job exists
	var exists bool
	err := h.db.QueryRow("SELECT EXISTS(SELECT 1 FROM jobs WHERE job_id = $1)", jobID).Scan(&exists)
	if err != nil {
		log.Printf("Database error checking job existence: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("Database error: %v", err),
		})
		return
	}

	if !exists {
		log.Printf("Job not found: %s", jobID)
		c.JSON(http.StatusNotFound, gin.H{
			"status":  "error",
			"message": "Job not found",
		})
		return
	}

	// Query database to retrieve dataset entries for this job
	rows, err := h.db.Query(`
		SELECT id, input, output, chunk, created_at, page_number 
		FROM dataset 
		WHERE job_id = $1 
		ORDER BY page_number ASC, id ASC
	`, jobID)
	if err != nil {
		log.Printf("Failed to query dataset: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("Failed to query dataset: %v", err),
		})
		return
	}
	defer rows.Close()

	var allEntries []DatasetEntryFormatted

	for rows.Next() {
		var id int
		var input, output, chunk string
		var createdAt time.Time
		var pageNumber int

		if err := rows.Scan(&id, &input, &output, &chunk, &createdAt, &pageNumber); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		allEntries = append(allEntries, DatasetEntryFormatted{
			Chunk:     chunk,
			CreatedAt: createdAt,
			ID:        id,
			Input:     input,
			Output:    output,
		})
	}

	if err := rows.Err(); err != nil {
		log.Printf("Error iterating rows: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("Error reading dataset: %v", err),
		})
		return
	}

	if len(allEntries) == 0 {
		c.JSON(http.StatusNotFound, gin.H{
			"status":  "error",
			"message": "No dataset entries found for this job",
		})
		return
	}

	// Query job metadata for filename
	var filename string
	err = h.db.QueryRow("SELECT filename FROM jobs WHERE job_id = $1", jobID).Scan(&filename)
	if err != nil {
		log.Printf("Failed to get job filename: %v", err)
		filename = jobID
	} else {
		if dotIndex := strings.LastIndex(filename, "."); dotIndex > 0 {
			filename = filename[:dotIndex]
		}
	}

	safeFilename := strings.ReplaceAll(filename, " ", "_")
	timestamp := time.Now().Format("20060102-150405")
	exportFilename := fmt.Sprintf("%s-%s-dataset", safeFilename, timestamp)

	switch format {
	case "json":
		c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=%s.json", exportFilename))
		c.Header("Content-Type", "application/json")
		c.JSON(http.StatusOK, allEntries)

	case "xlsx":
		f := excelize.NewFile()
		sheetName := "Dataset"
		f.SetSheetName("Sheet1", sheetName)

		// Set headers
		headers := []string{"ID", "Chunk", "Input", "Output", "Created At"}
		for i, header := range headers {
			cell := fmt.Sprintf("%c1", 'A'+i)
			f.SetCellValue(sheetName, cell, header)
		}

		// Style headers
		headerStyle, _ := f.NewStyle(&excelize.Style{
			Font: &excelize.Font{Bold: true},
			Fill: excelize.Fill{Type: "pattern", Color: []string{"#DDDDDD"}, Pattern: 1},
		})
		f.SetCellStyle(sheetName, "A1", "E1", headerStyle)

		// Add data rows
		for i, entry := range allEntries {
			row := i + 2
			f.SetCellValue(sheetName, fmt.Sprintf("A%d", row), entry.ID)
			f.SetCellValue(sheetName, fmt.Sprintf("B%d", row), entry.Chunk)
			f.SetCellValue(sheetName, fmt.Sprintf("C%d", row), entry.Input)
			f.SetCellValue(sheetName, fmt.Sprintf("D%d", row), entry.Output)
			f.SetCellValue(sheetName, fmt.Sprintf("E%d", row), entry.CreatedAt.Format("2006-01-02 15:04:05"))
		}

		// Set column widths
		f.SetColWidth(sheetName, "A", "A", 12)
		f.SetColWidth(sheetName, "B", "B", 60)
		f.SetColWidth(sheetName, "C", "C", 40)
		f.SetColWidth(sheetName, "D", "D", 40)
		f.SetColWidth(sheetName, "E", "E", 20)

		// Write to buffer
		var buf bytes.Buffer
		if err := f.Write(&buf); err != nil {
			log.Printf("Failed to write Excel file: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{
				"status":  "error",
				"message": "Failed to generate Excel file",
			})
			return
		}

		c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=%s.xlsx", exportFilename))
		c.Header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
		c.Data(http.StatusOK, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", buf.Bytes())

	case "csv":
		c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=%s.csv", exportFilename))
		c.Header("Content-Type", "text/csv; charset=utf-8")

		// Add UTF-8 BOM
		var buf bytes.Buffer
		buf.Write([]byte{0xEF, 0xBB, 0xBF})

		writer := csv.NewWriter(&buf)
		writer.Write([]string{"ID", "Chunk", "Input", "Output", "Created At"})

		for _, entry := range allEntries {
			writer.Write([]string{
				strconv.Itoa(entry.ID),
				entry.Chunk,
				entry.Input,
				entry.Output,
				entry.CreatedAt.Format("2006-01-02 15:04:05"),
			})
		}

		writer.Flush()
		if err := writer.Error(); err != nil {
			log.Printf("Error writing CSV: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{
				"status":  "error",
				"message": "Failed to generate CSV file",
			})
			return
		}

		c.Data(http.StatusOK, "text/csv; charset=utf-8", buf.Bytes())
	}
}

// ExportAllJobsDataset exports datasets from all jobs in the specified format
func (h *DatasetHandlers) ExportAllJobsDataset(c *gin.Context) {
	format := c.DefaultQuery("format", "json")

	if format != "json" && format != "xlsx" && format != "csv" {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "Invalid format specified. Supported formats: json, xlsx, csv",
		})
		return
	}

	rows, err := h.db.Query(`
		SELECT DISTINCT job_id FROM dataset
		ORDER BY job_id
	`)
	if err != nil {
		log.Printf("Failed to query job IDs: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("Failed to query jobs: %v", err),
		})
		return
	}
	defer rows.Close()

	var jobIDs []string
	for rows.Next() {
		var jobID string
		if err := rows.Scan(&jobID); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}
		jobIDs = append(jobIDs, jobID)
	}

	if err := rows.Err(); err != nil {
		log.Printf("Error iterating job rows: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("Error reading jobs: %v", err),
		})
		return
	}

	if len(jobIDs) == 0 {
		c.JSON(http.StatusNotFound, gin.H{
			"status":  "error",
			"message": "No jobs with datasets found",
		})
		return
	}

	var allEntries []DatasetEntryFormatted

	for _, jobID := range jobIDs {
		dataRows, err := h.db.Query(`
			SELECT id, input, output, chunk, created_at, page_number 
			FROM dataset 
			WHERE job_id = $1 
			ORDER BY page_number ASC, id ASC
		`, jobID)
		if err != nil {
			log.Printf("Failed to query dataset for job %s: %v", jobID, err)
			continue
		}

		for dataRows.Next() {
			var id int
			var input, output, chunk string
			var createdAt time.Time
			var pageNumber int

			if err := dataRows.Scan(&id, &input, &output, &chunk, &createdAt, &pageNumber); err != nil {
				log.Printf("Error scanning dataset row: %v", err)
				continue
			}

			allEntries = append(allEntries, DatasetEntryFormatted{
				Chunk:     chunk,
				CreatedAt: createdAt,
				ID:        id,
				Input:     input,
				Output:    output,
			})
		}
		dataRows.Close()
	}

	timestamp := time.Now().Format("20060102-150405")
	exportFilename := fmt.Sprintf("all-jobs-dataset-%s", timestamp)

	switch format {
	case "json":
		c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=%s.json", exportFilename))
		c.Header("Content-Type", "application/json")
		c.JSON(http.StatusOK, allEntries)

	case "xlsx":
		f := excelize.NewFile()
		sheetName := "Dataset"
		f.SetSheetName("Sheet1", sheetName)

		// Set headers
		headers := []string{"ID", "Chunk", "Input", "Output", "Created At"}
		for i, header := range headers {
			cell := fmt.Sprintf("%c1", 'A'+i)
			f.SetCellValue(sheetName, cell, header)
		}

		// Style headers
		headerStyle, _ := f.NewStyle(&excelize.Style{
			Font: &excelize.Font{Bold: true},
			Fill: excelize.Fill{Type: "pattern", Color: []string{"#DDDDDD"}, Pattern: 1},
		})
		f.SetCellStyle(sheetName, "A1", "E1", headerStyle)

		// Add data rows
		for i, entry := range allEntries {
			row := i + 2
			f.SetCellValue(sheetName, fmt.Sprintf("A%d", row), entry.ID)
			f.SetCellValue(sheetName, fmt.Sprintf("B%d", row), entry.Chunk)
			f.SetCellValue(sheetName, fmt.Sprintf("C%d", row), entry.Input)
			f.SetCellValue(sheetName, fmt.Sprintf("D%d", row), entry.Output)
			f.SetCellValue(sheetName, fmt.Sprintf("E%d", row), entry.CreatedAt.Format("2006-01-02 15:04:05"))
		}

		// Set column widths
		f.SetColWidth(sheetName, "A", "A", 12)
		f.SetColWidth(sheetName, "B", "B", 60)
		f.SetColWidth(sheetName, "C", "C", 40)
		f.SetColWidth(sheetName, "D", "D", 40)
		f.SetColWidth(sheetName, "E", "E", 20)

		// Write to buffer
		var buf bytes.Buffer
		if err := f.Write(&buf); err != nil {
			log.Printf("Failed to write Excel file: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{
				"status":  "error",
				"message": "Failed to generate Excel file",
			})
			return
		}

		c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=%s.xlsx", exportFilename))
		c.Header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
		c.Data(http.StatusOK, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", buf.Bytes())

	case "csv":
		c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=%s.csv", exportFilename))
		c.Header("Content-Type", "text/csv; charset=utf-8")

		// Add UTF-8 BOM
		var buf bytes.Buffer
		buf.Write([]byte{0xEF, 0xBB, 0xBF})

		writer := csv.NewWriter(&buf)
		writer.Write([]string{"ID", "Chunk", "Input", "Output", "Created At"})

		for _, entry := range allEntries {
			writer.Write([]string{
				strconv.Itoa(entry.ID),
				entry.Chunk,
				entry.Input,
				entry.Output,
				entry.CreatedAt.Format("2006-01-02 15:04:05"),
			})
		}

		writer.Flush()
		if err := writer.Error(); err != nil {
			log.Printf("Error writing CSV: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{
				"status":  "error",
				"message": "Failed to generate CSV file",
			})
			return
		}

		c.Data(http.StatusOK, "text/csv; charset=utf-8", buf.Bytes())
	}
}

// escapeCSVField escapes fields for CSV format
func escapeCSVField(field string) string {
	// Replace quotes with double quotes (CSV escaping)
	field = strings.ReplaceAll(field, "\"", "\"\"")

	// If the field contains commas, newlines, or quotes, wrap it in quotes
	if strings.ContainsAny(field, ",\"\r\n") {
		field = "\"" + field + "\""
	}

	return field
}
