package handlers

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

// Define the cost constants for OpenAI models (batch API pricing)
const (
	EmbeddingCostSmall  = 0.01  // text-embedding-3-small per 1M tokens
	EmbeddingCostLarge  = 0.065 // text-embedding-3-large per 1M tokens
	EmbeddingCostAda002 = 0.05  // text-embedding-ada-002 per 1M tokens
)

// CostRequest represents a request for cost estimation from frontend
type CostRequest struct {
	JobIDs []string `json:"job_ids"`
	Model  string   `json:"model"`
}

// CostEstimation represents the response for cost estimation
type CostEstimation struct {
	TokenCount    int     `json:"token_count"`
	CharCount     int     `json:"char_count"`
	EstimatedCost float64 `json:"estimated_cost"`
	Model         string  `json:"model"`
}

// WebSocketClient represents a connected WebSocket client
type WebSocketClient struct {
	conn     *websocket.Conn
	jobIDs   []string
	model    string
	lastSent time.Time
}

// VectorStoreHandlers contains handlers for vector store operations
type VectorStoreHandlers struct {
	db            *sql.DB
	upgrader      websocket.Upgrader
	clients       map[*websocket.Conn]*WebSocketClient
	clientsMutex  sync.Mutex
	refreshTicker *time.Ticker
}

// NewVectorStoreHandlers creates a new VectorStoreHandlers instance
func NewVectorStoreHandlers(db *sql.DB) *VectorStoreHandlers {
	h := &VectorStoreHandlers{
		db: db,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins for development
			},
		},
		clients: make(map[*websocket.Conn]*WebSocketClient),
	}

	// Start a ticker to refresh all clients periodically (every 10 seconds)
	h.refreshTicker = time.NewTicker(10 * time.Second)
	go func() {
		for range h.refreshTicker.C {
			h.refreshAllClients()
		}
	}()

	return h
}

// WebSocketHandler handles WebSocket connections for vector cost estimation
func (h *VectorStoreHandlers) WebSocketHandler(c *gin.Context) {
	// Upgrade HTTP connection to WebSocket
	conn, err := h.upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("Error upgrading to WebSocket: %v", err)
		return
	}

	// Register new client
	client := &WebSocketClient{
		conn:     conn,
		jobIDs:   []string{},
		model:    "text-embedding-3-small", // Default model
		lastSent: time.Time{},
	}

	h.clientsMutex.Lock()
	h.clients[conn] = client
	h.clientsMutex.Unlock()

	log.Printf("New vector cost WebSocket client connected. Total clients: %d", len(h.clients))

	// Start handling messages in a goroutine
	go h.handleMessages(client)
}

// handleMessages processes incoming WebSocket messages
func (h *VectorStoreHandlers) handleMessages(client *WebSocketClient) {
	defer func() {
		// Clean up on disconnect
		h.clientsMutex.Lock()
		delete(h.clients, client.conn)
		h.clientsMutex.Unlock()
		client.conn.Close()
		log.Printf("Vector cost WebSocket client disconnected. Total clients: %d", len(h.clients))
	}()

	for {
		// Read message
		messageType, message, err := client.conn.ReadMessage()
		if err != nil {
			log.Printf("Error reading WebSocket message: %v", err)
			break
		}

		// Process message if it's text message
		if messageType == websocket.TextMessage {
			var request CostRequest
			if err := json.Unmarshal(message, &request); err != nil {
				log.Printf("Error unmarshalling cost request: %v", err)
				continue
			}

			// Update client with new job IDs and model
			h.clientsMutex.Lock()
			client.jobIDs = request.JobIDs
			client.model = request.Model
			h.clientsMutex.Unlock()

			// Calculate and send cost estimation
			h.sendCostEstimation(client)
		}
	}
}

// refreshAllClients sends updated cost estimations to all clients
func (h *VectorStoreHandlers) refreshAllClients() {
	h.clientsMutex.Lock()
	defer h.clientsMutex.Unlock()

	for _, client := range h.clients {
		// Only refresh if it's been more than 10 seconds since last update
		if time.Since(client.lastSent) > 10*time.Second {
			go h.sendCostEstimation(client)
		}
	}
}

// calculateCostForModel returns the cost per 1M tokens for a given model
func calculateCostForModel(model string) float64 {
	switch model {
	case "text-embedding-3-small":
		return EmbeddingCostSmall
	case "text-embedding-3-large":
		return EmbeddingCostLarge
	case "text-embedding-ada-002":
		return EmbeddingCostAda002
	default:
		return EmbeddingCostSmall // Default to small model cost
	}
}

// sendCostEstimation calculates and sends cost estimation to a client
func (h *VectorStoreHandlers) sendCostEstimation(client *WebSocketClient) {
	// Don't send if no job IDs
	if len(client.jobIDs) == 0 {
		return
	}

	h.clientsMutex.Lock()
	jobIDs := client.jobIDs
	model := client.model
	h.clientsMutex.Unlock()

	// Initialize counters
	tokenCount := 0
	charCount := 0

	// Query database for chunks from selected jobs
	// Build query with placeholders for multiple job IDs
	query := "SELECT chunk FROM dataset WHERE job_id IN ("
	args := make([]interface{}, len(jobIDs))
	for i, jobID := range jobIDs {
		if i > 0 {
			query += ", "
		}
		query += fmt.Sprintf("$%d", i+1)
		args[i] = jobID
	}
	query += ")"

	// Execute query
	rows, err := h.db.Query(query, args...)
	if err != nil {
		log.Printf("Error querying chunks for cost estimation: %v", err)
		return
	}
	defer rows.Close()

	// Process results
	for rows.Next() {
		var chunk string
		if err := rows.Scan(&chunk); err != nil {
			log.Printf("Error scanning chunk: %v", err)
			continue
		}

		// Calculate token count and character count
		chunkCharCount := len(chunk)
		charCount += chunkCharCount

		// Approximate token count (4 chars per token is a common approximation)
		chunkTokenCount := (chunkCharCount + 3) / 4 // Round up division
		tokenCount += chunkTokenCount
	}

	// Calculate cost based on model and token count
	costPerMillion := calculateCostForModel(model)
	estimatedCost := float64(tokenCount) / 1000000.0 * costPerMillion

	// Create response
	response := CostEstimation{
		TokenCount:    tokenCount,
		CharCount:     charCount,
		EstimatedCost: estimatedCost,
		Model:         model,
	}

	// Send response to client
	responseJSON, err := json.Marshal(response)
	if err != nil {
		log.Printf("Error marshalling cost estimation: %v", err)
		return
	}

	if err := client.conn.WriteMessage(websocket.TextMessage, responseJSON); err != nil {
		log.Printf("Error sending cost estimation: %v", err)
		return
	}

	// Update last sent time
	h.clientsMutex.Lock()
	client.lastSent = time.Now()
	h.clientsMutex.Unlock()
}

// GetCostEstimation calculates cost estimation for the given job IDs and model
// This is a REST endpoint alternative to the WebSocket
func (h *VectorStoreHandlers) GetCostEstimation(c *gin.Context) {
	var request CostRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid request format",
		})
		return
	}

	if len(request.JobIDs) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "No job IDs provided",
		})
		return
	}

	// Initialize counters
	tokenCount := 0
	charCount := 0

	// Query database for chunks from selected jobs
	// Build query with placeholders for multiple job IDs
	query := "SELECT chunk FROM dataset WHERE job_id IN ("
	args := make([]interface{}, len(request.JobIDs))
	for i, jobID := range request.JobIDs {
		if i > 0 {
			query += ", "
		}
		query += fmt.Sprintf("$%d", i+1)
		args[i] = jobID
	}
	query += ")"

	// Execute query
	rows, err := h.db.Query(query, args...)
	if err != nil {
		log.Printf("Error querying chunks for cost estimation: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Database error",
		})
		return
	}
	defer rows.Close()

	// Process results
	for rows.Next() {
		var chunk string
		if err := rows.Scan(&chunk); err != nil {
			log.Printf("Error scanning chunk: %v", err)
			continue
		}

		// Calculate token count and character count
		chunkCharCount := len(chunk)
		charCount += chunkCharCount

		// Approximate token count (4 chars per token is a common approximation)
		chunkTokenCount := (chunkCharCount + 3) / 4 // Round up division
		tokenCount += chunkTokenCount
	}

	// Calculate cost based on model and token count
	costPerMillion := calculateCostForModel(request.Model)
	estimatedCost := float64(tokenCount) / 1000000.0 * costPerMillion

	// Return response
	c.JSON(http.StatusOK, CostEstimation{
		TokenCount:    tokenCount,
		CharCount:     charCount,
		EstimatedCost: estimatedCost,
		Model:         request.Model,
	})
}

// RegisterRoutes registers the vector store API routes
func (h *VectorStoreHandlers) RegisterRoutes(router *gin.RouterGroup) {
	// WebSocket endpoint
	router.GET("/vectorcost/ws", h.WebSocketHandler)

	// REST endpoints
	router.POST("/vectorcost", h.GetCostEstimation)
}
