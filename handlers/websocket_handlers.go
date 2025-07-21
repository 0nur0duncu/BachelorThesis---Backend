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

// JobStats represents job statistics
type JobStats struct {
	Total      int       `json:"total"`
	Completed  int       `json:"completed"`
	Ready      int       `json:"ready"`
	InProgress int       `json:"inProgress"`
	Failed     int       `json:"failed"`
	Timestamp  time.Time `json:"timestamp"`
}

// TokenUsageStats represents token usage statistics
type TokenUsageStats struct {
	Status    string    `json:"status"`
	JobStats  []gin.H   `json:"job_stats"`
	Summary   gin.H     `json:"summary"`
	Timestamp time.Time `json:"timestamp"`
}

// WebSocketHandlers contains handlers for WebSocket connections
type WebSocketHandlers struct {
	db                *sql.DB
	jobStatsClients   map[*websocket.Conn]bool
	jobStatsMutex     sync.Mutex
	jobBroadcast      chan []byte
	tokenStatsClients map[*websocket.Conn]bool
	tokenStatsMutex   sync.Mutex
	tokenBroadcast    chan []byte
}

// JobStatsUpgrader configures the WebSocket upgrader for job statistics
var jobStatsUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// Allow all origins for development
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// TokenStatsUpgrader configures the WebSocket upgrader for token statistics
var tokenStatsUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// Allow all origins for development
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// NewWebSocketHandlers creates a new WebSocketHandlers instance
func NewWebSocketHandlers(db *sql.DB) *WebSocketHandlers {
	h := &WebSocketHandlers{
		db:                db,
		jobStatsClients:   make(map[*websocket.Conn]bool),
		jobBroadcast:      make(chan []byte),
		tokenStatsClients: make(map[*websocket.Conn]bool),
		tokenBroadcast:    make(chan []byte),
	}

	// Start the broadcasters
	go h.jobStatsBroadcaster()
	go h.tokenStatsBroadcaster()

	// Start collectors
	go h.jobStatsCollector()
	go h.tokenStatsCollector()

	return h
}

// JobStatsWebSocketHandler handles WebSocket connections for job statistics updates
func (h *WebSocketHandlers) JobStatsWebSocketHandler(c *gin.Context) {
	ws, err := jobStatsUpgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("Error upgrading to job stats websocket: %v", err)
		return
	}

	// Register new client
	h.jobStatsMutex.Lock()
	h.jobStatsClients[ws] = true
	h.jobStatsMutex.Unlock()

	log.Printf("New job stats WebSocket client connected. Total clients: %d", len(h.jobStatsClients))

	// Handle disconnection
	defer func() {
		h.jobStatsMutex.Lock()
		delete(h.jobStatsClients, ws)
		h.jobStatsMutex.Unlock()
		ws.Close()
		log.Printf("Job stats WebSocket client disconnected. Total clients: %d", len(h.jobStatsClients))
	}()

	// Send initial stats immediately
	stats, err := h.fetchJobStats()
	if err == nil {
		data, err := json.Marshal(stats)
		if err == nil {
			ws.WriteMessage(websocket.TextMessage, data)
		}
	}

	// Read messages (not used, but needed to detect disconnection)
	for {
		_, _, err := ws.ReadMessage()
		if err != nil {
			break
		}
	}
}

// TokenStatsWebSocketHandler handles WebSocket connections for token usage statistics
func (h *WebSocketHandlers) TokenStatsWebSocketHandler(c *gin.Context) {
	ws, err := tokenStatsUpgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("Error upgrading to token stats websocket: %v", err)
		return
	}

	// Register new client
	h.tokenStatsMutex.Lock()
	h.tokenStatsClients[ws] = true
	h.tokenStatsMutex.Unlock()

	log.Printf("New token stats WebSocket client connected. Total clients: %d", len(h.tokenStatsClients))

	// Handle disconnection
	defer func() {
		h.tokenStatsMutex.Lock()
		delete(h.tokenStatsClients, ws)
		h.tokenStatsMutex.Unlock()
		ws.Close()
		log.Printf("Token stats WebSocket client disconnected. Total clients: %d", len(h.tokenStatsClients))
	}()

	// Send initial stats immediately
	stats, err := h.fetchTokenUsageStats()
	if err == nil {
		data, err := json.Marshal(stats)
		if err == nil {
			ws.WriteMessage(websocket.TextMessage, data)
		}
	}

	// Read messages (not used, but needed to detect disconnection)
	for {
		_, _, err := ws.ReadMessage()
		if err != nil {
			break
		}
	}
}

// jobStatsBroadcaster sends job statistics to all connected clients
func (h *WebSocketHandlers) jobStatsBroadcaster() {
	for {
		msg := <-h.jobBroadcast

		h.jobStatsMutex.Lock()
		for client := range h.jobStatsClients {
			err := client.WriteMessage(websocket.TextMessage, msg)
			if err != nil {
				log.Printf("Error broadcasting job stats to client: %v", err)
				client.Close()
				delete(h.jobStatsClients, client)
			}
		}
		h.jobStatsMutex.Unlock()
	}
}

// tokenStatsBroadcaster sends token usage statistics to all connected clients
func (h *WebSocketHandlers) tokenStatsBroadcaster() {
	for {
		msg := <-h.tokenBroadcast

		h.tokenStatsMutex.Lock()
		for client := range h.tokenStatsClients {
			err := client.WriteMessage(websocket.TextMessage, msg)
			if err != nil {
				log.Printf("Error broadcasting token stats to client: %v", err)
				client.Close()
				delete(h.tokenStatsClients, client)
			}
		}
		h.tokenStatsMutex.Unlock()
	}
}

// jobStatsCollector periodically collects job statistics and broadcasts them
func (h *WebSocketHandlers) jobStatsCollector() {
	ticker := time.NewTicker(3 * time.Second) // Update every 3 seconds
	defer ticker.Stop()

	for range ticker.C {
		// Always fetch stats regardless of client count to ensure consistent timing
		stats, err := h.fetchJobStats()
		if err != nil {
			log.Printf("Error fetching job stats: %v", err)
			continue
		}

		// Add timestamp to track update frequency
		stats.Timestamp = time.Now()

		jsonData, err := json.Marshal(stats)
		if err != nil {
			log.Printf("Error marshalling job stats: %v", err)
			continue
		}

		// Only broadcast if there are clients
		h.jobStatsMutex.Lock()
		clientCount := len(h.jobStatsClients)
		h.jobStatsMutex.Unlock()

		if clientCount > 0 {
			log.Printf("Broadcasting job stats to %d clients at %s", clientCount, stats.Timestamp.Format("15:04:05.000"))
			h.jobBroadcast <- jsonData
		}
	}
}

// tokenStatsCollector periodically collects token usage statistics and broadcasts them
func (h *WebSocketHandlers) tokenStatsCollector() {
	ticker := time.NewTicker(5 * time.Second) // Update every 5 seconds
	defer ticker.Stop()

	for range ticker.C {
		// Always fetch stats regardless of client count to ensure consistent timing
		stats, err := h.fetchTokenUsageStats()
		if err != nil {
			log.Printf("Error fetching token usage stats: %v", err)
			continue
		}

		// Add timestamp to track update frequency
		stats.Timestamp = time.Now()

		jsonData, err := json.Marshal(stats)
		if err != nil {
			log.Printf("Error marshalling token usage stats: %v", err)
			continue
		}

		// Only broadcast if there are clients
		h.tokenStatsMutex.Lock()
		clientCount := len(h.tokenStatsClients)
		h.tokenStatsMutex.Unlock()

		if clientCount > 0 {
			log.Printf("Broadcasting token usage stats to %d clients at %s", clientCount, stats.Timestamp.Format("15:04:05.000"))
			h.tokenBroadcast <- jsonData
		}
	}
}

// fetchJobStats fetches current job statistics from the database
func (h *WebSocketHandlers) fetchJobStats() (JobStats, error) {
	var stats JobStats

	// Get total jobs count
	err := h.db.QueryRow("SELECT COUNT(*) FROM jobs").Scan(&stats.Total)
	if err != nil {
		return stats, err
	}

	// Get completed jobs count
	err = h.db.QueryRow("SELECT COUNT(*) FROM jobs WHERE status = 'completed'").Scan(&stats.Completed)
	if err != nil {
		return stats, err
	}

	// Get ready jobs count (using 'extracted' status as mentioned earlier)
	err = h.db.QueryRow("SELECT COUNT(*) FROM jobs WHERE status = 'extracted'").Scan(&stats.Ready)
	if err != nil {
		return stats, err
	}

	// Get in-progress jobs count
	err = h.db.QueryRow("SELECT COUNT(*) FROM jobs WHERE status IN ('processing', 'queued', 'extracting', 'queuing')").Scan(&stats.InProgress)
	if err != nil {
		return stats, err
	}

	// Get failed jobs count
	err = h.db.QueryRow("SELECT COUNT(*) FROM jobs WHERE status = 'failed'").Scan(&stats.Failed)
	if err != nil {
		return stats, err
	}

	return stats, nil
}

// fetchTokenUsageStats fetches token usage statistics from the database
func (h *WebSocketHandlers) fetchTokenUsageStats() (TokenUsageStats, error) {
	var stats TokenUsageStats
	stats.Status = "success"

	// Query from database with provider information
	rows, err := h.db.Query(`
		SELECT 
			t.job_id,
			j.filename, 
			COUNT(*) as request_count,
			SUM(prompt_tokens) as total_prompt_tokens,
			SUM(response_tokens) as total_response_tokens,
			SUM(total_tokens) as grand_total_tokens,
			AVG(total_tokens) as avg_tokens_per_request,
			t.provider,
			t.model_name
		FROM token_usage t
		JOIN jobs j ON t.job_id = j.job_id
		GROUP BY t.job_id, j.filename, t.provider, t.model_name
		ORDER BY grand_total_tokens DESC
	`)
	if err != nil {
		return stats, err
	}
	defer rows.Close()

	var jobStats []gin.H
	var totalRequests int64
	var totalPromptTokens, totalResponseTokens, grandTotalTokens int64
	var providerStats = make(map[string]int64) // Track total tokens by provider

	for rows.Next() {
		var jobID, filename string
		var requestCount, promptTokens, responseTokens, tokenTotal int64
		var avgTokens float64
		var provider, modelName string

		if err := rows.Scan(&jobID, &filename, &requestCount, &promptTokens, &responseTokens, &tokenTotal, &avgTokens, &provider, &modelName); err != nil {
			return stats, err
		}

		totalRequests += requestCount
		totalPromptTokens += promptTokens
		totalResponseTokens += responseTokens
		grandTotalTokens += tokenTotal

		// Track tokens by provider
		providerStats[provider] += tokenTotal

		jobStats = append(jobStats, gin.H{
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

	// Add provider distribution to summary
	var providerBreakdown []gin.H
	for provider, tokens := range providerStats {
		percentage := 0.0
		if grandTotalTokens > 0 {
			percentage = float64(tokens) * 100 / float64(grandTotalTokens)
		}
		providerBreakdown = append(providerBreakdown, gin.H{
			"provider":      provider,
			"tokens":        tokens,
			"percentage":    percentage,
			"formatted_pct": fmt.Sprintf("%.1f%%", percentage),
		})
	}

	stats.JobStats = jobStats
	stats.Summary = gin.H{
		"total_requests":        totalRequests,
		"total_prompt_tokens":   totalPromptTokens,
		"total_response_tokens": totalResponseTokens,
		"grand_total_tokens":    grandTotalTokens,
		"provider_breakdown":    providerBreakdown,
		"estimated_cost_usd":    fmt.Sprintf("$%.4f", float64(grandTotalTokens)*0.000003), // Example rate: $0.000003 per token
	}

	return stats, nil
}
