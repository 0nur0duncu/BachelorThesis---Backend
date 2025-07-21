package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/BachelorThesis/Backend/config"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/streadway/amqp"
)

// HealthStatus represents the health status of system components
type HealthStatus struct {
	Backend   ComponentHealth `json:"backend"`
	RabbitMQ  ComponentHealth `json:"rabbitmq"`
	Database  ComponentHealth `json:"database"`
	Timestamp time.Time       `json:"timestamp"`
}

// ComponentHealth represents the health of a single system component
type ComponentHealth struct {
	Status       string `json:"status"` // "healthy", "degraded", or "unhealthy"
	Message      string `json:"message"`
	ResponseTime int64  `json:"responseTime"` // in milliseconds
}

// HealthHandlers contains handlers for health check operations
type HealthHandlers struct {
	db                *sql.DB
	healthClients     map[*websocket.Conn]bool
	healthMutex       sync.Mutex
	healthBroadcast   chan []byte
	healthCheckTicker *time.Ticker
	stopChan          chan struct{}
}

// HealthUpgrader configures the WebSocket upgrader for health status
var healthUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for development
	},
}

// NewHealthHandlers creates a new HealthHandlers instance
func NewHealthHandlers(db *sql.DB) *HealthHandlers {
	h := &HealthHandlers{
		db:              db,
		healthClients:   make(map[*websocket.Conn]bool),
		healthBroadcast: make(chan []byte),
		stopChan:        make(chan struct{}),
	}

	// Start the broadcaster and health checker goroutines
	go h.healthBroadcaster()
	h.startHealthChecker(10 * time.Second) // Check every 10 seconds

	return h
}

// GetHealthStatus returns the current health status of all components
func (h *HealthHandlers) GetHealthStatus(c *gin.Context) {
	status := h.checkHealth()

	c.JSON(http.StatusOK, status)
}

// HealthWebSocketHandler handles WebSocket connections for health status updates
func (h *HealthHandlers) HealthWebSocketHandler(c *gin.Context) {
	ws, err := healthUpgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("Error upgrading to health websocket: %v", err)
		return
	}

	// Register new client
	h.healthMutex.Lock()
	h.healthClients[ws] = true
	h.healthMutex.Unlock()

	log.Printf("New health WebSocket client connected. Total clients: %d", len(h.healthClients))

	// Handle disconnection
	defer func() {
		h.healthMutex.Lock()
		delete(h.healthClients, ws)
		h.healthMutex.Unlock()
		ws.Close()
		log.Printf("Health WebSocket client disconnected. Total clients: %d", len(h.healthClients))
	}()

	// Send initial health status immediately
	status := h.checkHealth()
	data, err := json.Marshal(status)
	if err == nil {
		ws.WriteMessage(websocket.TextMessage, data)
	}

	// Keep connection alive by reading messages (not used, but needed to detect disconnection)
	for {
		_, _, err := ws.ReadMessage()
		if err != nil {
			break
		}
	}
}

// healthBroadcaster sends health status to all connected clients
func (h *HealthHandlers) healthBroadcaster() {
	for {
		msg := <-h.healthBroadcast

		h.healthMutex.Lock()
		for client := range h.healthClients {
			err := client.WriteMessage(websocket.TextMessage, msg)
			if err != nil {
				log.Printf("Error broadcasting health status to client: %v", err)
				client.Close()
				delete(h.healthClients, client)
			}
		}
		h.healthMutex.Unlock()
	}
}

// startHealthChecker periodically checks health and broadcasts updates
func (h *HealthHandlers) startHealthChecker(interval time.Duration) {
	h.healthCheckTicker = time.NewTicker(interval)

	go func() {
		for {
			select {
			case <-h.healthCheckTicker.C:
				status := h.checkHealth()
				data, err := json.Marshal(status)
				if err != nil {
					log.Printf("Error marshalling health status: %v", err)
					continue
				}

				// Only broadcast if there are clients
				h.healthMutex.Lock()
				clientCount := len(h.healthClients)
				h.healthMutex.Unlock()

				if clientCount > 0 {
					h.healthBroadcast <- data
				}
			case <-h.stopChan:
				h.healthCheckTicker.Stop()
				return
			}
		}
	}()
}

// StopHealthChecker stops the health check ticker
func (h *HealthHandlers) StopHealthChecker() {
	close(h.stopChan)
}

// checkHealth performs health checks on all components
func (h *HealthHandlers) checkHealth() HealthStatus {
	status := HealthStatus{
		Timestamp: time.Now(),
	}

	// Check backend health (always healthy since we're running)
	status.Backend = ComponentHealth{
		Status:       "healthy",
		Message:      "Backend service is running",
		ResponseTime: 1, // Negligible time since this is the current process
	}

	// Check RabbitMQ health
	rabbitStart := time.Now()
	rabbitHealth := h.checkRabbitMQHealth()
	status.RabbitMQ = ComponentHealth{
		Status:       rabbitHealth.Status,
		Message:      rabbitHealth.Message,
		ResponseTime: time.Since(rabbitStart).Milliseconds(),
	}

	// Check Database health
	dbStart := time.Now()
	dbHealth := h.checkDatabaseHealth()
	status.Database = ComponentHealth{
		Status:       dbHealth.Status,
		Message:      dbHealth.Message,
		ResponseTime: time.Since(dbStart).Milliseconds(),
	}

	return status
}

// checkRabbitMQHealth checks the RabbitMQ connection status
func (h *HealthHandlers) checkRabbitMQHealth() ComponentHealth {
	health := ComponentHealth{
		Status:  "unhealthy",
		Message: "Failed to connect to RabbitMQ",
	}

	// Try to establish a connection with RabbitMQ
	conn, err := amqp.Dial(config.GetRabbitMQURL())
	if err != nil {
		return health
	}
	defer conn.Close()

	// Try to create a channel
	ch, err := conn.Channel()
	if err != nil {
		health.Message = "Connected to RabbitMQ but failed to create channel"
		health.Status = "degraded"
		return health
	}
	defer ch.Close()

	// Try to declare a test queue
	_, err = ch.QueueDeclare(
		"health_check", // name
		false,          // durable
		true,           // delete when unused
		false,          // exclusive
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		health.Message = "Connected to RabbitMQ but failed to declare a test queue"
		health.Status = "degraded"
		return health
	}

	health.Status = "healthy"
	health.Message = "RabbitMQ is reachable and operational"
	return health
}

// checkDatabaseHealth checks the database connection status
func (h *HealthHandlers) checkDatabaseHealth() ComponentHealth {
	health := ComponentHealth{
		Status:  "unhealthy",
		Message: "Failed to connect to database",
	}

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Try to ping the database
	err := h.db.PingContext(ctx)
	if err != nil {
		return health
	}

	// Try a simple query to verify read capability
	var count int
	err = h.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM jobs").Scan(&count)
	if err != nil {
		health.Status = "degraded"
		health.Message = "Database connection is up, but query failed"
		return health
	}

	health.Status = "healthy"
	health.Message = "Database is reachable and operational"
	return health
}
