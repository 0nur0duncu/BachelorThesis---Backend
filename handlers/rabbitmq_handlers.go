package handlers

import (
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

type RabbitMQHandlers struct {
	db              *sql.DB
	purgeMutex      *sync.Mutex
	queueMutex      *sync.Mutex
	purgeInProgress *bool
	queuePaused     *bool
	rabbitmqUrl     string
	rabbitmqUser    string
	rabbitmqPass    string
	clients         map[*websocket.Conn]bool
	clientsMutex    sync.Mutex
	broadcast       chan []byte
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// Allow all origins for development
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func NewRabbitMQHandlers(
	db *sql.DB,
	purgeMutex *sync.Mutex,
	queueMutex *sync.Mutex,
	purgeInProgress *bool,
	queuePaused *bool,
) *RabbitMQHandlers {
	// Get RabbitMQ credentials from environment variables
	rabbitmqUser := os.Getenv("RABBITMQ_USER")
	if rabbitmqUser == "" {
		rabbitmqUser = "guest" // Default RabbitMQ username
	}

	rabbitmqPass := os.Getenv("RABBITMQ_PASS")
	if rabbitmqPass == "" {
		rabbitmqPass = "guest" // Default RabbitMQ password
	}

	rabbitmqUrl := os.Getenv("RABBITMQ_API_URL")
	if rabbitmqUrl == "" {
		rabbitmqUrl = "http://localhost:15672/api" // Default RabbitMQ Management API URL
	}

	h := &RabbitMQHandlers{
		db:              db,
		purgeMutex:      purgeMutex,
		queueMutex:      queueMutex,
		purgeInProgress: purgeInProgress,
		queuePaused:     queuePaused,
		rabbitmqUrl:     rabbitmqUrl,
		rabbitmqUser:    rabbitmqUser,
		rabbitmqPass:    rabbitmqPass,
		clients:         make(map[*websocket.Conn]bool),
		broadcast:       make(chan []byte),
	}

	// Start broadcaster goroutine
	go h.broadcaster()

	// Start data collection goroutine
	go h.collectRabbitMQData()

	return h
}

// GetRabbitMQOverview proxies requests to RabbitMQ's overview endpoint
func (h *RabbitMQHandlers) GetRabbitMQOverview(c *gin.Context) {
	// Create a new request to RabbitMQ
	req, err := http.NewRequest("GET", h.rabbitmqUrl+"/overview", nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create request"})
		return
	}

	// Set the Authorization header with credentials from env vars
	req.SetBasicAuth(h.rabbitmqUser, h.rabbitmqPass)

	// Make the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "Failed to connect to RabbitMQ"})
		return
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read response"})
		return
	}

	// Set the same status code
	c.Status(resp.StatusCode)

	// Set the content type
	c.Header("Content-Type", "application/json")

	// Write the response body
	c.Writer.Write(body)
}

// GetRabbitMQQueues proxies requests to RabbitMQ's queues endpoint
func (h *RabbitMQHandlers) GetRabbitMQQueues(c *gin.Context) {
	// Create a new request to RabbitMQ
	req, err := http.NewRequest("GET", h.rabbitmqUrl+"/queues", nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create request"})
		return
	}

	// Set the Authorization header with credentials from env vars
	req.SetBasicAuth(h.rabbitmqUser, h.rabbitmqPass)

	// Make the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "Failed to connect to RabbitMQ"})
		return
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read response"})
		return
	}

	// Set the same status code
	c.Status(resp.StatusCode)

	// Set the content type
	c.Header("Content-Type", "application/json")

	// Write the response body
	c.Writer.Write(body)
}

// WebSocketHandler handles WebSocket connections for real-time RabbitMQ updates
func (h *RabbitMQHandlers) WebSocketHandler(c *gin.Context) {
	// Upgrade HTTP request to WebSocket
	ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("Error upgrading to websocket: %v", err)
		return
	}

	// Register new client
	h.clientsMutex.Lock()
	h.clients[ws] = true
	h.clientsMutex.Unlock()

	log.Printf("New WebSocket client connected. Total clients: %d", len(h.clients))

	// Handle disconnection
	defer func() {
		h.clientsMutex.Lock()
		delete(h.clients, ws)
		h.clientsMutex.Unlock()
		ws.Close()
		log.Printf("WebSocket client disconnected. Total clients: %d", len(h.clients))
	}()

	// Read messages (not used, but needed to detect disconnection)
	for {
		_, _, err := ws.ReadMessage()
		if err != nil {
			break
		}
	}
}

// broadcaster sends messages to all connected clients
func (h *RabbitMQHandlers) broadcaster() {
	for {
		msg := <-h.broadcast

		h.clientsMutex.Lock()
		for client := range h.clients {
			err := client.WriteMessage(websocket.TextMessage, msg)
			if err != nil {
				log.Printf("Error broadcasting to client: %v", err)
				client.Close()
				delete(h.clients, client)
			}
		}
		h.clientsMutex.Unlock()
	}
}

// collectRabbitMQData periodically collects RabbitMQ data and broadcasts it
func (h *RabbitMQHandlers) collectRabbitMQData() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		h.clientsMutex.Lock()
		clientCount := len(h.clients)
		h.clientsMutex.Unlock()

		// Only fetch data if there are active clients
		if clientCount > 0 {
			// Fetch overview data
			overviewData, err := h.fetchRabbitMQData("/overview")
			if err != nil {
				log.Printf("Error fetching RabbitMQ overview data: %v", err)
				continue
			}

			// Fetch queues data
			queuesData, err := h.fetchRabbitMQData("/queues")
			if err != nil {
				log.Printf("Error fetching RabbitMQ queues data: %v", err)
				continue
			}

			// Create combined data packet
			data := map[string]interface{}{
				"type":     "update",
				"overview": overviewData,
				"queues":   queuesData,
				"time":     time.Now().Format(time.RFC3339),
			}

			// Convert to JSON and broadcast
			jsonData, err := json.Marshal(data)
			if err != nil {
				log.Printf("Error marshalling data: %v", err)
				continue
			}

			h.broadcast <- jsonData
		}
	}
}

// fetchRabbitMQData fetches data from RabbitMQ management API
func (h *RabbitMQHandlers) fetchRabbitMQData(path string) (interface{}, error) {
	// Use credentials from environment variables
	req, err := http.NewRequest("GET", h.rabbitmqUrl+path, nil)
	if err != nil {
		return nil, err
	}

	// Using basic auth with credentials from env vars
	req.SetBasicAuth(h.rabbitmqUser, h.rabbitmqPass)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var data interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, err
	}

	return data, nil
}
