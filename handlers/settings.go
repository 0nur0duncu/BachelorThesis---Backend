package handlers

import (
	"database/sql"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type Settings struct {
	GoogleCloudVisionAPIKey string `json:"google_cloud_vision_api_key"`
	OCRWorkerCount          int    `json:"ocr_worker_count"`
	DatasetWorkerCount      int    `json:"dataset_worker_count"`
	ModelAPIKey             string `json:"model_api_key"`
	ModelProvider           string `json:"model_provider"`
	ModelName               string `json:"model_name"`
	ItemsPerPage            int    `json:"items_per_page"`
	TestModelAPIEndpoint    string `json:"test_model_api_endpoint"`
	DatasetGenerationPrompt string `json:"dataset_generation_prompt"`
	CohereAPIKey            string `json:"cohere_api_key"`
}

// ProviderSettings represents provider-specific stored settings
type ProviderSettings struct {
	Provider  string `json:"provider"`
	APIKey    string `json:"api_key"`
	ModelName string `json:"model_name"`
}

func GetSettings(db *sql.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		var settings Settings
		err := db.QueryRow(`
			SELECT 
				google_cloud_vision_api_key,
				ocr_worker_count,
				dataset_worker_count,
				model_api_key,
				model_provider,
				model_name,
				items_per_page,
				test_model_api_endpoint,
				dataset_generation_prompt,
				cohere_api_key
			FROM settings WHERE id = 1
		`).Scan(
			&settings.GoogleCloudVisionAPIKey,
			&settings.OCRWorkerCount,
			&settings.DatasetWorkerCount,
			&settings.ModelAPIKey,
			&settings.ModelProvider,
			&settings.ModelName,
			&settings.ItemsPerPage,
			&settings.TestModelAPIEndpoint,
			&settings.DatasetGenerationPrompt,
			&settings.CohereAPIKey,
		)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"status":  "error",
				"message": "Failed to fetch settings",
			})
			return
		}

		c.JSON(http.StatusOK, settings)
	}
}

func UpdateSettings(db *sql.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		var settings Settings
		if err := c.ShouldBindJSON(&settings); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"status":  "error",
				"message": "Invalid request data",
			})
			return
		}

		_, err := db.Exec(`
			UPDATE settings SET
				google_cloud_vision_api_key = $1,
				ocr_worker_count = $2,
				dataset_worker_count = $3,
				model_api_key = $4,
				model_provider = $5,
				model_name = $6,
				items_per_page = $7,
				test_model_api_endpoint = $8,
				dataset_generation_prompt = $9,
				cohere_api_key = $10,
				updated_at = $11
			WHERE id = 1
		`,
			settings.GoogleCloudVisionAPIKey,
			settings.OCRWorkerCount,
			settings.DatasetWorkerCount,
			settings.ModelAPIKey,
			settings.ModelProvider,
			settings.ModelName,
			settings.ItemsPerPage,
			settings.TestModelAPIEndpoint,
			settings.DatasetGenerationPrompt,
			settings.CohereAPIKey,
			time.Now(),
		)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"status":  "error",
				"message": "Failed to update settings",
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"status":  "success",
			"message": "Settings updated successfully",
		})
	}
}

// GetProviderSettings retrieves settings for a specific provider
func GetProviderSettings(db *sql.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Ensure the provider_settings table exists
		_, err := db.Exec(`
			CREATE TABLE IF NOT EXISTS provider_settings (
				id SERIAL PRIMARY KEY,
				provider TEXT NOT NULL UNIQUE,
				api_key TEXT NOT NULL DEFAULT '',
				model_name TEXT NOT NULL DEFAULT '',
				created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
			)
		`)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"status":  "error",
				"message": "Failed to ensure provider settings table",
				"error":   err.Error(),
			})
			return
		}

		// Get provider from query
		provider := c.Query("provider")
		if provider == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"status":  "error",
				"message": "Provider parameter is required",
			})
			return
		}

		// Fetch provider settings
		var settings ProviderSettings
		settings.Provider = provider

		err = db.QueryRow(`
			SELECT api_key, model_name FROM provider_settings WHERE provider = $1
		`, provider).Scan(&settings.APIKey, &settings.ModelName)

		if err != nil {
			// If settings don't exist for this provider yet, return empty settings
			if err == sql.ErrNoRows {
				c.JSON(http.StatusOK, settings)
				return
			}

			c.JSON(http.StatusInternalServerError, gin.H{
				"status":  "error",
				"message": "Failed to fetch provider settings",
				"error":   err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, settings)
	}
}

// UpdateProviderSettings saves settings for a specific provider
func UpdateProviderSettings(db *sql.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Ensure the provider_settings table exists
		_, err := db.Exec(`
			CREATE TABLE IF NOT EXISTS provider_settings (
				id SERIAL PRIMARY KEY,
				provider TEXT NOT NULL UNIQUE,
				api_key TEXT NOT NULL DEFAULT '',
				model_name TEXT NOT NULL DEFAULT '',
				created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
			)
		`)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"status":  "error",
				"message": "Failed to ensure provider settings table",
				"error":   err.Error(),
			})
			return
		}

		// Parse request body
		var settings ProviderSettings
		if err := c.ShouldBindJSON(&settings); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"status":  "error",
				"message": "Invalid request data",
				"error":   err.Error(),
			})
			return
		}

		// Validate provider
		if settings.Provider == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"status":  "error",
				"message": "Provider is required",
			})
			return
		}

		// Insert or update provider settings using UPSERT pattern
		_, err = db.Exec(`
			INSERT INTO provider_settings (provider, api_key, model_name, updated_at)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (provider) 
			DO UPDATE SET 
				api_key = $2,
				model_name = $3,
				updated_at = $4
		`, settings.Provider, settings.APIKey, settings.ModelName, time.Now())

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"status":  "error",
				"message": "Failed to update provider settings",
				"error":   err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"status":  "success",
			"message": "Provider settings updated successfully",
		})
	}
}

// GetOpenAIAPIKey retrieves the OpenAI API key from the database
func GetOpenAIAPIKey(db *sql.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		var apiKey string
		err := db.QueryRow(`
			SELECT model_api_key 
			FROM settings 
			WHERE model_provider = 'openai' 
			AND id = 1
		`).Scan(&apiKey)

		if err != nil {
			if err == sql.ErrNoRows {
				c.JSON(http.StatusNotFound, gin.H{
					"status":  "error",
					"message": "OpenAI API key not found",
				})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{
				"status":  "error",
				"message": "Failed to fetch OpenAI API key",
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"api_key": apiKey,
		})
	}
}
