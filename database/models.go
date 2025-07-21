package database

import (
	"time"
)

// Job represents a task to be processed
type Job struct {
	JobID          string         `gorm:"column:job_id;primaryKey" json:"job_id"`
	Filename       string         `gorm:"column:filename;null" json:"filename"`
	Status         string         `gorm:"column:status;default:queued" json:"status"`
	TotalTasks     int            `gorm:"column:total_tasks;default:0" json:"total_tasks"`
	CompletedTasks int            `gorm:"column:completed_tasks;default:0" json:"completed_tasks"`
	ExcludedPages  []int          `json:"excluded_pages"`
	CreatedAt      time.Time      `gorm:"column:created_at;default:CURRENT_TIMESTAMP" json:"created_at"`
	Results        []WorkerResult `gorm:"foreignKey:JobID"`
}

// TableName specifies the table name for Job
func (Job) TableName() string {
	return "jobs"
}

// WorkerResult represents the result of a worker processing a job
type WorkerResult struct {
	ID          int       `gorm:"column:id;primaryKey;autoIncrement"`
	JobID       string    `gorm:"column:job_id;index"`
	PageNumber  int       `gorm:"column:page_number"`
	Text        string    `gorm:"column:text;type:text;null"`
	Path        string    `gorm:"column:path;type:varchar(255)"`
	ProcessedAt time.Time `gorm:"column:processed_at;default:CURRENT_TIMESTAMP"`
	Job         Job       `gorm:"foreignKey:JobID;references:JobID"`
}

// TableName specifies the table name for WorkerResult
func (WorkerResult) TableName() string {
	return "worker_results"
}
