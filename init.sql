-- Eklentileri kur (gerekirse)
CREATE EXTENSION IF NOT EXISTS citext;

-- jobs tablosunu oluştur
CREATE TABLE IF NOT EXISTS jobs (
    job_id VARCHAR(36) PRIMARY KEY,
    filename VARCHAR(255),
    status VARCHAR(50) DEFAULT 'pending', -- Changed default from 'queued' to 'pending'
    total_tasks INTEGER DEFAULT 0,
    completed_tasks INTEGER DEFAULT 0,
    excluded_pages INTEGER[] DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- worker_results tablosunu oluştur
CREATE TABLE IF NOT EXISTS worker_results (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(36) REFERENCES jobs(job_id),
    page_number INTEGER,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    path VARCHAR(255), -- This field will store the path to the PNG file
    text TEXT,
    processing_status VARCHAR(50) -- Added missing column for OCR queue status tracking
);

-- dataset tablosunu oluştur
CREATE TABLE IF NOT EXISTS dataset (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(36) NOT NULL,
    page_number INTEGER NOT NULL,
    input TEXT NOT NULL,
    output TEXT NOT NULL,
    chunk TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_job FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
);

-- token_usage tablosunu oluştur
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
);

-- settings tablosunu oluştur
CREATE TABLE IF NOT EXISTS settings (
    id SERIAL PRIMARY KEY,
    google_cloud_vision_api_key TEXT DEFAULT '',
    ocr_worker_count INTEGER DEFAULT 2,
    dataset_worker_count INTEGER DEFAULT 2,
    model_api_key TEXT DEFAULT '',
    model_provider TEXT DEFAULT 'gemini',
    model_name TEXT DEFAULT '',
    items_per_page INTEGER DEFAULT 10,
    test_model_api_endpoint TEXT DEFAULT '',
    dataset_generation_prompt TEXT DEFAULT 'Generate a synthetic dataset from this OCR text:

{{text}}',
    cohere_api_key TEXT DEFAULT '',
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Insert default settings if not exists
INSERT INTO settings (id, google_cloud_vision_api_key, ocr_worker_count, dataset_worker_count, model_api_key, model_provider, model_name, items_per_page, test_model_api_endpoint, dataset_generation_prompt, cohere_api_key)
SELECT 1, '', 2, 2, '', 'gemini', '', 10, '', 'Generate a synthetic dataset from this OCR text:

{{text}}', ''
WHERE NOT EXISTS (SELECT 1 FROM settings WHERE id = 1);

-- İndeksler oluştur
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_worker_results_job_id ON worker_results(job_id);
CREATE INDEX IF NOT EXISTS idx_worker_results_page ON worker_results(page_number);
CREATE INDEX IF NOT EXISTS idx_dataset_job_id ON dataset(job_id);
CREATE INDEX IF NOT EXISTS idx_token_usage_job_id ON token_usage(job_id);

-- Add index on path column for faster lookups
CREATE INDEX IF NOT EXISTS idx_worker_results_path ON worker_results(path);

-- Add index on processing_status column for faster OCR queue checks
CREATE INDEX IF NOT EXISTS idx_worker_results_processing_status ON worker_results(processing_status);

-- Add helpful comment for the processing_status column
COMMENT ON COLUMN worker_results.processing_status IS 'Tracks the processing status of OCR queue jobs (queued, processing, etc.)';

-- provider_settings tablosunu oluştur
CREATE TABLE IF NOT EXISTS provider_settings (
    id SERIAL PRIMARY KEY,
    provider TEXT NOT NULL UNIQUE,
    api_key TEXT NOT NULL DEFAULT '',
    model_name TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- AI modelleri için tablo oluştur
CREATE TABLE IF NOT EXISTS models (
    id SERIAL PRIMARY KEY,
    provider TEXT NOT NULL,
    name TEXT NOT NULL,
    display_name TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(provider, name)
);

-- Varsayılan modelleri ekle
INSERT INTO models (provider, name, display_name)
VALUES 
    ('gemini', 'gemini-1.5-flash-latest', 'Gemini 1.5 Flash Latest'),
    ('gemini', 'gemini-1.5-pro-latest', 'Gemini 1.5 Pro Latest'),
    ('openai', 'gpt-3.5-turbo', 'GPT-3.5 Turbo'),
    ('openai', 'gpt-4o', 'GPT-4o'),
    ('xai', 'grok-1', 'Grok-1')
ON CONFLICT (provider, name) DO NOTHING;

-- token_usage tablosunu güncelle
ALTER TABLE token_usage ADD COLUMN IF NOT EXISTS provider TEXT;
ALTER TABLE token_usage ADD COLUMN IF NOT EXISTS model_name TEXT;

-- Insert default provider settings for Gemini and OpenAI
INSERT INTO provider_settings (provider, api_key, model_name)
VALUES ('gemini', '', 'gemini-1.5-flash-latest')
ON CONFLICT (provider) DO NOTHING;

INSERT INTO provider_settings (provider, api_key, model_name)
VALUES ('openai', '', 'gpt-3.5-turbo')
ON CONFLICT (provider) DO NOTHING;

INSERT INTO provider_settings (provider, api_key, model_name)
VALUES ('xai', '', 'grok-1')
ON CONFLICT (provider) DO NOTHING;

-- Add index for faster provider lookups
CREATE INDEX IF NOT EXISTS idx_provider_settings_provider ON provider_settings(provider);