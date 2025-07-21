# OCR Dataset Generation Platform

A comprehensive platform for processing documents through OCR (Optical Character Recognition) and generating synthetic datasets for language model training using various AI providers.

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [API Documentation](#api-documentation)
- [Deployment](#deployment)
- [Contributing](#contributing)
- [License](#license)

## 🎯 Overview

This platform automates the process of:

1. **Document Processing**: Upload PDFs or images for OCR processing
2. **OCR Extraction**: Extract text using Google Cloud Vision API
3. **Dataset Generation**: Create synthetic training datasets using AI models (Gemini, OpenAI, XAI)
4. **Vector Store Management**: Generate embeddings and manage vector databases
5. **Real-time Monitoring**: WebSocket-based real-time updates and monitoring

## ✨ Features

### Core Functionality

- 📄 **Multi-format Support**: Process PDFs and images
- 🔍 **Advanced OCR**: Google Cloud Vision API integration with Turkish and English support
- 🤖 **AI Dataset Generation**: Support for Gemini, OpenAI GPT, and XAI Grok models
- 📊 **Multiple Export Formats**: JSON, CSV, XLSX export options
- 🔄 **Queue Management**: RabbitMQ-based job processing
- 📈 **Real-time Monitoring**: WebSocket updates for job progress

### Advanced Features

- 🧮 **Vector Embeddings**: OpenAI embedding generation with cost estimation
- 💾 **Database Management**: PostgreSQL with comprehensive data models
- 🎛️ **Admin Controls**: Queue management and system health monitoring
- 📱 **Responsive UI**: Real-time dashboard with job management
- 🔧 **Configurable Settings**: Flexible model and provider configuration

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Frontend      │    │   Go Backend    │    │  Python FastAPI │
│   Dashboard     │◄──►│   Main API      │◄──►│  Vector Store   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │   PostgreSQL    │
                    │   Database      │
                    └─────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │   RabbitMQ      │
                    │   Message Queue │
                    └─────────────────┘
```

### Technology Stack

**Backend Services:**

- **Go**: Main API server with Gin framework
- **Python FastAPI**: Vector store and embedding services
- **PostgreSQL**: Primary database
- **RabbitMQ**: Message queue for job processing

**AI/ML Services:**

- **Google Cloud Vision API**: OCR processing
- **Google Gemini**: Dataset generation
- **OpenAI GPT**: Alternative dataset generation
- **XAI Grok**: Additional AI model support
- **OpenAI Embeddings**: Vector generation

**Infrastructure:**

- **Docker**: Containerization
- **WebSockets**: Real-time updates
- **CORS**: Cross-origin resource sharing

## 🔧 Prerequisites

- **Go** 1.23+
- **Python** 3.8+
- **Docker** and **Docker Compose**
- **PostgreSQL** 12+
- **RabbitMQ** 3.8+

### API Keys Required

- Google Cloud Vision API key
- Google Gemini API key (optional)
- OpenAI API key (optional)
- XAI API key (optional)

## 🚀 Installation

### 1. Clone the Repository

```bash
git clone <repository-url>
cd BachelorThesis/main/Backend
```

### 2. Environment Setup

Create `.env` file:

```bash
cp .env.example .env
```

Configure your `.env` file:

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_USER=guest
DB_PASSWORD=guest
DB_NAME=lisanstezi

# RabbitMQ Configuration
RABBITMQ_URL=amqp://guest:guest@localhost:5672/
RABBITMQ_USER=guest
RABBITMQ_PASS=guest
RABBITMQ_API_URL=http://localhost:15672/api

# API Keys
GOOGLE_CLOUD_VISION_API_KEY=your_google_vision_api_key
GEMINI_API_KEY=your_gemini_api_key
OPENAI_API_KEY=your_openai_api_key
XAI_API_KEY=your_xai_api_key

# Server Configuration
PORT=8080
```

### 3. Docker Setup (Recommended)

Start PostgreSQL and RabbitMQ:

```bash
docker-compose up -d db rabbitmq
```

Wait for services to be ready, then initialize the database:

```bash
# Database will be automatically initialized with init.sql
```

### 4. Go Backend Setup

Install dependencies:

```bash
go mod tidy
```

Run the Go backend:

```bash
go run main.go
```

The Go API will be available at `http://localhost:8080`

### 5. Python FastAPI Setup

Navigate to Python service directory and set up virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

Install Python dependencies:

```bash
pip install -r requirements.txt
```

Run the Python FastAPI service:

```bash
python app.py
```

The Python API will be available at `http://localhost:4545`

## ⚙️ Configuration

### Model Configuration

The platform supports multiple AI providers. Configure them in the settings:

1. **Gemini Configuration**:

   - Provider: `gemini`
   - Model: `gemini-2.5-pro-preview-03-25`
   - API Key: Set in environment variables

2. **OpenAI Configuration**:

   - Provider: `openai`
   - Model: `gpt-4` or `gpt-3.5-turbo`
   - API Key: Set in environment variables

3. **XAI Configuration**:
   - Provider: `xai`
   - Model: `grok-beta`
   - API Key: Set in environment variables

### Worker Configuration

Adjust worker counts in settings:

- **OCR Workers**: Number of concurrent OCR processing workers
- **Dataset Workers**: Number of concurrent dataset generation workers

## 📖 Usage

### 1. Document Upload and OCR

**Upload PDF:**

```bash
curl -X POST http://localhost:8080/api/v1/ocr/pdf \
  -F "file=@document.pdf" \
  -F "excluded_pages=[1,2]"
```

**Upload Image:**

```bash
curl -X POST http://localhost:8080/api/v1/ocr/image \
  -F "file=@image.jpg"
```

### 2. Start OCR Processing

**Process specific job:**

```bash
curl -X POST http://localhost:8080/api/v1/jobs/{jobID}/start
```

**Process all jobs:**

```bash
curl -X POST http://localhost:8080/api/v1/jobs/start-all
```

### 3. Generate Dataset

**Start dataset generation:**

```bash
curl -X POST http://localhost:8080/api/v1/dataset/generate
```

**Generate for specific job:**

```bash
curl -X POST http://localhost:8080/api/v1/dataset/jobs/{jobID}/generate
```

### 4. Export Data

**Export as JSON:**

```bash
curl -X GET http://localhost:8080/api/v1/dataset/jobs/{jobID}/export?format=json
```

**Export as XLSX:**

```bash
curl -X GET http://localhost:8080/api/v1/dataset/jobs/{jobID}/export?format=xlsx
```

### 5. Vector Store Operations

**Generate embeddings:**

```bash
curl -X POST http://localhost:4545/embeddings/generate \
  -H "Content-Type: application/json" \
  -d '{"job_ids": ["job1", "job2"], "model": "text-embedding-3-small"}'
```

**Get cost estimation:**

```bash
curl -X POST http://localhost:4545/vectorcost \
  -H "Content-Type: application/json" \
  -d '{"job_ids": ["job1"], "model": "text-embedding-3-small"}'
```

## 📚 API Documentation

### Main Go API Endpoints

#### Jobs Management

- `GET /api/v1/jobs` - List all jobs
- `GET /api/v1/jobs/{jobID}` - Get job details
- `POST /api/v1/jobs/{jobID}/start` - Start job processing
- `DELETE /api/v1/jobs/{jobID}` - Delete job

#### OCR Processing

- `POST /api/v1/ocr/pdf` - Upload PDF for OCR
- `POST /api/v1/ocr/image` - Upload image for OCR
- `POST /api/v1/process/start` - Start OCR processing

#### Dataset Generation

- `POST /api/v1/dataset/generate` - Generate datasets
- `GET /api/v1/dataset/jobs/{jobID}/pages/{pageNumber}` - Get page dataset
- `GET /api/v1/dataset/jobs/{jobID}/export` - Export job dataset

#### Administration

- `POST /api/v1/admin/queue/clear` - Clear OCR queue
- `POST /api/v1/admin/queue/clear-results` - Clear results queue
- `GET /api/v1/health` - Health check

#### WebSocket Endpoints

- `ws://localhost:8080/api/v1/ws/job-stats` - Job statistics updates
- `ws://localhost:8080/api/v1/ws/token-stats` - Token usage updates
- `ws://localhost:8080/api/v1/ws/health` - Health status updates

### Python FastAPI Endpoints

#### Vector Operations

- `POST /embeddings/generate` - Generate embeddings
- `POST /vectorcost` - Calculate embedding costs
- `ws://localhost:4545/vectorcost/ws` - Real-time cost updates

## 🐳 Deployment

### Docker Deployment

1. **Build and run all services:**

```bash
docker-compose up -d
```

2. **Scale workers:**

```bash
docker-compose up -d --scale backend=2
```

3. **View logs:**

```bash
docker-compose logs -f backend
```

### Production Deployment

1. **Environment Variables**: Set production API keys and configurations
2. **Database**: Use managed PostgreSQL service
3. **Message Queue**: Use managed RabbitMQ or Amazon MQ
4. **Load Balancer**: Configure load balancing for multiple instances
5. **Monitoring**: Set up logging and monitoring (Prometheus, Grafana)

### Health Checks

The platform includes comprehensive health checks:

- **Backend Service**: `GET /api/v1/health`
- **Database Connectivity**: Automatic database ping
- **RabbitMQ Connectivity**: Queue availability check
- **WebSocket Monitoring**: Real-time health updates

## 🔍 Monitoring and Debugging

### Logging

Logs are structured and include:

- **Request/Response logging**
- **Error tracking**
- **Performance metrics**
- **Token usage statistics**

### Real-time Monitoring

WebSocket connections provide real-time updates for:

- **Job Progress**: Processing status and completion
- **Token Usage**: AI model consumption tracking
- **System Health**: Service availability
- **Queue Status**: Message queue statistics

### Debug Mode

Enable debug logging:

```bash
export GIN_MODE=debug
go run main.go
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines

- Follow Go and Python best practices
- Add tests for new features
- Update documentation
- Ensure backward compatibility

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙋‍♂️ Support

For support and questions:

- Create an issue in the repository
- Check the documentation
- Review the API endpoints

## 🔮 Future Enhancements

- [ ] Support for additional AI providers
- [ ] Advanced document preprocessing
- [ ] Machine learning model training integration
- [ ] Enhanced vector database support
- [ ] Automated deployment pipelines
- [ ] Advanced analytics and reporting

---

Built with ❤️ for Bachelor Thesis project - OCR Dataset Generation Platform
