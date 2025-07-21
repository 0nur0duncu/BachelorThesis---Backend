# Vector Store Service Setup

This service provides the backend functionality for embedding text chunks and storing them in a Qdrant vector database.

## Prerequisites

1. Python 3.8 or higher
2. Qdrant server (can be run using Docker)
3. OpenAI API key

## Installation

1. Clone the repository and navigate to the Backend directory:

```bash
cd Backend
```

2. Create a virtual environment and install dependencies:

### On Windows:
```bash
run_vectorstore_server.bat
# Press Ctrl+C to stop the server after dependencies are installed
```

### On Linux/Mac:
```bash
chmod +x run_vectorstore_server.sh
./run_vectorstore_server.sh
# Press Ctrl+C to stop the server after dependencies are installed
```

## Configuration

1. Copy the `env_example` file to `.env`:

```bash
cp env_example .env
```

2. Edit the `.env` file and add your OpenAI API key:

```
OPENAI_API_KEY=your_openai_api_key_here
```

## Running Qdrant

You can run Qdrant using Docker:

```bash
docker run -p 6333:6333 -p 6334:6334 qdrant/qdrant:latest
```

## Running the Vector Store Service

### On Windows:
```bash
run_vectorstore_server.bat
```

### On Linux/Mac:
```bash
chmod +x run_vectorstore_server.sh
./run_vectorstore_server.sh
```

The server will start on port 4545.

## API Endpoints

### Embedding Generation
- `POST /api/embeddings/` - Generate embeddings for a list of texts

### Vector Store Operations
- `POST /api/v1/vectorstore/embed` - Embed and store chunks from selected jobs
- `POST /api/v1/vectorstore/snapshot` - Create a snapshot of a collection
- `GET /api/v1/vectorstore/snapshots/{collection_name}` - List snapshots for a collection

### WebSocket
- `ws://localhost:4545/api/v1/vectorstore/ws` - Real-time cost estimation

## Usage with Frontend

The frontend app communicates with this server to:
1. Get real-time cost estimates via WebSocket
2. Submit job IDs for embedding and storage in Qdrant
3. Create and manage snapshots

## Troubleshooting

- If you see CORS errors, ensure the CORS middleware in `app.py` is configured correctly
- Check logs in `logs/vectorstore_api.log` for detailed error messages 