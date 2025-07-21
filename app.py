from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import logging
from dotenv import load_dotenv
import os
from handlers.embedding_handler import router as embedding_router
from handlers.vectorstore_handler import router as vectorstore_router

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/vectorstore_api.log")
    ]
)
logger = logging.getLogger(__name__)

# Create logs directory if it doesn't exist
os.makedirs("logs", exist_ok=True)

# Create the FastAPI app
app = FastAPI(
    title="Vector Store API",
    description="API for embedding generation and vector store management",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Include routers
app.include_router(embedding_router)
app.include_router(vectorstore_router)

@app.get("/")
async def root():
    return {"message": "Vector Store API is running"}

@app.get("/health")
async def health_check():
    return {"status": "ok"}

if __name__ == "__main__":
    # Run the FastAPI app
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=4545,
        reload=True
    ) 