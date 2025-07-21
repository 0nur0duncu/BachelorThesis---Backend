from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import logging
import os
import asyncio
import aiohttp
# from dotenv import load_dotenv

# Load environment variables
# load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize the router
router = APIRouter(prefix="/api/embeddings", tags=["embeddings"])

# Cache for the API key
OPENAI_API_KEY = None

async def get_openai_api_key() -> str:
    """
    Get the OpenAI API key from environment variable.
    """
    global OPENAI_API_KEY
    
    # Return cached key if available
    if OPENAI_API_KEY:
        return OPENAI_API_KEY
    
    # Try to get API key from environment variable
    OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
    
    if not OPENAI_API_KEY:
        raise ValueError("OpenAI API key is not set in environment variables")
    
    return OPENAI_API_KEY

# Request model
class EmbeddingRequest(BaseModel):
    texts: List[str]
    model: str = "text-embedding-3-small"

# Batch size for embedding API calls
BATCH_SIZE = 100

async def get_openai_embeddings(texts: List[str], model: str) -> List[List[float]]:
    """
    Get embeddings from OpenAI API.
    """
    api_key = await get_openai_api_key()
    if not api_key:
        raise ValueError("OpenAI API key is not set")
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    
    all_embeddings = []
    
    # Process in batches to avoid API limitations
    for i in range(0, len(texts), BATCH_SIZE):
        batch = texts[i:i+BATCH_SIZE]
        
        async with aiohttp.ClientSession() as session:
            try:
                url = "https://api.openai.com/v1/embeddings"
                payload = {
                    "input": batch,
                    "model": model
                }
                
                async with session.post(url, json=payload, headers=headers) as response:
                    if response.status == 200:
                        result = await response.json()
                        batch_embeddings = [item["embedding"] for item in result["data"]]
                        all_embeddings.extend(batch_embeddings)
                    else:
                        error_text = await response.text()
                        logger.error(f"OpenAI API error: {error_text}")
                        raise HTTPException(status_code=response.status, detail=f"OpenAI API error: {error_text}")
            except Exception as e:
                logger.error(f"Error getting embeddings from OpenAI: {str(e)}")
                raise HTTPException(status_code=500, detail=f"Error getting embeddings: {str(e)}")
    
    return all_embeddings

@router.post("/")
async def create_embeddings(request: EmbeddingRequest):
    """
    Create embeddings for a list of texts using the specified model.
    """
    logger.info(f"Creating embeddings for {len(request.texts)} texts using model {request.model}")
    
    try:
        embeddings = await get_openai_embeddings(request.texts, request.model)
        
        return JSONResponse(
            content={
                "success": True,
                "embeddings": embeddings,
                "count": len(embeddings),
                "model": request.model
            }
        )
    except Exception as e:
        logger.error(f"Error in create_embeddings endpoint: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": f"Error creating embeddings: {str(e)}"}
        ) 