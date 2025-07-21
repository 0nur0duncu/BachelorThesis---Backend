from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect, UploadFile, File, Form
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import logging
import json
import asyncio
import os
from utils.vectorstore import VectorStoreManager

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize the router
router = APIRouter(prefix="/api/v1/vectorstore", tags=["vectorstore"])

# Initialize the vector store manager
vector_store_manager = VectorStoreManager()

# Request models
class EmbedRequest(BaseModel):
    job_ids: List[str]
    model: str
    collection_name: str = "default_collection"

class SnapshotRequest(BaseModel):
    collection_name: str

class WebSocketMessage(BaseModel):
    job_ids: List[str]
    model: str

class QueryRequest(BaseModel):
    collection_name: str
    query_text: str
    limit: int = 20

# Add a new endpoint for querying collections by text
@router.post("/query")
async def query_collection(request: QueryRequest):
    """
    Query a collection for documents similar to the query text.
    """
    logger.info(f"Querying collection {request.collection_name} with text: {request.query_text}")
    
    try:
        # Check if collection exists
        collections = await vector_store_manager.list_collections()
        if not collections.get("success", False):
            return JSONResponse(
                status_code=500,
                content={"success": False, "message": "Failed to list collections"}
            )
            
        collection_names = [c["name"] for c in collections.get("collections", [])]
        if request.collection_name not in collection_names:
            return JSONResponse(
                status_code=404,
                content={"success": False, "message": f"Collection '{request.collection_name}' not found"}
            )
        
        # Get collection info to determine the embedding model
        collection_info = await vector_store_manager.get_collection_info(request.collection_name)
        if not collection_info.get("success", False):
            return JSONResponse(
                status_code=500,
                content={"success": False, "message": "Failed to get collection info"}
            )
            
        # Get the embedding model used in this collection
        model = collection_info.get("collection", {}).get("model_used", "text-embedding-3-small")
        
        # Get embedding for the query text
        embeddings = await vector_store_manager.get_embeddings([request.query_text], model)
        if not embeddings or len(embeddings) == 0:
            return JSONResponse(
                status_code=500,
                content={"success": False, "message": "Failed to generate embedding for query text"}
            )
            
        # Search for similar documents in Qdrant
        query_vector = embeddings[0]
        try:
            # Use search method instead of query
            search_results = vector_store_manager.qdrant_client.search(
                collection_name=request.collection_name,
                query_vector=query_vector,
                limit=request.limit
            )
            
            # Format the results
            results = []
            for result in search_results:
                results.append({
                    "score": result.score,
                    "payload": result.payload
                })
                
            return JSONResponse(content={
                "success": True,
                "results": results,
                "query": request.query_text,
                "collection": request.collection_name
            })
        except Exception as e:
            logger.error(f"Error searching Qdrant: {str(e)}")
            return JSONResponse(
                status_code=500,
                content={"success": False, "message": f"Error searching Qdrant: {str(e)}"}
            )
        
    except Exception as e:
        logger.error(f"Error in query_collection endpoint: {str(e)}")
        logger.exception("Exception details:")
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": f"Error processing request: {str(e)}"}
        )

# Connected WebSocket clients for cost estimation
cost_estimation_connections = {}

@router.websocket("/ws")
async def websocket_cost_endpoint(websocket: WebSocket):
    await websocket.accept()
    client_id = id(websocket)
    cost_estimation_connections[client_id] = websocket
    logger.info(f"WebSocket client connected: {client_id}")
    
    try:
        while True:
            # Wait for messages from the client
            data = await websocket.receive_text()
            try:
                # Parse the message
                message = json.loads(data)
                logger.info(f"Received message from client {client_id}: {message}")
                
                job_ids = message.get("job_ids", [])
                model = message.get("model", "text-embedding-3-small")
                
                # Fetch chunks to calculate cost
                all_chunks = []
                try:
                    for job_id in job_ids:
                        chunks = await vector_store_manager.fetch_job_chunks(job_id)
                        all_chunks.extend(chunks)
                    
                    # Calculate total character count
                    total_chars = sum(len(chunk.get("chunk", "")) for chunk in all_chunks)
                    
                    # Estimate tokens (approximate 4 chars per token)
                    estimated_tokens = total_chars // 4
                    
                    # Calculate cost based on model
                    cost_per_1m_tokens = {
                        "text-embedding-3-small": 0.01,
                        "text-embedding-3-large": 0.065,
                        "text-embedding-ada-002": 0.05
                    }.get(model, 0.01)
                    
                    estimated_cost = (estimated_tokens / 1000000) * cost_per_1m_tokens
                    
                    # Send cost estimation back to the client
                    await websocket.send_json({
                        "token_count": estimated_tokens,
                        "char_count": total_chars,
                        "estimated_cost": estimated_cost,
                        "model": model
                    })
                except Exception as e:
                    logger.error(f"Error calculating cost: {str(e)}")
                    await websocket.send_json({
                        "token_count": 0,
                        "char_count": 0,
                        "estimated_cost": 0,
                        "model": model,
                        "error": str(e)
                    })
                
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON received from client {client_id}")
                await websocket.send_json({"error": "Invalid JSON message"})
            except Exception as e:
                logger.error(f"Error processing message from client {client_id}: {str(e)}")
                await websocket.send_json({"error": str(e)})
    except WebSocketDisconnect:
        logger.info(f"WebSocket client disconnected: {client_id}")
    except Exception as e:
        logger.error(f"WebSocket error for client {client_id}: {str(e)}")
    finally:
        if client_id in cost_estimation_connections:
            del cost_estimation_connections[client_id]

@router.post("/embed")
async def embed_chunks(request: EmbedRequest):
    """
    Endpoint to embed chunks from selected jobs and store them in Qdrant.
    Asynchronously fetches chunks, creates embeddings, and stores them.
    """
    logger.info(f"Received embed request for jobs: {request.job_ids}")
    
    if not request.job_ids:
        return JSONResponse(
            status_code=400,
            content={"success": False, "message": "No job IDs provided"}
        )
    
    try:
        # Process the request asynchronously
        result = await vector_store_manager.embed_and_store_chunks(
            job_ids=request.job_ids,
            collection_name=request.collection_name,
            model=request.model
        )
        
        return JSONResponse(content=result)
    except Exception as e:
        logger.error(f"Error in embed_chunks endpoint: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": f"Error processing request: {str(e)}"}
        )

@router.post("/snapshot")
async def create_snapshot(request: SnapshotRequest):
    """
    Create a snapshot of a Qdrant collection.
    """
    logger.info(f"Creating snapshot for collection: {request.collection_name}")
    
    try:
        result = await vector_store_manager.create_snapshot(request.collection_name)
        return JSONResponse(content=result)
    except Exception as e:
        logger.error(f"Error creating snapshot: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": f"Error creating snapshot: {str(e)}"}
        )

@router.get("/snapshots/{collection_name}")
async def list_snapshots(collection_name: str):
    """
    List all snapshots for a collection.
    """
    logger.info(f"Listing snapshots for collection: {collection_name}")
    
    try:
        result = await vector_store_manager.list_snapshots(collection_name)
        return JSONResponse(content=result)
    except Exception as e:
        logger.error(f"Error listing snapshots: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": f"Error listing snapshots: {str(e)}"}
        )

@router.get("/collections")
async def list_collections():
    """
    List all collections and their information.
    """
    logger.info("Listing collections")
    
    try:
        result = await vector_store_manager.list_collections()
        return JSONResponse(content=result)
    except Exception as e:
        logger.error(f"Error in list_collections endpoint: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": f"Error listing collections: {str(e)}"}
        )

@router.get("/collections/{collection_name}")
async def get_collection_info(collection_name: str):
    """
    Get detailed information about a specific collection.
    """
    logger.info(f"Getting information for collection: {collection_name}")
    
    try:
        result = await vector_store_manager.get_collection_info(collection_name)
        return JSONResponse(content=result)
    except Exception as e:
        logger.error(f"Error in get_collection_info endpoint: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": f"Error getting collection info: {str(e)}"}
        )

@router.get("/snapshots/{collection_name}/download")
async def download_snapshot(collection_name: str, snapshot: str = None):
    """
    Download a snapshot for a collection.
    If snapshot ID is provided, download that specific snapshot,
    otherwise download the latest snapshot.
    """
    logger.info(f"Downloading snapshot for collection: {collection_name}, snapshot ID: {snapshot}")
    
    try:
        snapshot_path = await vector_store_manager.download_snapshot(collection_name, snapshot)
        if snapshot_path:
            # Extract the filename from the path without modifying it
            filename = os.path.basename(snapshot_path)
            
            logger.info(f"Serving snapshot file: {snapshot_path} with filename: {filename}")
            
            # Return the file with explicit content-disposition header
            # Use the original filename without modifications
            return FileResponse(
                path=snapshot_path,
                filename=filename,
                media_type="application/octet-stream",
                headers={
                    "Content-Disposition": f'attachment; filename="{filename}"'
                }
            )
        else:
            return JSONResponse(
                status_code=404,
                content={"success": False, "message": "Snapshot not found"}
            )
    except Exception as e:
        logger.error(f"Error downloading snapshot: {str(e)}")
        logger.exception("Exception details:")
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": f"Error downloading snapshot: {str(e)}"}
        )

@router.delete("/snapshots/{collection_name}/{snapshot_id}")
async def delete_snapshot(collection_name: str, snapshot_id: str):
    """
    Delete a snapshot from a collection.
    """
    logger.info(f"Deleting snapshot {snapshot_id} from collection: {collection_name}")
    
    try:
        result = await vector_store_manager.delete_snapshot(collection_name, snapshot_id)
        return JSONResponse(content=result)
    except Exception as e:
        logger.error(f"Error deleting snapshot: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": f"Error deleting snapshot: {str(e)}"}
        )

@router.delete("/collections/{collection_name}")
async def delete_collection(collection_name: str):
    """
    Delete a collection.
    """
    logger.info(f"Deleting collection: {collection_name}")
    
    try:
        result = await vector_store_manager.delete_collection(collection_name)
        return JSONResponse(content=result)
    except Exception as e:
        logger.error(f"Error deleting collection: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": f"Error deleting collection: {str(e)}"}
        )

@router.post("/snapshots/upload")
async def upload_and_restore_snapshot(
    file: UploadFile = File(...),
    collection_name: str = Form(...),
):
    """
    Upload a snapshot file and restore it to a collection.
    
    Args:
        file: Snapshot file to upload
        collection_name: Name of the collection to restore to
        
    Returns:
        JSON response with success status and message
    """
    logger.info(f"Uploading snapshot file for collection: {collection_name}")
    
    try:
        # Create directory for uploaded files if it doesn't exist
        os.makedirs("temp/uploads", exist_ok=True)
        
        # Make sure we don't double the .snapshot extension
        original_filename = file.filename
        base_name = original_filename
        if base_name.endswith('.snapshot'):
            base_name = base_name[:-9]  # Remove .snapshot extension
            
        # Generate unique filename for the uploaded file
        file_location = f"temp/uploads/{collection_name}_{base_name}"
        
        # Ensure the file has .snapshot extension (exactly once)
        if not file_location.endswith('.snapshot'):
            file_location += '.snapshot'
            
        logger.info(f"Snapshot file will be saved at: {file_location}")
        
        # Save the uploaded file as binary data without modifying it
        contents = await file.read()
        with open(file_location, "wb") as buffer:
            buffer.write(contents)
        
        logger.info(f"Snapshot file saved at: {file_location}")
        
        # Restore the snapshot
        result = await vector_store_manager.restore_snapshot(collection_name, file_location)
        return JSONResponse(content=result)
    except Exception as e:
        logger.error(f"Error uploading and restoring snapshot: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": f"Error uploading and restoring snapshot: {str(e)}"}
        ) 