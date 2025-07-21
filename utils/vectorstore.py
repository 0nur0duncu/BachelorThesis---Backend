from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
import aiohttp
import asyncio
import logging
from typing import List, Dict, Any
import os
import requests

logger = logging.getLogger(__name__)

# Embedding dimensions for different models
MODEL_DIMENSIONS = {
    "text-embedding-3-small": 1536,
    "text-embedding-3-large": 3072,
    "text-embedding-ada-002": 1536
}

class VectorStoreManager:
    def __init__(self, qdrant_url: str = "http://localhost:6333", api_key: str = None):
        self.qdrant_client = QdrantClient(url=qdrant_url, api_key=api_key)
        self.embedding_service_url = "http://localhost:4545/api/embeddings"  # Updated port from 8000 to 4545
    
    def ensure_collection_exists(self, collection_name: str, embedding_model: str):
        """Ensure the collection exists, creating it if necessary."""
        try:
            # Check if collection exists
            collections = self.qdrant_client.get_collections().collections
            collection_names = [collection.name for collection in collections]
            
            if collection_name not in collection_names:
                # Create collection with the right vector dimensions for the model
                vector_size = MODEL_DIMENSIONS.get(embedding_model, 1536)  # Default to 1536 if model not recognized
                self.qdrant_client.create_collection(
                    collection_name=collection_name,
                    vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE)
                )
                logger.info(f"Created new collection '{collection_name}' with vector size {vector_size}")
            else:
                logger.info(f"Collection '{collection_name}' already exists")
            
            return True
        except Exception as e:
            logger.error(f"Error ensuring collection exists: {str(e)}")
            return False
    
    async def fetch_job_chunks(self, job_id: str) -> List[Dict[str, Any]]:
        """Fetch chunks for a specific job ID from the JSON export API."""
        async with aiohttp.ClientSession() as session:
            url = f"http://localhost:8080/api/v1/dataset/job/{job_id}/export"
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        chunks = await response.json()
                        logger.info(f"Fetched {len(chunks)} chunks for job {job_id}")
                        
                        # Ensure job_id is set for each chunk
                        for chunk in chunks:
                            if 'job_id' not in chunk or not chunk['job_id']:
                                chunk['job_id'] = job_id
                        
                        return chunks
                    else:
                        error_text = await response.text()
                        logger.error(f"Error fetching chunks for job {job_id}: {error_text}")
                        return []
            except Exception as e:
                logger.error(f"Exception fetching chunks for job {job_id}: {str(e)}")
                return []
    
    async def get_embeddings(self, texts: List[str], model: str) -> List[List[float]]:
        """Get embeddings for a list of texts using the embedding service."""
        async with aiohttp.ClientSession() as session:
            try:
                payload = {
                    "texts": texts,
                    "model": model
                }
                async with session.post(self.embedding_service_url, json=payload) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result.get("embeddings", [])
                    else:
                        error_text = await response.text()
                        logger.error(f"Error getting embeddings: {error_text}")
                        return []
            except Exception as e:
                logger.error(f"Exception getting embeddings: {str(e)}")
                return []
    
    async def embed_and_store_chunks(
        self, 
        job_ids: List[str], 
        collection_name: str, 
        model: str
    ) -> Dict[str, Any]:
        """Fetch chunks for multiple jobs, get embeddings, and store in Qdrant."""
        # First ensure the collection exists
        if not self.ensure_collection_exists(collection_name, model):
            return {"success": False, "message": "Failed to create or verify collection"}
        
        # Fetch chunks for all jobs in parallel
        fetch_tasks = [self.fetch_job_chunks(job_id) for job_id in job_ids]
        all_chunks_lists = await asyncio.gather(*fetch_tasks)
        
        # Flatten the list of chunk lists and ensure job_id is set for each chunk
        all_chunks = []
        for i, chunks_list in enumerate(all_chunks_lists):
            job_id = job_ids[i]
            for chunk in chunks_list:
                # Ensure each chunk has a job_id
                if 'job_id' not in chunk or not chunk['job_id']:
                    chunk['job_id'] = job_id
            all_chunks.extend(chunks_list)
        
        if not all_chunks:
            return {"success": False, "message": "No chunks found for the selected jobs"}
        
        # Extract text from chunks
        texts = [chunk.get("chunk", "") for chunk in all_chunks]
        
        # Get embeddings for all texts
        embeddings = await self.get_embeddings(texts, model)
        
        if not embeddings or len(embeddings) != len(texts):
            return {"success": False, "message": "Failed to generate embeddings for chunks"}
        
        # Prepare points for Qdrant
        points = []
        for i, (chunk, embedding) in enumerate(zip(all_chunks, embeddings)):
            # Log metadata for debugging
            logger.info(f"Chunk metadata: job_id={chunk.get('job_id', 'MISSING')}, id={chunk.get('id', 'MISSING')}")
            
            metadata = {
                "job_id": chunk.get("job_id", ""),
                "chunk_id": chunk.get("id", 0),
                "page_number": chunk.get("page_number", 0),
                "created_at": chunk.get("created_at", ""),
                "text": chunk.get("chunk", "")
            }
            points.append(PointStruct(
                id=i,  # Use sequential IDs
                vector=embedding,
                payload=metadata
            ))
        
        # Store points in Qdrant in batches
        batch_size = 100  # Adjust based on your needs
        for i in range(0, len(points), batch_size):
            batch = points[i:i+batch_size]
            try:
                self.qdrant_client.upsert(
                    collection_name=collection_name,
                    points=batch
                )
                logger.info(f"Stored batch of {len(batch)} points in Qdrant")
            except Exception as e:
                logger.error(f"Error storing embeddings in Qdrant: {str(e)}")
                return {"success": False, "message": f"Error storing embeddings: {str(e)}"}
        
        return {
            "success": True,
            "message": f"Successfully embedded and stored {len(points)} chunks in collection '{collection_name}'",
            "embedded_count": len(points),
            "collection": collection_name
        }

    async def create_snapshot(self, collection_name: str) -> Dict[str, Any]:
        """Create a snapshot of the collection."""
        try:
            result = self.qdrant_client.create_snapshot(collection_name=collection_name)
            return {
                "success": True,
                "message": f"Successfully created snapshot for collection '{collection_name}'",
                "snapshot_name": result.name
            }
        except Exception as e:
            logger.error(f"Error creating snapshot: {str(e)}")
            return {"success": False, "message": f"Error creating snapshot: {str(e)}"}

    async def list_snapshots(self, collection_name: str) -> Dict[str, Any]:
        """List all snapshots for a collection."""
        try:
            snapshot_list = self.qdrant_client.list_snapshots(collection_name=collection_name)
            formatted_snapshots = []
            
            for snapshot in snapshot_list:
                formatted_snapshots.append({
                    "id": snapshot.name,  # Use snapshot name as ID
                    "name": snapshot.name,
                    "created_at": snapshot.creation_time,
                    "size_bytes": snapshot.size if hasattr(snapshot, 'size') else None,
                    "path": snapshot.location if hasattr(snapshot, 'location') else None
                })
            
            return {
                "success": True,
                "snapshots": formatted_snapshots
            }
        except Exception as e:
            logger.error(f"Error listing snapshots: {str(e)}")
            return {"success": False, "message": f"Error listing snapshots: {str(e)}"}

    async def list_collections(self) -> Dict[str, Any]:
        """List all collections and their information."""
        try:
            collections_info = []
            collections = self.qdrant_client.get_collections().collections
            
            for collection in collections:
                try:
                    collection_info = self.qdrant_client.get_collection(collection.name)
                    points_count = self.qdrant_client.count(collection_name=collection.name).count
                    
                    # Try to determine which model was used based on vector size
                    vector_size = collection_info.config.params.vectors.size
                    model_used = "unknown"
                    for model_name, dim in MODEL_DIMENSIONS.items():
                        if dim == vector_size:
                            model_used = model_name
                            break
                    
                    collections_info.append({
                        "name": collection.name,
                        "points_count": points_count,
                        "vector_size": vector_size,
                        "distance": str(collection_info.config.params.vectors.distance),
                        "model_used": model_used,
                        "created_at": collection_info.config.params.created_at if hasattr(collection_info.config.params, 'created_at') else None
                    })
                except Exception as e:
                    logger.error(f"Error getting info for collection {collection.name}: {str(e)}")
                    collections_info.append({
                        "name": collection.name,
                        "error": str(e)
                    })
            
            return {
                "success": True,
                "collections": collections_info
            }
        except Exception as e:
            logger.error(f"Error listing collections: {str(e)}")
            return {"success": False, "message": f"Error listing collections: {str(e)}"}
    
    async def get_collection_info(self, collection_name: str) -> Dict[str, Any]:
        """Get detailed information about a specific collection."""
        try:
            collection_info = self.qdrant_client.get_collection(collection_name)
            points_count = self.qdrant_client.count(collection_name=collection_name).count
            
            # Get a sample point to see metadata structure (if any points exist)
            sample_point = None
            if points_count > 0:
                try:
                    # Use search instead of query
                    vector_size = collection_info.config.params.vectors.size
                    zero_vector = [0.0] * vector_size
                    search_result = self.qdrant_client.search(
                        collection_name=collection_name,
                        query_vector=zero_vector,
                        limit=1
                    )
                    if search_result and len(search_result) > 0:
                        sample_point = search_result[0].payload
                except Exception as e:
                    logger.warning(f"Failed to retrieve sample point: {str(e)}")
                    # Continue without sample point
            
            # Try to determine which model was used based on vector size
            vector_size = collection_info.config.params.vectors.size
            model_used = "unknown"
            for model_name, dim in MODEL_DIMENSIONS.items():
                if dim == vector_size:
                    model_used = model_name
                    break
            
            return {
                "success": True,
                "collection": {
                    "name": collection_name,
                    "points_count": points_count,
                    "vector_size": vector_size,
                    "distance": str(collection_info.config.params.vectors.distance),
                    "model_used": model_used,
                    "created_at": collection_info.config.params.created_at if hasattr(collection_info.config.params, 'created_at') else None,
                    "sample_point": sample_point
                }
            }
        except Exception as e:
            logger.error(f"Error getting collection info: {str(e)}")
            return {"success": False, "message": f"Error getting collection info: {str(e)}"}

    async def delete_snapshot(self, collection_name: str, snapshot_id: str) -> Dict[str, Any]:
        """Delete a snapshot from a collection."""
        try:
            self.qdrant_client.delete_snapshot(collection_name=collection_name, snapshot_name=snapshot_id)
            return {
                "success": True,
                "message": f"Successfully deleted snapshot '{snapshot_id}' from collection '{collection_name}'"
            }
        except Exception as e:
            logger.error(f"Error deleting snapshot: {str(e)}")
            return {"success": False, "message": f"Error deleting snapshot: {str(e)}"}

    async def delete_collection(self, collection_name: str) -> Dict[str, Any]:
        """Delete a collection and its associated snapshots."""
        try:
            # First list and delete all snapshots associated with this collection
            try:
                snapshot_list = self.qdrant_client.list_snapshots(collection_name=collection_name)
                for snapshot in snapshot_list:
                    logger.info(f"Deleting snapshot {snapshot.name} from collection {collection_name}")
                    try:
                        self.qdrant_client.delete_snapshot(collection_name=collection_name, snapshot_name=snapshot.name)
                    except Exception as e:
                        logger.warning(f"Failed to delete snapshot {snapshot.name}: {str(e)}")
                        # Continue with other snapshots
            except Exception as e:
                logger.warning(f"Failed to list snapshots for collection {collection_name}: {str(e)}")
                # Continue with collection deletion
            
            # Delete local snapshot files if they exist
            try:
                snapshot_dir = "temp/snapshots"
                if os.path.exists(snapshot_dir):
                    for filename in os.listdir(snapshot_dir):
                        if filename.startswith(f"{collection_name}_") or filename.startswith(f"{collection_name}-"):
                            file_path = os.path.join(snapshot_dir, filename)
                            logger.info(f"Deleting local snapshot file: {file_path}")
                            os.remove(file_path)
            except Exception as e:
                logger.warning(f"Failed to delete local snapshot files: {str(e)}")
                # Continue with collection deletion
            
            # Now delete the collection
            self.qdrant_client.delete_collection(collection_name=collection_name)
            return {
                "success": True,
                "message": f"Successfully deleted collection '{collection_name}' and its snapshots"
            }
        except Exception as e:
            logger.error(f"Error deleting collection: {str(e)}")
            return {"success": False, "message": f"Error deleting collection: {str(e)}"}

    async def restore_snapshot(self, collection_name: str, snapshot_file: str) -> Dict[str, Any]:
        """
        Restore a collection from an uploaded snapshot file.
        
        Args:
            collection_name: Name of the collection to restore to
            snapshot_file: Path to the uploaded snapshot file
            
        Returns:
            Dictionary with success status and message
        """
        try:
            # Create temporary directory for uploads if it doesn't exist
            os.makedirs("temp/uploads", exist_ok=True)
            
            # Check if the snapshot file exists
            if not os.path.exists(snapshot_file):
                logger.error(f"Snapshot file not found: {snapshot_file}")
                return {"success": False, "message": "Snapshot file not found"}
            
            # Check file size and log it - if too small might not be a valid snapshot
            file_size = os.path.getsize(snapshot_file)
            logger.info(f"Snapshot file size: {file_size} bytes")
            if file_size < 1000:  # Arbitrary small size check
                logger.warning(f"Snapshot file seems very small ({file_size} bytes), might not be a valid snapshot")
            
            # Qdrant default port is 6333
            qdrant_port = 6333
            
            # Get the Qdrant URL from the client
            qdrant_host = self.qdrant_client._client._host
            api_key = self.qdrant_client._client._api_key
            
            # Handle case where host already includes port or scheme
            if qdrant_host.startswith(('http://', 'https://')):
                qdrant_url = qdrant_host
            else:
                # Check if host contains port
                if ':' in qdrant_host:
                    host_parts = qdrant_host.split(':')
                    qdrant_host = host_parts[0]
                    if len(host_parts) > 1 and host_parts[1].isdigit():
                        qdrant_port = int(host_parts[1])
                
                qdrant_url = f"http://{qdrant_host}:{qdrant_port}"
            
            logger.info(f"Using Qdrant URL for restoration: {qdrant_url}")
            
            headers = {}
            if api_key:
                headers["api-key"] = api_key
            
            # Use the local file to upload and restore
            with open(snapshot_file, 'rb') as file:
                file_content = file.read()  # Read file content to log size
                logger.info(f"File content size for upload: {len(file_content)} bytes")
                
                # Check the first few bytes to see if it looks like a valid file format
                first_bytes = file_content[:50] if len(file_content) >= 50 else file_content
                logger.info(f"First bytes of file (hex): {first_bytes.hex()[:100]}")
                
                # Reset file pointer to beginning
                file.seek(0)
                
                # Print the final filename being sent to ensure it's correct
                filename = os.path.basename(snapshot_file)
                logger.info(f"Uploading file with name: {filename}")
                
                response = requests.post(
                    f"{qdrant_url}/collections/{collection_name}/snapshots/upload?priority=snapshot",
                    headers=headers,
                    files={"snapshot": (filename, file, 'application/octet-stream')}
                )
                
            # Log both status code and response content for debugging
            logger.info(f"Qdrant response status: {response.status_code}")
            logger.info(f"Qdrant response content: {response.text}")
            
            if response.status_code == 200:
                logger.info(f"Successfully restored snapshot to collection '{collection_name}'")
                return {
                    "success": True,
                    "message": f"Successfully restored snapshot to collection '{collection_name}'",
                    "collection_name": collection_name,
                    "snapshot_file": snapshot_file
                }
            else:
                error_message = f"Error restoring snapshot: HTTP {response.status_code} - {response.text}"
                logger.error(error_message)
                return {"success": False, "message": error_message}
                
        except Exception as e:
            logger.error(f"Error restoring snapshot: {str(e)}")
            logger.exception("Detailed exception info:")
            return {"success": False, "message": f"Error restoring snapshot: {str(e)}"}

    async def download_snapshot(self, collection_name: str, snapshot_id: str = None) -> str:
        """
        Download a snapshot for a collection.
        
        Args:
            collection_name: Name of the collection
            snapshot_id: ID of the specific snapshot to download (optional)
            
        Returns:
            Path to the downloaded snapshot file or None if not found
        """
        try:
            # First list available snapshots
            snapshot_list = self.qdrant_client.list_snapshots(collection_name=collection_name)
            if not snapshot_list:
                logger.error(f"No snapshots found for collection: {collection_name}")
                return None
            
            # Find the target snapshot
            target_snapshot = None
            if snapshot_id:
                # Look for a specific snapshot by ID
                for snapshot in snapshot_list:
                    if snapshot.name == snapshot_id:
                        target_snapshot = snapshot
                        break
                
                if not target_snapshot:
                    logger.error(f"Snapshot with ID {snapshot_id} not found for collection {collection_name}")
                    return None
            else:
                # Sort snapshots by creation time (newest first) and get the latest
                sorted_snapshots = sorted(
                    snapshot_list, 
                    key=lambda x: x.creation_time if hasattr(x, 'creation_time') else 0,
                    reverse=True
                )
                target_snapshot = sorted_snapshots[0]
            
            logger.info(f"Preparing to download snapshot: {target_snapshot.name} for collection: {collection_name}")
            
            # Create temporary directory for snapshots if it doesn't exist
            os.makedirs("temp/snapshots", exist_ok=True)
            
            # Define the local path for the snapshot - use the original snapshot name
            # The snapshot name already contains the collection name and timestamp
            snapshot_filename = f"{target_snapshot.name}"
            
            # Ensure the filename has the .snapshot extension (only once)
            if not snapshot_filename.endswith('.snapshot'):
                snapshot_filename += '.snapshot'
            
            local_path = f"temp/snapshots/{snapshot_filename}"
            logger.info(f"Will save snapshot to path: {local_path}")
            
            # Get the Qdrant URL from the client
            qdrant_url = self.qdrant_client._client._host
            
            # Ensure the URL has a proper scheme
            if not qdrant_url.startswith(('http://', 'https://')):
                qdrant_port = 6333
                # Check if host contains port
                if ':' in qdrant_url:
                    host_parts = qdrant_url.split(':')
                    qdrant_url = host_parts[0]
                    if len(host_parts) > 1 and host_parts[1].isdigit():
                        qdrant_port = int(host_parts[1])
                
                qdrant_url = f"http://{qdrant_url}:{qdrant_port}"
            
            # Build the URL to download the snapshot
            download_url = f"{qdrant_url}/collections/{collection_name}/snapshots/{target_snapshot.name}"
            
            # Set headers
            headers = {}
            api_key = self.qdrant_client._client._api_key
            if api_key:
                headers["api-key"] = api_key
            
            logger.info(f"Downloading snapshot from URL: {download_url}")
            
            # Download the snapshot from the Qdrant server
            try:
                response = requests.get(download_url, headers=headers, stream=True)
                response.raise_for_status()  # Raises an exception if the request failed
                
                content_length = int(response.headers.get('content-length', 0))
                logger.info(f"Snapshot file size from headers: {content_length} bytes")
                
                with open(local_path, 'wb') as f:
                    # Write the snapshot data to the file in chunks
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                # Get the actual file size
                file_size = os.path.getsize(local_path)
                logger.info(f"Downloaded snapshot file size: {file_size} bytes")
                
                if file_size < 1000:
                    logger.warning(f"Downloaded snapshot file is very small ({file_size} bytes), might not be valid")
                
                return local_path
            except Exception as e:
                logger.error(f"Error downloading snapshot file: {str(e)}")
                return None
            
        except Exception as e:
            logger.error(f"Error in download_snapshot: {str(e)}")
            logger.exception("Detailed exception stack:")
            return None 