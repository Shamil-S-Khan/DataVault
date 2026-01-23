"""
Semantic search using FAISS and sentence transformers.
Enables similarity-based dataset discovery.
"""
from typing import List, Dict, Any, Optional, Tuple
import numpy as np
import faiss
from sentence_transformers import SentenceTransformer
from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId
import logging
import pickle
import os

logger = logging.getLogger(__name__)


class SemanticSearch:
    """Semantic search engine using FAISS."""
    
    def __init__(
        self,
        db: AsyncIOMotorDatabase,
        model_name: str = "all-MiniLM-L6-v2",
        index_path: str = "faiss_index.bin"
    ):
        self.db = db
        self.model_name = model_name
        self.index_path = index_path
        
        # Load sentence transformer
        logger.info(f"Loading sentence transformer: {model_name}")
        self.model = SentenceTransformer(model_name)
        self.embedding_dim = self.model.get_sentence_embedding_dimension()
        
        # FAISS index
        self.index = None
        self.dataset_ids = []
    
    async def build_index(self, limit: Optional[int] = None):
        """
        Build FAISS index from all datasets.
        
        Args:
            limit: Optional limit on number of datasets
        """
        logger.info("Building FAISS index...")
        
        # Fetch datasets
        cursor = self.db.datasets.find({})
        if limit:
            cursor = cursor.limit(limit)
        
        datasets = await cursor.to_list(length=limit or 10000)
        
        if not datasets:
            logger.warning("No datasets found for indexing")
            return
        
        # Prepare texts and IDs
        texts = []
        dataset_ids = []
        
        for dataset in datasets:
            # Combine name and description
            text = f"{dataset.get('canonical_name', '')} {dataset.get('description', '')}"
            
            if dataset.get('llm_summary'):
                text += f" {dataset['llm_summary']}"
            
            texts.append(text)
            dataset_ids.append(str(dataset['_id']))
        
        # Generate embeddings
        logger.info(f"Generating embeddings for {len(texts)} datasets...")
        embeddings = self.model.encode(
            texts,
            convert_to_numpy=True,
            show_progress_bar=True,
            batch_size=32
        )
        
        # Convert to float32 for FAISS
        embeddings = embeddings.astype('float32')
        
        # Create FAISS index
        logger.info("Creating FAISS index...")
        self.index = faiss.IndexFlatIP(self.embedding_dim)  # Inner product (cosine similarity)
        
        # Normalize embeddings for cosine similarity
        faiss.normalize_L2(embeddings)
        
        # Add to index
        self.index.add(embeddings)
        self.dataset_ids = dataset_ids
        
        logger.info(f"FAISS index built with {self.index.ntotal} vectors")
        
        # Save index
        self.save_index()
    
    def save_index(self):
        """Save FAISS index to disk."""
        if self.index is None:
            logger.warning("No index to save")
            return
        
        try:
            # Save FAISS index
            faiss.write_index(self.index, self.index_path)
            
            # Save dataset IDs
            with open(f"{self.index_path}.ids", 'wb') as f:
                pickle.dump(self.dataset_ids, f)
            
            logger.info(f"Index saved to {self.index_path}")
            
        except Exception as e:
            logger.error(f"Error saving index: {e}")
    
    def load_index(self) -> bool:
        """
        Load FAISS index from disk.
        
        Returns:
            True if successful
        """
        if not os.path.exists(self.index_path):
            logger.warning(f"Index file not found: {self.index_path}")
            return False
        
        try:
            # Load FAISS index
            self.index = faiss.read_index(self.index_path)
            
            # Load dataset IDs
            with open(f"{self.index_path}.ids", 'rb') as f:
                self.dataset_ids = pickle.load(f)
            
            logger.info(f"Index loaded from {self.index_path} ({self.index.ntotal} vectors)")
            return True
            
        except Exception as e:
            logger.error(f"Error loading index: {e}")
            return False
    
    def search(
        self,
        query: str,
        k: int = 10,
        threshold: float = 0.0
    ) -> List[Tuple[str, float]]:
        """
        Search for similar datasets.
        
        Args:
            query: Search query
            k: Number of results
            threshold: Minimum similarity threshold
            
        Returns:
            List of (dataset_id, similarity_score) tuples
        """
        if self.index is None:
            logger.error("Index not built or loaded")
            return []
        
        # Generate query embedding
        query_embedding = self.model.encode([query], convert_to_numpy=True)
        query_embedding = query_embedding.astype('float32')
        
        # Normalize for cosine similarity
        faiss.normalize_L2(query_embedding)
        
        # Search
        distances, indices = self.index.search(query_embedding, k)
        
        # Format results
        results = []
        for dist, idx in zip(distances[0], indices[0]):
            if idx < len(self.dataset_ids) and dist >= threshold:
                results.append((self.dataset_ids[idx], float(dist)))
        
        return results
    
    async def search_datasets(
        self,
        query: str,
        k: int = 10,
        threshold: float = 0.3
    ) -> List[Dict[str, Any]]:
        """
        Search datasets and return full documents.
        
        Args:
            query: Search query
            k: Number of results
            threshold: Minimum similarity threshold
            
        Returns:
            List of dataset documents with similarity scores
        """
        # Search index
        results = self.search(query, k, threshold)
        
        if not results:
            return []
        
        # Fetch datasets
        dataset_ids = [ObjectId(dataset_id) for dataset_id, _ in results]
        datasets = await self.db.datasets.find({
            '_id': {'$in': dataset_ids}
        }).to_list(length=k)
        
        # Create lookup map
        dataset_map = {str(d['_id']): d for d in datasets}
        
        # Combine with scores
        enriched_results = []
        for dataset_id, score in results:
            if dataset_id in dataset_map:
                dataset = dataset_map[dataset_id]
                dataset['similarity_score'] = score
                enriched_results.append(dataset)
        
        return enriched_results
    
    async def find_similar_datasets(
        self,
        dataset_id: ObjectId,
        k: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Find datasets similar to a given dataset.
        
        Args:
            dataset_id: Reference dataset ID
            k: Number of similar datasets to return
            
        Returns:
            List of similar datasets
        """
        # Get reference dataset
        dataset = await self.db.datasets.find_one({'_id': dataset_id})
        
        if not dataset:
            return []
        
        # Create query from dataset
        query = f"{dataset.get('canonical_name', '')} {dataset.get('description', '')}"
        
        # Search (k+1 to exclude the dataset itself)
        results = await self.search_datasets(query, k=k+1, threshold=0.0)
        
        # Filter out the reference dataset
        similar = [r for r in results if str(r['_id']) != str(dataset_id)]
        
        return similar[:k]
    
    async def update_index_incremental(self, dataset_id: ObjectId):
        """
        Add a single dataset to the index.
        
        Args:
            dataset_id: Dataset ID to add
        """
        if self.index is None:
            logger.warning("Index not initialized, cannot add incrementally")
            return
        
        # Get dataset
        dataset = await self.db.datasets.find_one({'_id': dataset_id})
        
        if not dataset:
            return
        
        # Generate embedding
        text = f"{dataset.get('canonical_name', '')} {dataset.get('description', '')}"
        embedding = self.model.encode([text], convert_to_numpy=True)
        embedding = embedding.astype('float32')
        
        # Normalize
        faiss.normalize_L2(embedding)
        
        # Add to index
        self.index.add(embedding)
        self.dataset_ids.append(str(dataset_id))
        
        logger.info(f"Added dataset {dataset_id} to index")
        
        # Save updated index
        self.save_index()


# Global semantic search instance
semantic_search = None


async def get_semantic_search(db: AsyncIOMotorDatabase) -> SemanticSearch:
    """Get or create semantic search instance."""
    global semantic_search
    
    if semantic_search is None:
        semantic_search = SemanticSearch(db)
        
        # Try to load existing index
        if not semantic_search.load_index():
            # Build new index
            await semantic_search.build_index()
    
    return semantic_search
