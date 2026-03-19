"""
Semantic search using FastEmbed vectors and a local Zvec-style
memory-mapped HNSW index.

This replaces FAISS + sentence-transformers for semantic retrieval paths.
"""
from typing import List, Dict, Any, Optional, Tuple
import asyncio
import numpy as np
from fastembed import TextEmbedding
import hnswlib
from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId
import logging
import pickle
import os
import time

logger = logging.getLogger(__name__)


class SemanticSearch:
    """Semantic search engine using FastEmbed + local Zvec-style HNSW."""
    
    def __init__(
        self,
        db: AsyncIOMotorDatabase,
        model_name: str = "BAAI/bge-small-en-v1.5",
        index_path: str = "zvec_datasets"
    ):
        self.db = db
        self.model_name = os.getenv("FASTEMBED_MODEL", model_name)
        self.index_path = os.getenv("ZVEC_COLLECTION_PATH", index_path)
        self.max_elements = int(os.getenv("ZVEC_MAX_ELEMENTS", "250000"))
        self.ef_construction = int(os.getenv("ZVEC_EF_CONSTRUCTION", "200"))
        self.M = int(os.getenv("ZVEC_M", "16"))
        self.ef_search = int(os.getenv("ZVEC_EF_SEARCH", "64"))

        logger.info(f"Loading FastEmbed model: {self.model_name}")
        self.model = TextEmbedding(model_name=self.model_name)
        probe = self._encode_texts(["semantic-probe"])
        self.embedding_dim = probe.shape[1]

        # Local Zvec-like HNSW collection
        self.index = None
        self.dataset_ids: List[str] = []
        self.label_to_dataset_id: Dict[int, str] = {}
        self.dataset_id_to_label: Dict[str, int] = {}
        self.payloads: Dict[str, Dict[str, Any]] = {}
        self._build_lock = asyncio.Lock()
        self._is_building = False
        self._is_ready = False
        self._last_not_ready_log_ts = 0.0
        self._not_ready_log_interval_sec = 30.0

        index_dir = os.path.dirname(self.index_path)
        if index_dir:
            os.makedirs(index_dir, exist_ok=True)

    def _encode_texts(self, texts: List[str]) -> np.ndarray:
        """Encode text batch to dense vectors using FastEmbed."""
        normalized = [((text or "").strip() or " ") for text in texts]
        vectors = list(self.model.embed(normalized))
        if not vectors:
            return np.empty((0, self.embedding_dim), dtype=np.float32)
        return np.asarray(vectors, dtype=np.float32)

    @staticmethod
    def _normalize(vectors: np.ndarray) -> np.ndarray:
        """L2-normalize vectors for cosine similarity over HNSW."""
        if vectors.size == 0:
            return vectors
        norms = np.linalg.norm(vectors, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        return vectors / norms

    @staticmethod
    def _build_text(dataset: Dict[str, Any]) -> str:
        """Build semantic text representation for a dataset."""
        text = f"{dataset.get('display_name', '')} {dataset.get('canonical_name', '')} {dataset.get('description', '')}"
        if dataset.get('llm_summary'):
            text += f" {dataset['llm_summary']}"
        return text.strip() or "dataset"
    
    async def build_index(self, limit: Optional[int] = None):
        """
        Build local Zvec-style HNSW index from all datasets.
        
        Args:
            limit: Optional limit on number of datasets
        """
        if self._is_building:
            return

        async with self._build_lock:
            if self._is_building:
                return
            self._is_building = True
            try:
                logger.info("Building Zvec-style HNSW index...")
            
                # Fetch datasets
                cursor = self.db.datasets.find({})
                if limit:
                    cursor = cursor.limit(limit)
                
                datasets = await cursor.to_list(length=limit if limit else None)
                
                if not datasets:
                    logger.warning("No datasets found for indexing")
                    self._is_ready = False
                    return
            
                # Prepare texts, ids, and payloads
                texts = []
                dataset_ids = []
                payloads = {}
                
                for dataset in datasets:
                    text = self._build_text(dataset)
                    texts.append(text)
                    dataset_id = str(dataset['_id'])
                    dataset_ids.append(dataset_id)
                    payloads[dataset_id] = {
                        '_id': dataset_id,
                        'canonical_name': dataset.get('canonical_name'),
                        'display_name': dataset.get('display_name'),
                        'description': dataset.get('description', ''),
                        'domain': dataset.get('domain'),
                        'modality': dataset.get('modality'),
                        'trend_score': dataset.get('trend_score'),
                        'quality_label': dataset.get('quality_label'),
                        'source': dataset.get('source', {}),
                        'size': dataset.get('size', {}),
                    }
            
                # Generate embeddings
                logger.info(f"Generating embeddings for {len(texts)} datasets...")
                embeddings = self._normalize(self._encode_texts(texts))

                # Create local HNSW index
                logger.info("Creating local HNSW collection...")
                self.index = hnswlib.Index(space='cosine', dim=self.embedding_dim)
                max_elements = max(self.max_elements, len(dataset_ids) + 1000)
                self.index.init_index(
                    max_elements=max_elements,
                    ef_construction=self.ef_construction,
                    M=self.M
                )
                self.index.set_ef(self.ef_search)

                labels = np.arange(len(dataset_ids), dtype=np.int64)
                self.index.add_items(embeddings, labels)
                self.dataset_ids = dataset_ids
                self.label_to_dataset_id = {int(i): dataset_ids[i] for i in range(len(dataset_ids))}
                self.dataset_id_to_label = {dataset_ids[i]: int(i) for i in range(len(dataset_ids))}
                self.payloads = payloads

                logger.info(f"HNSW index built with {len(dataset_ids)} vectors")
                
                # Save index
                self.save_index()
                self._is_ready = True
            except Exception as e:
                self._is_ready = False
                logger.error(f"Error building semantic index: {e}")
            finally:
                self._is_building = False
    
    def save_index(self):
        """Persist local Zvec-style HNSW index and metadata to disk."""
        if self.index is None:
            logger.warning("No index to save")
            return
        
        try:
            self.index.save_index(self.index_path)

            with open(f"{self.index_path}.ids", 'wb') as f:
                pickle.dump(self.dataset_ids, f)
            with open(f"{self.index_path}.mappings", 'wb') as f:
                pickle.dump(
                    {
                        'label_to_dataset_id': self.label_to_dataset_id,
                        'dataset_id_to_label': self.dataset_id_to_label,
                        'payloads': self.payloads,
                    },
                    f,
                )
            
            logger.info(f"Index saved to {self.index_path}")
            
        except Exception as e:
            logger.error(f"Error saving index: {e}")
    
    def load_index(self) -> bool:
        """
        Load local Zvec-style HNSW index from disk.
        
        Returns:
            True if successful
        """
        if not os.path.exists(self.index_path):
            logger.warning(f"Index file not found: {self.index_path}")
            return False
        
        try:
            self.index = hnswlib.Index(space='cosine', dim=self.embedding_dim)
            self.index.load_index(self.index_path, max_elements=self.max_elements)
            self.index.set_ef(self.ef_search)

            with open(f"{self.index_path}.ids", 'rb') as f:
                self.dataset_ids = pickle.load(f)
            mappings_path = f"{self.index_path}.mappings"
            if os.path.exists(mappings_path):
                with open(mappings_path, 'rb') as f:
                    mappings = pickle.load(f)
                    self.label_to_dataset_id = mappings.get('label_to_dataset_id', {})
                    self.dataset_id_to_label = mappings.get('dataset_id_to_label', {})
                    self.payloads = mappings.get('payloads', {})
            else:
                self.label_to_dataset_id = {
                    int(i): dataset_id for i, dataset_id in enumerate(self.dataset_ids)
                }
                self.dataset_id_to_label = {
                    dataset_id: int(i) for i, dataset_id in enumerate(self.dataset_ids)
                }
                self.payloads = {}
            
            logger.info(f"Index loaded from {self.index_path} ({len(self.dataset_ids)} vectors)")
            self._is_ready = True
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
            now = time.time()
            if now - self._last_not_ready_log_ts >= self._not_ready_log_interval_sec:
                if self._is_building:
                    logger.info("Semantic index build in progress; semantic search will use fallback until ready")
                else:
                    logger.warning("Semantic index not ready; semantic search will use fallback")
                self._last_not_ready_log_ts = now
            return []
        
        query_embedding = self._normalize(self._encode_texts([query]))
        labels, distances = self.index.knn_query(query_embedding, k=k)
        
        # Format results
        results = []
        for idx, dist in zip(labels[0], distances[0]):
            idx = int(idx)
            dataset_id = self.label_to_dataset_id.get(idx)
            if not dataset_id:
                continue
            similarity = 1.0 - float(dist)
            if similarity >= threshold:
                results.append((dataset_id, similarity))
        
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

        # Fast path: serve from in-memory payloads (no per-query Mongo round trip)
        enriched_results = []
        missing_ids: List[str] = []
        for dataset_id, score in results:
            payload = self.payloads.get(dataset_id)
            if payload:
                dataset = dict(payload)
                dataset['similarity_score'] = score
                enriched_results.append(dataset)
            else:
                missing_ids.append(dataset_id)

        # Fallback for any payload misses
        if missing_ids:
            object_ids = [ObjectId(dataset_id) for dataset_id in missing_ids]
            datasets = await self.db.datasets.find({'_id': {'$in': object_ids}}).to_list(length=len(missing_ids))
            dataset_map = {str(d['_id']): d for d in datasets}
            for dataset_id, score in results:
                if dataset_id in missing_ids and dataset_id in dataset_map:
                    dataset = dict(dataset_map[dataset_id])
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
        
        dataset_id_str = str(dataset_id)
        if dataset_id_str in self.dataset_id_to_label:
            await self.build_index()
            return

        if len(self.dataset_ids) + 1 > self.max_elements:
            await self.build_index()
            return

        text = self._build_text(dataset)
        embedding = self._normalize(self._encode_texts([text]))

        new_label = len(self.dataset_ids)
        self.index.add_items(embedding, np.array([new_label], dtype=np.int64))
        self.dataset_ids.append(dataset_id_str)
        self.label_to_dataset_id[new_label] = dataset_id_str
        self.dataset_id_to_label[dataset_id_str] = new_label
        self.payloads[dataset_id_str] = {
            '_id': dataset_id_str,
            'canonical_name': dataset.get('canonical_name'),
            'display_name': dataset.get('display_name'),
            'description': dataset.get('description', ''),
            'domain': dataset.get('domain'),
            'modality': dataset.get('modality'),
            'trend_score': dataset.get('trend_score'),
            'quality_label': dataset.get('quality_label'),
            'source': dataset.get('source', {}),
            'size': dataset.get('size', {}),
        }

        logger.info(f"Added dataset {dataset_id} to index")
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
            # Build new index in background to avoid first-request latency spikes
            logger.warning("Semantic index missing; starting background build")
            asyncio.create_task(semantic_search.build_index())
    
    return semantic_search
