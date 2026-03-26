"""
Upgraded semantic search implementation using BGE-base-v1.5.
Supports query-specific instruction prefixes and incremental FAISS updates.
"""
import os
import logging
import pickle
import numpy as np
import faiss
from typing import List, Dict, Any, Optional, Tuple
from fastembed import TextEmbedding

logger = logging.getLogger(__name__)

# Constants
MODEL_NAME = "BAAI/bge-small-en-v1.5"
DIMENSION = 384
INDEX_PATH = "faiss_index.bin"
MAPPING_PATH = "faiss_mapping.pkl"
QUERY_PREFIX = "Represent this sentence for searching relevant passages: "

class SemanticSearch:
    """Semantic search engine using BGE embeddings and FAISS."""
    
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SemanticSearch, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, 'initialized') and self.initialized:
            return
            
        self.model = None
        self.index = None
        self.id_map = []  # List of MongoDB IDs mapped to FAISS indices
        self.initialized = True
        
        # Load index if exists
        self._load_index()

    def _ensure_model(self):
        """Lazy-load the embedding model."""
        if self.model is None:
            logger.info(f"Loading embedding model: {MODEL_NAME} using FastEmbed")
            self.model = TextEmbedding(model_name=MODEL_NAME)
    
    def _load_index(self):
        """Load index and mapping from disk."""
        if os.path.exists(INDEX_PATH) and os.path.exists(MAPPING_PATH):
            try:
                self.index = faiss.read_index(INDEX_PATH)
                with open(MAPPING_PATH, 'rb') as f:
                    self.id_map = pickle.load(f)
                logger.info(f"Loaded FAISS index with {len(self.id_map)} items")
            except Exception as e:
                logger.error(f"Error loading FAISS index: {e}")
                self._create_empty_index()
        else:
            self._create_empty_index()

    def _create_empty_index(self):
        """Initialize a new empty FAISS index."""
        # IndexFlatIP = Inner Product similarity (cosine similarity for normalized vectors)
        self.index = faiss.IndexFlatIP(DIMENSION)
        self.id_map = []
        logger.info("Created new empty FAISS index")

    def _save_index(self):
        """Persist index and mapping to disk."""
        try:
            faiss.write_index(self.index, INDEX_PATH)
            with open(MAPPING_PATH, 'wb') as f:
                pickle.dump(self.id_map, f)
            logger.debug(f"Saved FAISS index to {INDEX_PATH}")
        except Exception as e:
            logger.error(f"Error saving FAISS index: {e}")

    def embed_query(self, query: str) -> np.ndarray:
        """Embed a search query with the required BGE prefix."""
        self._ensure_model()
        full_query = f"{QUERY_PREFIX}{query}"
        # FastEmbed returns a generator of numpy arrays
        embeddings = list(self.model.embed([full_query]))
        return embeddings[0].astype('float32')

    def embed_document(self, text: str) -> np.ndarray:
        """Embed a document string (no prefix)."""
        self._ensure_model()
        embeddings = list(self.model.embed([text]))
        return embeddings[0].astype('float32')

    async def search(self, query: str, top_k: int = 100) -> List[Dict[str, Any]]:
        """Perform semantic search and return ranked results."""
        if self.index.ntotal == 0:
            return []

        query_vector = self._ensure_2d(self.embed_query(query))
        
        # Search FAISS
        scores, indices = self.index.search(query_vector, min(top_k, self.index.ntotal))
        
        results = []
        for score, idx in zip(scores[0], indices[0]):
            if idx != -1 and idx < len(self.id_map):
                results.append({
                    "id": str(self.id_map[idx]),
                    "score": float(score)
                })
        
        return results

    def add_to_index(self, dataset_id: str, text: str):
        """Add a document to the index incrementally."""
        # Avoid duplicates in the ID map if possible, though FAISS allows it
        if dataset_id in self.id_map:
            logger.debug(f"ID {dataset_id} already in index, skipping incremental add")
            return

        vector = self._ensure_2d(self.embed_document(text))
        self.index.add(vector)
        self.id_map.append(dataset_id)
        self._save_index()
        logger.info(f"Added dataset {dataset_id} to FAISS index. Total: {self.index.ntotal}")

    async def find_similar_datasets(self, dataset_id: Any, k: int = 5, db: Any = None) -> List[Dict[str, Any]]:
        """Find datasets similar to a given dataset ID."""
        target_id = str(dataset_id)
        if target_id not in self.id_map:
            logger.warning(f"Dataset {target_id} not in semantic index")
            return []

        idx = self.id_map.index(target_id)
        
        # Explicitly fetch the vector from FAISS by ID if possible
        # Since it's IndexFlatIP, we can reconstruct if it's not too large
        try:
            target_vector = self._ensure_2d(self.index.reconstruct(idx))
        except:
            # Fallback if reconstruct is not supported by this index type
            logger.error("FAISS index does not support reconstruction")
            return []

        # Search for similar (k+1 because the query itself will be top match)
        scores, indices = self.index.search(target_vector, k + 1)
        
        results = []
        for score, i in zip(scores[0], indices[0]):
            res_id = str(self.id_map[i])
            if res_id != target_id:
                # We need to fetch metadata from DB if we want full objects
                if db is not None:
                    from bson import ObjectId
                    ds = await db.datasets.find_one({"_id": ObjectId(res_id)})
                    if ds:
                        ds["similarity_score"] = float(score)
                        results.append(ds)
                else:
                    results.append({"id": res_id, "similarity_score": float(score)})
            
            if len(results) >= k:
                break
                
        return results

    def _ensure_2d(self, vector: np.ndarray) -> np.ndarray:
        """FAISS expects (n, d) array."""
        if len(vector.shape) == 1:
            return vector.reshape(1, -1)
        return vector

    async def rebuild_from_mongo(self, db):
        """Rebuild the entire index from MongoDB datasets."""
        logger.info("Rebuilding FAISS index from MongoDB...")
        cursor = db.datasets.find({}, {"_id": 1, "canonical_name": 1, "description": 1, "domain": 1, "modality": 1})
        
        all_ids = []
        all_texts = []
        
        async for ds in cursor:
            text = f"{ds.get('canonical_name', '')} {ds.get('description', '')} {ds.get('domain', '')} {ds.get('modality', '')}"
            all_ids.append(str(ds['_id']))
            all_texts.append(text)
            
        if not all_texts:
            logger.warning("No datasets found to index")
            return

        self._ensure_model()
        logger.info(f"Embedding {len(all_texts)} datasets for rebuild using FastEmbed...")
        
        # TextEmbedding.embed returns a generator, we convert to a single numpy array
        # FastEmbed handles batching internally
        embeddings_gen = self.model.embed(all_texts, batch_size=64)
        embeddings = np.array(list(embeddings_gen))
        
        self._create_empty_index()
        self.index.add(embeddings.astype('float32'))
        self.id_map = all_ids
        self._save_index()
        logger.info("FAISS index rebuild complete")

# Singleton provider
_engine = None

async def get_semantic_search(db=None) -> SemanticSearch:
    """Provider function for SemanticSearch singleton."""
    global _engine
    if _engine is None:
        _engine = SemanticSearch()
        # If the index is empty, try to rebuild it if db is provided
        if _engine.index.ntotal == 0 and db is not None:
            await _engine.rebuild_from_mongo(db)
    return _engine
