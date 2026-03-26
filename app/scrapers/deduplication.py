"""
Deduplication logic using fuzzy matching and embeddings.
Identifies duplicate datasets across different platforms.
"""
from typing import List, Dict, Any, Optional, Tuple
import logging
from rapidfuzz import fuzz
from sentence_transformers import SentenceTransformer
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from bson import ObjectId

logger = logging.getLogger(__name__)


class DatasetDeduplicator:
    """
    Deduplicates datasets using fuzzy string matching and semantic embeddings.
    """
    
    def __init__(
        self,
        fuzzy_threshold: float = 85.0,
        embedding_threshold: float = 0.85,
        model_name: str = "BAAI/bge-base-en-v1.5"

    ):
        """
        Initialize deduplicator.
        
        Args:
            fuzzy_threshold: Minimum similarity score for fuzzy matching (0-100)
            embedding_threshold: Minimum cosine similarity for embeddings (0-1)
            model_name: Sentence transformer model name
        """
        self.fuzzy_threshold = fuzzy_threshold
        self.embedding_threshold = embedding_threshold
        
        # Load sentence transformer model
        logger.info(f"Loading embedding model: {model_name}")
        self.model = SentenceTransformer(model_name)
    
    def fuzzy_match(self, name1: str, name2: str) -> float:
        """
        Calculate fuzzy similarity between two dataset names.
        
        Args:
            name1: First dataset name
            name2: Second dataset name
            
        Returns:
            Similarity score (0-100)
        """
        # Use token sort ratio for better matching with reordered words
        return fuzz.token_sort_ratio(name1.lower(), name2.lower())
    
    def generate_embedding(self, text: str) -> np.ndarray:
        """
        Generate embedding vector for text.
        
        Args:
            text: Input text (name + description)
            
        Returns:
            Embedding vector
        """
        return self.model.encode(text, convert_to_numpy=True)
    
    def embedding_similarity(
        self,
        embedding1: np.ndarray,
        embedding2: np.ndarray
    ) -> float:
        """
        Calculate cosine similarity between embeddings.
        
        Args:
            embedding1: First embedding vector
            embedding2: Second embedding vector
            
        Returns:
            Cosine similarity (0-1)
        """
        # Reshape for sklearn
        emb1 = embedding1.reshape(1, -1)
        emb2 = embedding2.reshape(1, -1)
        
        return cosine_similarity(emb1, emb2)[0][0]
    
    def is_duplicate(
        self,
        dataset1: Dict[str, Any],
        dataset2: Dict[str, Any]
    ) -> Tuple[bool, float]:
        """
        Check if two datasets are duplicates.
        
        Args:
            dataset1: First dataset
            dataset2: Second dataset
            
        Returns:
            Tuple of (is_duplicate, similarity_score)
        """
        # First check fuzzy matching on names
        name1 = dataset1.get('canonical_name', '')
        name2 = dataset2.get('canonical_name', '')
        
        fuzzy_score = self.fuzzy_match(name1, name2)
        
        if fuzzy_score >= self.fuzzy_threshold:
            logger.debug(f"Fuzzy match: {name1} <-> {name2} ({fuzzy_score:.2f})")
            return True, fuzzy_score / 100.0
        
        # If fuzzy matching is inconclusive, check embeddings
        if fuzzy_score > 70:  # Only compute embeddings for potential matches
            text1 = f"{name1} {dataset1.get('description', '')}"
            text2 = f"{name2} {dataset2.get('description', '')}"
            
            emb1 = self.generate_embedding(text1)
            emb2 = self.generate_embedding(text2)
            
            emb_score = self.embedding_similarity(emb1, emb2)
            
            if emb_score >= self.embedding_threshold:
                logger.debug(f"Embedding match: {name1} <-> {name2} ({emb_score:.3f})")
                return True, emb_score
        
        return False, 0.0
    
    def find_canonical_dataset(
        self,
        new_dataset: Dict[str, Any],
        existing_datasets: List[Dict[str, Any]]
    ) -> Optional[str]:
        """
        Find canonical dataset ID for a new dataset.
        
        Args:
            new_dataset: New dataset to check
            existing_datasets: List of existing datasets
            
        Returns:
            Canonical dataset ID if duplicate found, None otherwise
        """
        best_match = None
        best_score = 0.0
        
        for existing in existing_datasets:
            is_dup, score = self.is_duplicate(new_dataset, existing)
            
            if is_dup and score > best_score:
                best_score = score
                best_match = existing.get('_id')
        
        if best_match:
            logger.info(
                f"Found duplicate: {new_dataset['canonical_name']} -> "
                f"{best_match} (score: {best_score:.3f})"
            )
        
        return str(best_match) if best_match else None
    
    def deduplicate_batch(
        self,
        datasets: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Deduplicate a batch of datasets.
        
        Args:
            datasets: List of datasets to deduplicate
            
        Returns:
            List of unique datasets with canonical IDs assigned
        """
        unique_datasets = []
        canonical_map = {}  # Maps dataset to canonical ID
        
        for dataset in datasets:
            name = dataset['canonical_name']
            
            # Check against already processed datasets
            canonical_id = self.find_canonical_dataset(dataset, unique_datasets)
            
            if canonical_id:
                # This is a duplicate
                canonical_map[name] = canonical_id
            else:
                # This is unique, add to list
                if '_id' not in dataset:
                    dataset['_id'] = ObjectId()
                unique_datasets.append(dataset)
                canonical_map[name] = str(dataset['_id'])
        
        logger.info(
            f"Deduplication complete: {len(datasets)} -> {len(unique_datasets)} "
            f"({len(datasets) - len(unique_datasets)} duplicates removed)"
        )
        
        return unique_datasets
    
    def generate_embeddings_batch(
        self,
        datasets: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Generate embeddings for a batch of datasets.
        
        Args:
            datasets: List of datasets
            
        Returns:
            Datasets with embedding_vector field added
        """
        texts = []
        for dataset in datasets:
            text = f"{dataset.get('canonical_name', '')} {dataset.get('description', '')}"
            texts.append(text)
        
        # Batch encode for efficiency
        logger.info(f"Generating embeddings for {len(texts)} datasets...")
        embeddings = self.model.encode(texts, convert_to_numpy=True, show_progress_bar=True)
        
        # Convert to float16 to save space (50% reduction)
        embeddings = embeddings.astype(np.float16)
        
        # Add embeddings to datasets
        for dataset, embedding in zip(datasets, embeddings):
            dataset['embedding_vector'] = embedding.tolist()
        
        return datasets


# Global deduplicator instance
deduplicator = DatasetDeduplicator()
