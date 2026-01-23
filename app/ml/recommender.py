"""
ML-based recommendation system for datasets using semantic similarity.
Uses Sentence Transformers to generate embeddings and find similar datasets.
"""
import asyncio
import numpy as np
from typing import List, Dict, Any, Optional
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import logging
from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId

logger = logging.getLogger(__name__)


class DatasetRecommender:
    """
    Content-based recommendation system using semantic embeddings.
    Generates recommendations based on dataset description similarity.
    """
    
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.model = None
        self._embeddings_cache = {}
        
    def _load_model(self):
        """Lazy load the sentence transformer model."""
        if self.model is None:
            logger.info("Loading sentence transformer model...")
            # Use lightweight model for production
            self.model = SentenceTransformer('all-MiniLM-L6-v2')
            logger.info("Model loaded successfully")
    
    async def generate_embeddings_batch(self, batch_size: int = 100):
        """
        Generate embeddings for all datasets that don't have them yet.
        Run this periodically or after new datasets are added.
        """
        self._load_model()
        
        logger.info("Starting batch embedding generation...")
        
        # Find datasets without embeddings
        query = {
            '$or': [
                {'embedding_vector': {'$exists': False}},
                {'embedding_vector': None}
            ]
        }
        
        cursor = self.db.datasets.find(query)
        datasets_to_process = await cursor.to_list(length=None)
        
        logger.info(f"Found {len(datasets_to_process)} datasets needing embeddings")
        
        if not datasets_to_process:
            logger.info("All datasets already have embeddings")
            return
        
        # Process in batches
        for i in range(0, len(datasets_to_process), batch_size):
            batch = datasets_to_process[i:i + batch_size]
            
            # Prepare texts for embedding
            texts = []
            for dataset in batch:
                # Combine multiple fields for richer embeddings
                text = f"{dataset.get('display_name', '')} {dataset.get('canonical_name', '')} {dataset.get('description', '')}"
                texts.append(text)
            
            # Generate embeddings
            embeddings = self.model.encode(texts, show_progress_bar=True)
            
            # Update database
            for dataset, embedding in zip(batch, embeddings):
                await self.db.datasets.update_one(
                    {'_id': dataset['_id']},
                    {'$set': {'embedding_vector': embedding.tolist()}}
                )
            
            logger.info(f"Processed batch {i//batch_size + 1}/{(len(datasets_to_process) + batch_size - 1)//batch_size}")
        
        logger.info("Batch embedding generation complete")
    
    async def get_similar_datasets(
        self, 
        dataset_id: str, 
        limit: int = 10,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Find similar datasets using cosine similarity of embeddings.
        
        Args:
            dataset_id: ID of the target dataset
            limit: Number of recommendations to return
            filters: Optional filters (e.g., same platform, modality)
        
        Returns:
            List of similar datasets with similarity scores
        """
        # Get target dataset
        target = await self.db.datasets.find_one({'_id': ObjectId(dataset_id)})
        if not target:
            raise ValueError(f"Dataset {dataset_id} not found")
        
        # Generate embedding if missing
        if not target.get('embedding_vector'):
            self._load_model()
            text = f"{target.get('display_name', '')} {target.get('canonical_name', '')} {target.get('description', '')}"
            embedding = self.model.encode([text])[0]
            await self.db.datasets.update_one(
                {'_id': target['_id']},
                {'$set': {'embedding_vector': embedding.tolist()}}
            )
            target['embedding_vector'] = embedding.tolist()
        
        target_embedding = np.array(target['embedding_vector']).reshape(1, -1)
        
        # Build query for candidate datasets
        query = {
            '_id': {'$ne': target['_id']},  # Exclude self
            'embedding_vector': {'$exists': True, '$ne': None}
        }
        
        # Apply filters
        if filters:
            if filters.get('same_modality'):
                query['modality'] = target.get('modality')
            if filters.get('same_platform'):
                query['source.platform'] = target.get('source', {}).get('platform')
            if filters.get('domain'):
                query['domain'] = filters['domain']
        
        # Get candidate datasets
        candidates = await self.db.datasets.find(query).to_list(length=None)
        
        if not candidates:
            logger.warning(f"No candidate datasets found for {dataset_id}")
            return []
        
        # Calculate similarities
        candidate_embeddings = np.array([c['embedding_vector'] for c in candidates])
        similarities = cosine_similarity(target_embedding, candidate_embeddings)[0]
        
        # Sort by similarity
        ranked_indices = np.argsort(similarities)[::-1][:limit]
        
        # Format results
        recommendations = []
        for idx in ranked_indices:
            candidate = candidates[idx]
            recommendations.append({
                'id': str(candidate['_id']),
                'name': candidate.get('display_name') or candidate.get('canonical_name'),
                'canonical_name': candidate.get('canonical_name'),
                'description': (candidate.get('description', '') or '')[:200],
                'domain': candidate.get('domain'),
                'modality': candidate.get('modality'),
                'similarity_score': float(similarities[idx]),
                'trend_score': candidate.get('trend_score', 0),
                'source': candidate.get('source', {}),
                'size': candidate.get('size', {})
            })
        
        return recommendations
    
    def _generate_match_reasons(self, dataset: Dict[str, Any], query: str, similarity_score: float) -> List[str]:
        """
        Generate explainable match reasons for a dataset.
        Explains why this dataset was recommended.
        """
        reasons = []
        query_lower = query.lower()
        description = (dataset.get('description', '') or '').lower()
        name = (dataset.get('canonical_name', '') or '').lower()
        
        # Semantic similarity reason
        if similarity_score >= 0.7:
            reasons.append(f"High semantic match ({similarity_score*100:.0f}% similar)")
        elif similarity_score >= 0.5:
            reasons.append(f"Moderate semantic match ({similarity_score*100:.0f}% similar)")
        else:
            reasons.append(f"Related content ({similarity_score*100:.0f}% similar)")
        
        # Domain/modality match
        if dataset.get('domain'):
            domain = dataset['domain'].lower()
            if domain in query_lower or any(word in domain for word in query_lower.split()):
                reasons.append(f"Matches {dataset['domain']} domain")
        
        if dataset.get('modality'):
            modality = dataset['modality'].lower()
            if modality in query_lower or any(word in modality for word in query_lower.split()):
                reasons.append(f"Contains {dataset['modality']} data")
        
        # Keyword overlap
        query_words = set(word for word in query_lower.split() if len(word) > 3)
        desc_words = set(word for word in description.split() if len(word) > 3)
        overlap = query_words & desc_words
        if overlap and len(overlap) >= 2:
            reasons.append(f"Keyword matches: {', '.join(list(overlap)[:3])}")
        
        # Popularity
        downloads = dataset.get('source', {}).get('source_metadata', {}).get('downloads', 0)
        if downloads > 1000:
            reasons.append(f"Popular dataset ({downloads:,} downloads)")
        
        # Quality
        quality_label = dataset.get('quality_label')
        if quality_label in ['Excellent', 'Good']:
            reasons.append(f"{quality_label} quality rating")
        
        return reasons[:4]  # Max 4 reasons
    
    async def get_recommendations_by_query(
        self,
        query_text: str,
        limit: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        include_explanations: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Get dataset recommendations based on a text query with explainable results.
        Useful for "find datasets like: [description]"
        
        Args:
            query_text: Natural language query
            limit: Number of results
            filters: Optional filters
            include_explanations: Include match reasons
        
        Returns:
            List of matching datasets with explanations
        """
        self._load_model()
        
        # Generate query embedding
        query_embedding = self.model.encode([query_text])[0].reshape(1, -1)
        
        # Build database query
        db_query = {'embedding_vector': {'$exists': True, '$ne': None}}
        if filters:
            db_query.update(filters)
        
        # Get candidates
        candidates = await self.db.datasets.find(db_query).to_list(length=None)
        
        if not candidates:
            return []
        
        # Calculate similarities
        candidate_embeddings = np.array([c['embedding_vector'] for c in candidates])
        similarities = cosine_similarity(query_embedding, candidate_embeddings)[0]
        
        # Sort and format
        ranked_indices = np.argsort(similarities)[::-1][:limit]
        
        recommendations = []
        for idx in ranked_indices:
            candidate = candidates[idx]
            similarity_score = float(similarities[idx])
            
            result = {
                'id': str(candidate['_id']),
                'name': candidate.get('display_name') or candidate.get('canonical_name'),
                'canonical_name': candidate.get('canonical_name'),
                'description': (candidate.get('description', '') or '')[:200],
                'domain': candidate.get('domain'),
                'modality': candidate.get('modality'),
                'similarity_score': similarity_score,
                'trend_score': candidate.get('trend_score', 0),
                'quality_label': candidate.get('quality_label'),
                'source': candidate.get('source', {})
            }
            
            # Add match explanations
            if include_explanations:
                result['match_reasons'] = self._generate_match_reasons(
                    candidate, query_text, similarity_score
                )
            
            recommendations.append(result)
        
        return recommendations


# Global instance
_recommender = None

async def get_recommender(db: AsyncIOMotorDatabase) -> DatasetRecommender:
    """Get or create the global recommender instance."""
    global _recommender
    if _recommender is None:
        _recommender = DatasetRecommender(db)
    return _recommender
