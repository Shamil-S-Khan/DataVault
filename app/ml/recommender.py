"""
ML-based recommendation system for datasets using semantic similarity.
Uses FastEmbed vectors stored in a local Zvec-style HNSW collection
through app.ml.semantic_search.
"""
from typing import List, Dict, Any, Optional
import logging
from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId
from app.ml.semantic_search import get_semantic_search

logger = logging.getLogger(__name__)


class DatasetRecommender:
    """
    Content-based recommendation system using semantic embeddings.
    Generates recommendations based on dataset description similarity.
    """
    
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self._search_engine = None

    async def _get_search_engine(self):
        """Lazy-load semantic search engine instance."""
        if self._search_engine is None:
            self._search_engine = await get_semantic_search(self.db)
        return self._search_engine
    
    async def generate_embeddings_batch(self, batch_size: int = 100):
        """
        Build/rebuild semantic index from current datasets.
        The batch_size arg is accepted for API compatibility.
        """
        search_engine = await self._get_search_engine()
        await search_engine.build_index()
        logger.info("Semantic vector index rebuild complete")
    
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
        target = await self.db.datasets.find_one({'_id': ObjectId(dataset_id)})
        if not target:
            raise ValueError(f"Dataset {dataset_id} not found")
        search_engine = await self._get_search_engine()
        similar_docs = await search_engine.find_similar_datasets(target['_id'], k=max(limit * 3, limit + 5))

        recommendations = []
        for candidate in similar_docs:
            if filters:
                if filters.get('same_modality') and candidate.get('modality') != target.get('modality'):
                    continue
                if filters.get('same_platform'):
                    c_platform = (candidate.get('source') or {}).get('platform')
                    t_platform = (target.get('source') or {}).get('platform')
                    if c_platform != t_platform:
                        continue
                if filters.get('domain') and candidate.get('domain') != filters.get('domain'):
                    continue

            recommendations.append({
                'id': str(candidate.get('_id')),
                'name': candidate.get('display_name') or candidate.get('canonical_name'),
                'canonical_name': candidate.get('canonical_name'),
                'description': (candidate.get('description', '') or '')[:200],
                'domain': candidate.get('domain'),
                'modality': candidate.get('modality'),
                'similarity_score': float(candidate.get('similarity_score', 0.0)),
                'trend_score': candidate.get('trend_score', 0),
                'source': candidate.get('source', {}),
                'size': candidate.get('size', {}),
            })

            if len(recommendations) >= limit:
                break

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
        search_engine = await self._get_search_engine()
        candidates = await search_engine.search_datasets(query_text, k=max(limit * 4, limit + 10), threshold=0.0)

        # Fallback: if semantic index is still warming/missing, use text search for responsiveness
        if not candidates:
            cursor = self.db.datasets.find({'$text': {'$search': query_text}}).sort('trend_score', -1).limit(max(limit * 4, limit + 10))
            text_candidates = await cursor.to_list(length=max(limit * 4, limit + 10))
            for item in text_candidates:
                item['similarity_score'] = float(item.get('trend_score', 0.0)) * 0.6
            candidates = text_candidates

        recommendations = []
        for candidate in candidates:
            if filters:
                if filters.get('domain') and candidate.get('domain') != filters.get('domain'):
                    continue
                if filters.get('modality') and candidate.get('modality') != filters.get('modality'):
                    continue
                if filters.get('source.platform'):
                    platform = (candidate.get('source') or {}).get('platform', '').lower()
                    if platform != str(filters.get('source.platform', '')).lower():
                        continue

            similarity_score = float(candidate.get('similarity_score', 0.0))
            
            result = {
                'id': str(candidate.get('_id')),
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
            if len(recommendations) >= limit:
                break
        
        return recommendations


# Global instance
_recommender = None

async def get_recommender(db: AsyncIOMotorDatabase) -> DatasetRecommender:
    """Get or create the global recommender instance."""
    global _recommender
    if _recommender is None:
        _recommender = DatasetRecommender(db)
    return _recommender
