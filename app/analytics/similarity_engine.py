"""
Dataset Similarity Engine.
Finds similar datasets using text-based similarity on metadata.
"""
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from bson import ObjectId
import logging
import re
from collections import Counter

logger = logging.getLogger(__name__)


class SimilarityEngine:
    """
    Find similar datasets based on multiple similarity signals:
    1. Domain/task matching
    2. Text similarity (description, tags)
    3. Modality matching
    4. Size similarity
    """
    
    def __init__(self):
        self.stop_words = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
            'of', 'with', 'by', 'from', 'is', 'are', 'was', 'were', 'be', 'been',
            'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
            'could', 'should', 'may', 'might', 'must', 'can', 'this', 'that',
            'these', 'those', 'it', 'its', 'as', 'if', 'then', 'than', 'so',
            'such', 'both', 'each', 'all', 'any', 'some', 'no', 'not', 'only',
            'same', 'just', 'very', 'also', 'about', 'into', 'through', 'during',
            'before', 'after', 'above', 'below', 'between', 'under', 'again',
            'further', 'once', 'here', 'there', 'when', 'where', 'why', 'how',
            'which', 'who', 'whom', 'what', 'dataset', 'data', 'set', 'used',
            'using', 'use', 'contains', 'include', 'includes', 'included'
        }
    
    def tokenize(self, text: str) -> List[str]:
        """Tokenize text into words, removing stop words."""
        if not text:
            return []
        
        # Lowercase and extract words
        words = re.findall(r'\b[a-z]+\b', text.lower())
        
        # Remove stop words and short words
        return [w for w in words if w not in self.stop_words and len(w) > 2]
    
    def get_dataset_features(self, dataset: Dict[str, Any]) -> Dict[str, Any]:
        """Extract features from a dataset for similarity comparison."""
        metadata = dataset.get('source', {}).get('source_metadata', {})
        intelligence = dataset.get('intelligence', {})
        
        # Get text content
        description = dataset.get('description', '') or ''
        name = dataset.get('canonical_name', '') or ''
        
        # Get tags
        tags = metadata.get('tags', []) or []
        if isinstance(tags, list):
            tags = [str(t).lower() for t in tags]
        else:
            tags = []
        
        # Get tasks from intelligence
        tasks = intelligence.get('tasks', []) or []
        
        # Tokenize description
        tokens = self.tokenize(description) + self.tokenize(name)
        
        return {
            'domain': (dataset.get('domain') or '').lower(),
            'modality': (dataset.get('modality') or '').lower(),
            'tokens': set(tokens),
            'tags': set(tags),
            'tasks': set([t.lower() for t in tasks]),
            'size_samples': dataset.get('size', {}).get('samples'),
            'platform': dataset.get('source', {}).get('platform', '').lower(),
        }
    
    def calculate_similarity(
        self, 
        features1: Dict[str, Any], 
        features2: Dict[str, Any]
    ) -> Tuple[float, Dict[str, float]]:
        """
        Calculate similarity score between two datasets.
        
        Returns:
            Tuple of (overall_score, breakdown)
        """
        breakdown = {}
        
        # 1. Domain match (25%)
        if features1['domain'] and features2['domain']:
            domain_match = 1.0 if features1['domain'] == features2['domain'] else 0.0
        elif features1['domain'] or features2['domain']:
            domain_match = 0.3  # Partial if one has domain
        else:
            domain_match = 0.5  # Neutral if neither has domain
        breakdown['domain'] = domain_match
        
        # 2. Modality match (20%)
        if features1['modality'] and features2['modality']:
            modality_match = 1.0 if features1['modality'] == features2['modality'] else 0.0
        elif features1['modality'] or features2['modality']:
            modality_match = 0.3
        else:
            modality_match = 0.5
        breakdown['modality'] = modality_match
        
        # 3. Token overlap (Jaccard) (25%)
        tokens1, tokens2 = features1['tokens'], features2['tokens']
        if tokens1 and tokens2:
            intersection = len(tokens1 & tokens2)
            union = len(tokens1 | tokens2)
            token_sim = intersection / union if union > 0 else 0
        else:
            token_sim = 0.0
        breakdown['content'] = token_sim
        
        # 4. Tag overlap (15%)
        tags1, tags2 = features1['tags'], features2['tags']
        if tags1 and tags2:
            intersection = len(tags1 & tags2)
            union = len(tags1 | tags2)
            tag_sim = intersection / union if union > 0 else 0
        else:
            tag_sim = 0.0
        breakdown['tags'] = tag_sim
        
        # 5. Task overlap (15%)
        tasks1, tasks2 = features1['tasks'], features2['tasks']
        if tasks1 and tasks2:
            intersection = len(tasks1 & tasks2)
            union = len(tasks1 | tasks2)
            task_sim = intersection / union if union > 0 else 0
        else:
            task_sim = 0.0
        breakdown['tasks'] = task_sim
        
        # Weighted overall score
        overall = (
            domain_match * 0.25 +
            modality_match * 0.20 +
            token_sim * 0.25 +
            tag_sim * 0.15 +
            task_sim * 0.15
        )
        
        return overall, breakdown
    
    async def find_similar(
        self, 
        dataset_id: str,
        db,
        limit: int = 10,
        min_similarity: float = 0.1
    ) -> List[Dict[str, Any]]:
        """
        Find datasets similar to the given dataset.
        
        Args:
            dataset_id: ID of the source dataset
            db: MongoDB database instance
            limit: Maximum number of similar datasets to return
            min_similarity: Minimum similarity threshold
            
        Returns:
            List of similar datasets with similarity scores
        """
        # Get source dataset
        source = await db.datasets.find_one({'_id': ObjectId(dataset_id)})
        if not source:
            return []
        
        source_features = self.get_dataset_features(source)
        
        # Build query to filter candidates
        # First, try to find datasets with same domain or modality
        query = {'_id': {'$ne': ObjectId(dataset_id)}}
        
        # If source has domain/modality, prefer matching ones
        or_conditions = []
        if source_features['domain']:
            or_conditions.append({'domain': source.get('domain')})
        if source_features['modality']:
            or_conditions.append({'modality': source.get('modality')})
        
        if or_conditions:
            query['$or'] = or_conditions
        
        # Fetch candidates (limit to reasonable number for performance)
        candidates = await db.datasets.find(query).limit(500).to_list(length=500)
        
        # If we don't have enough candidates with matching domain/modality,
        # also include some random ones
        if len(candidates) < 50:
            extra = await db.datasets.find(
                {'_id': {'$ne': ObjectId(dataset_id)}}
            ).limit(200).to_list(length=200)
            
            existing_ids = {str(c['_id']) for c in candidates}
            for c in extra:
                if str(c['_id']) not in existing_ids:
                    candidates.append(c)
                    if len(candidates) >= 500:
                        break
        
        # Calculate similarity for each candidate
        similarities = []
        for candidate in candidates:
            candidate_features = self.get_dataset_features(candidate)
            score, breakdown = self.calculate_similarity(source_features, candidate_features)
            
            if score >= min_similarity:
                similarities.append({
                    'dataset': candidate,
                    'similarity_score': round(score, 3),
                    'breakdown': {k: round(v, 2) for k, v in breakdown.items()}
                })
        
        # Sort by similarity and return top results
        similarities.sort(key=lambda x: x['similarity_score'], reverse=True)
        
        return similarities[:limit]
    
    def format_similar_dataset(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Format a similar dataset for API response."""
        dataset = item['dataset']
        metadata = dataset.get('source', {}).get('source_metadata', {})
        
        return {
            'id': str(dataset['_id']),
            'name': dataset.get('canonical_name'),
            'description': (dataset.get('description', '') or '')[:200],
            'domain': dataset.get('domain'),
            'modality': dataset.get('modality'),
            'platform': dataset.get('source', {}).get('platform'),
            'downloads': metadata.get('downloads'),
            'likes': metadata.get('likes'),
            'license': dataset.get('license') or metadata.get('license'),
            'similarity_score': item['similarity_score'],
            'similarity_breakdown': item['breakdown'],
            'match_reasons': self._get_match_reasons(item['breakdown'])
        }
    
    def _get_match_reasons(self, breakdown: Dict[str, float]) -> List[str]:
        """Generate human-readable reasons for the match."""
        reasons = []
        
        if breakdown.get('domain', 0) >= 0.8:
            reasons.append('Same domain')
        if breakdown.get('modality', 0) >= 0.8:
            reasons.append('Same modality')
        if breakdown.get('content', 0) >= 0.3:
            reasons.append('Similar content')
        if breakdown.get('tags', 0) >= 0.3:
            reasons.append('Overlapping tags')
        if breakdown.get('tasks', 0) >= 0.3:
            reasons.append('Similar tasks')
        
        return reasons if reasons else ['Related dataset']


# Singleton instance
similarity_engine = SimilarityEngine()
