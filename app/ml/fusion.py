"""
Reciprocal Rank Fusion (RRF) for hybrid search.
Combines rankings from multiple search sources into a single unified result set.
"""
from typing import List, Dict, Any, Union

def reciprocal_rank_fusion(
    keyword_results: List[Dict[str, Any]], 
    semantic_results: List[Dict[str, Any]], 
    k: int = 60
) -> List[str]:
    """
    Standard RRF formula: score = sum(1 / (k + rank))
    
    Args:
        keyword_results: List of dataset dicts from MongoDB (must have 'id' or '_id').
        semantic_results: List of objects with 'id' and 'score' from semantic search.
        k: Smoothing constant (default 60 as per standard benchmark).
        
    Returns:
        List of dataset IDs sorted by fused score in descending order.
    """
    rrf_scores = {}

    # Rank Keyword Results
    for rank, item in enumerate(keyword_results, start=1):
        # Extract ID (could be 'id' from frontend-ready dict or '_id' from Mongo raw)
        ds_id = str(item.get('_id') or item.get('id'))
        rrf_scores[ds_id] = rrf_scores.get(ds_id, 0) + (1.0 / (k + rank))

    # Rank Semantic Results
    for rank, item in enumerate(semantic_results, start=1):
        ds_id = str(item.get('id'))
        rrf_scores[ds_id] = rrf_scores.get(ds_id, 0) + (1.0 / (k + rank))

    # Sort by fused score descending
    sorted_ids = sorted(rrf_scores.keys(), key=lambda x: rrf_scores[x], reverse=True)
    
    return sorted_ids
