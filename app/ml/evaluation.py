"""
Evaluation metrics for recommendation systems.
Implements standard information retrieval metrics.
"""
import numpy as np
from typing import List, Dict, Set, Any
import logging

logger = logging.getLogger(__name__)


class RecommendationEvaluator:
    """
    Evaluate recommendation system performance using IR metrics.
    """
    
    @staticmethod
    def precision_at_k(recommended: List[str], relevant: Set[str], k: int) -> float:
        """
        Calculate Precision@K.
        
        Precision@K = (# of relevant items in top K) / K
        
        Args:
            recommended: List of recommended item IDs (in ranked order)
            relevant: Set of truly relevant item IDs
            k: Cutoff position
        
        Returns:
            Precision at K (0-1)
        """
        if k <= 0:
            return 0.0
        
        top_k = recommended[:k]
        relevant_in_top_k = sum(1 for item in top_k if item in relevant)
        
        return relevant_in_top_k / k
    
    @staticmethod
    def recall_at_k(recommended: List[str], relevant: Set[str], k: int) -> float:
        """
        Calculate Recall@K.
        
        Recall@K = (# of relevant items in top K) / (total # of relevant items)
        
        Args:
            recommended: List of recommended item IDs
            relevant: Set of truly relevant item IDs
            k: Cutoff position
        
        Returns:
            Recall at K (0-1)
        """
        if not relevant or k <= 0:
            return 0.0
        
        top_k = recommended[:k]
        relevant_in_top_k = sum(1 for item in top_k if item in relevant)
        
        return relevant_in_top_k / len(relevant)
    
    @staticmethod
    def average_precision(recommended: List[str], relevant: Set[str]) -> float:
        """
        Calculate Average Precision (AP).
        
        AP = (sum of P@k for each relevant item) / (total # of relevant items)
        
        Args:
            recommended: List of recommended item IDs
            relevant: Set of truly relevant item IDs
        
        Returns:
            Average precision (0-1)
        """
        if not relevant:
            return 0.0
        
        score = 0.0
        num_relevant_found = 0
        
        for k, item in enumerate(recommended, 1):
            if item in relevant:
                num_relevant_found += 1
                precision_at_k = num_relevant_found / k
                score += precision_at_k
        
        return score / len(relevant)
    
    @staticmethod
    def mean_average_precision(
        recommendations: List[List[str]], 
        relevant_sets: List[Set[str]]
    ) -> float:
        """
        Calculate Mean Average Precision (MAP).
        
        MAP = mean of AP across all queries
        
        Args:
            recommendations: List of recommendation lists (one per query)
            relevant_sets: List of relevant item sets (one per query)
        
        Returns:
            MAP score (0-1)
        """
        if len(recommendations) != len(relevant_sets):
            raise ValueError("Number of recommendations must match number of relevant sets")
        
        if not recommendations:
            return 0.0
        
        aps = [
            RecommendationEvaluator.average_precision(rec, rel)
            for rec, rel in zip(recommendations, relevant_sets)
        ]
        
        return np.mean(aps)
    
    @staticmethod
    def dcg_at_k(recommended: List[str], relevant: Set[str], k: int) -> float:
        """
        Calculate Discounted Cumulative Gain at K.
        
        DCG@K = sum(rel_i / log2(i + 1)) for i in 1..k
        
        Assumes binary relevance (1 if relevant, 0 if not).
        
        Args:
            recommended: List of recommended item IDs
            relevant: Set of truly relevant item IDs
            k: Cutoff position
        
        Returns:
            DCG at K
        """
        if k <= 0:
            return 0.0
        
        dcg = 0.0
        for i, item in enumerate(recommended[:k], 1):
            if item in relevant:
                dcg += 1.0 / np.log2(i + 1)
        
        return dcg
    
    @staticmethod
    def ndcg_at_k(recommended: List[str], relevant: Set[str], k: int) -> float:
        """
        Calculate Normalized Discounted Cumulative Gain at K.
        
        NDCG@K = DCG@K / IDCG@K
        where IDCG@K is the DCG of the ideal ranking
        
        Args:
            recommended: List of recommended item IDs
            relevant: Set of truly relevant item IDs
            k: Cutoff position
        
        Returns:
            NDCG at K (0-1)
        """
        dcg = RecommendationEvaluator.dcg_at_k(recommended, relevant, k)
        
        # Ideal ranking: all relevant items first
        ideal_ranking = list(relevant)[:k]
        idcg = RecommendationEvaluator.dcg_at_k(ideal_ranking, relevant, k)
        
        if idcg == 0:
            return 0.0
        
        return dcg / idcg
    
    @staticmethod
    def hit_rate_at_k(recommended: List[str], relevant: Set[str], k: int) -> float:
        """
        Calculate Hit Rate at K.
        
        HR@K = 1 if any relevant item in top K, else 0
        
        Args:
            recommended: List of recommended item IDs
            relevant: Set of truly relevant item IDs
            k: Cutoff position
        
        Returns:
            1.0 if hit, 0.0 otherwise
        """
        if k <= 0:
            return 0.0
        
        top_k = set(recommended[:k])
        return 1.0 if top_k & relevant else 0.0
    
    @staticmethod
    def mrr(recommended: List[str], relevant: Set[str]) -> float:
        """
        Calculate Mean Reciprocal Rank.
        
        MRR = 1 / rank of first relevant item
        
        Args:
            recommended: List of recommended item IDs
            relevant: Set of truly relevant item IDs
        
        Returns:
            MRR score (0-1)
        """
        for rank, item in enumerate(recommended, 1):
            if item in relevant:
                return 1.0 / rank
        
        return 0.0
    
    @staticmethod
    def evaluate_recommendations(
        recommended: List[str],
        relevant: Set[str],
        k_values: List[int] = [5, 10, 20]
    ) -> Dict[str, Any]:
        """
        Comprehensive evaluation with multiple metrics.
        
        Args:
            recommended: List of recommended item IDs
            relevant: Set of truly relevant item IDs
            k_values: List of K values to evaluate
        
        Returns:
            Dictionary with all metrics
        """
        results = {
            'precision': {},
            'recall': {},
            'ndcg': {},
            'hit_rate': {},
            'map': RecommendationEvaluator.average_precision(recommended, relevant),
            'mrr': RecommendationEvaluator.mrr(recommended, relevant)
        }
        
        for k in k_values:
            results['precision'][f'@{k}'] = RecommendationEvaluator.precision_at_k(
                recommended, relevant, k
            )
            results['recall'][f'@{k}'] = RecommendationEvaluator.recall_at_k(
                recommended, relevant, k
            )
            results['ndcg'][f'@{k}'] = RecommendationEvaluator.ndcg_at_k(
                recommended, relevant, k
            )
            results['hit_rate'][f'@{k}'] = RecommendationEvaluator.hit_rate_at_k(
                recommended, relevant, k
            )
        
        return results
    
    @staticmethod
    def evaluate_batch(
        recommendations: List[List[str]],
        relevant_sets: List[Set[str]],
        k_values: List[int] = [5, 10, 20]
    ) -> Dict[str, Any]:
        """
        Evaluate multiple recommendation lists (e.g., different users/queries).
        
        Returns averaged metrics across all evaluations.
        
        Args:
            recommendations: List of recommendation lists
            relevant_sets: List of relevant item sets
            k_values: K values to evaluate
        
        Returns:
            Dictionary with averaged metrics
        """
        if len(recommendations) != len(relevant_sets):
            raise ValueError("Mismatch in number of recommendations and relevant sets")
        
        # Collect all individual results
        all_results = [
            RecommendationEvaluator.evaluate_recommendations(rec, rel, k_values)
            for rec, rel in zip(recommendations, relevant_sets)
        ]
        
        # Average the results
        averaged = {
            'precision': {},
            'recall': {},
            'ndcg': {},
            'hit_rate': {},
            'map': np.mean([r['map'] for r in all_results]),
            'mrr': np.mean([r['mrr'] for r in all_results])
        }
        
        for k in k_values:
            key = f'@{k}'
            averaged['precision'][key] = np.mean([r['precision'][key] for r in all_results])
            averaged['recall'][key] = np.mean([r['recall'][key] for r in all_results])
            averaged['ndcg'][key] = np.mean([r['ndcg'][key] for r in all_results])
            averaged['hit_rate'][key] = np.mean([r['hit_rate'][key] for r in all_results])
        
        return averaged


# Global evaluator instance
evaluator = RecommendationEvaluator()
