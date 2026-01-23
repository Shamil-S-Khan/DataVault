"""
Comprehensive evaluation framework for the recommendation system.
Tests system performance with various metrics.
"""
import asyncio
import logging
import random
from typing import List, Dict, Set
from motor.motor_asyncio import AsyncIOMotorClient
from app.config import settings
from app.ml.recommender import DatasetRecommender
from app.ml.evaluation import RecommendationEvaluator
from bson import ObjectId

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def get_ground_truth_relevance(
    db,
    target_id: str,
    candidates: List[str],
    relevance_criteria: str = 'same_domain'
) -> Set[str]:
    """
    Generate ground truth relevant items based on criteria.
    
    Args:
        db: Database connection
        target_id: Target dataset ID
        candidates: List of candidate dataset IDs
        relevance_criteria: How to define relevance ('same_domain', 'same_modality', 'both')
    
    Returns:
        Set of relevant dataset IDs
    """
    # Get target dataset
    target = await db.datasets.find_one({'_id': ObjectId(target_id)})
    if not target:
        return set()
    
    # Build relevance query
    relevance_query = {'_id': {'$in': [ObjectId(cid) for cid in candidates]}}
    
    if relevance_criteria == 'same_domain':
        relevance_query['domain'] = target.get('domain')
    elif relevance_criteria == 'same_modality':
        relevance_query['modality'] = target.get('modality')
    elif relevance_criteria == 'both':
        relevance_query['domain'] = target.get('domain')
        relevance_query['modality'] = target.get('modality')
    
    # Get relevant datasets
    relevant = await db.datasets.find(relevance_query, {'_id': 1}).to_list(None)
    
    return {str(d['_id']) for d in relevant}


async def evaluate_recommendation_system():
    """
    Run comprehensive evaluation of the recommendation system.
    """
    # Connect to MongoDB
    client = AsyncIOMotorClient(settings.mongodb_uri)
    db = client[settings.mongodb_db_name]
    
    logger.info("Connected to MongoDB")
    
    # Initialize recommender and evaluator
    recommender = DatasetRecommender(db)
    evaluator = RecommendationEvaluator()
    
    # Get sample of datasets with embeddings for testing
    sample_size = 100
    datasets = await db.datasets.find(
        {'embedding_vector': {'$exists': True, '$ne': None}},
        {'_id': 1, 'canonical_name': 1, 'domain': 1, 'modality': 1}
    ).limit(sample_size).to_list(None)
    
    logger.info(f"Testing with {len(datasets)} datasets")
    
    if len(datasets) < 10:
        logger.error("Not enough datasets with embeddings. Run generate_embeddings.py first.")
        client.close()
        return
    
    # Randomly select test datasets
    test_datasets = random.sample(datasets, min(50, len(datasets)))
    
    logger.info(f"Selected {len(test_datasets)} test datasets")
    
    # Collect recommendations for each test dataset
    all_recommendations = []
    all_relevant_sets = []
    
    k_values = [5, 10, 20]
    
    for i, dataset in enumerate(test_datasets, 1):
        dataset_id = str(dataset['_id'])
        
        try:
            # Get recommendations
            recommendations = await recommender.get_similar_datasets(
                dataset_id=dataset_id,
                limit=20
            )
            
            recommended_ids = [rec['id'] for rec in recommendations]
            
            # Get ground truth (datasets with same domain)
            all_dataset_ids = [str(d['_id']) for d in datasets if str(d['_id']) != dataset_id]
            relevant_ids = await get_ground_truth_relevance(
                db, dataset_id, all_dataset_ids, 'same_domain'
            )
            
            all_recommendations.append(recommended_ids)
            all_relevant_sets.append(relevant_ids)
            
            if i % 10 == 0:
                logger.info(f"Processed {i}/{len(test_datasets)} test cases")
        
        except Exception as e:
            logger.warning(f"Error processing dataset {dataset_id}: {e}")
            continue
    
    logger.info(f"Completed {len(all_recommendations)} test cases")
    
    # Calculate metrics
    logger.info("\n" + "="*60)
    logger.info("RECOMMENDATION SYSTEM EVALUATION RESULTS")
    logger.info("="*60)
    
    # Batch evaluation
    results = evaluator.evaluate_batch(
        recommendations=all_recommendations,
        relevant_sets=all_relevant_sets,
        k_values=k_values
    )
    
    logger.info("\n📊 AVERAGED METRICS:")
    logger.info(f"  MAP (Mean Average Precision): {results['map']:.4f}")
    logger.info(f"  MRR (Mean Reciprocal Rank):   {results['mrr']:.4f}")
    
    logger.info("\n📈 PRECISION @ K:")
    for k in k_values:
        logger.info(f"  P@{k:2d}: {results['precision'][f'@{k}']:.4f}")
    
    logger.info("\n🎯 RECALL @ K:")
    for k in k_values:
        logger.info(f"  R@{k:2d}: {results['recall'][f'@{k}']:.4f}")
    
    logger.info("\n⚡ NDCG @ K:")
    for k in k_values:
        logger.info(f"  NDCG@{k:2d}: {results['ndcg'][f'@{k}']:.4f}")
    
    logger.info("\n🎲 HIT RATE @ K:")
    for k in k_values:
        logger.info(f"  HR@{k:2d}: {results['hit_rate'][f'@{k}']:.4f}")
    
    # Baseline comparison (random recommendations)
    logger.info("\n" + "="*60)
    logger.info("BASELINE COMPARISON (Random Recommendations)")
    logger.info("="*60)
    
    random_recommendations = []
    for _ in range(len(test_datasets)):
        # Generate random recommendations
        random_sample = random.sample(
            [str(d['_id']) for d in datasets],
            min(20, len(datasets))
        )
        random_recommendations.append(random_sample)
    
    baseline_results = evaluator.evaluate_batch(
        recommendations=random_recommendations,
        relevant_sets=all_relevant_sets,
        k_values=k_values
    )
    
    logger.info("\n📊 BASELINE METRICS:")
    logger.info(f"  MAP: {baseline_results['map']:.4f}")
    logger.info(f"  MRR: {baseline_results['mrr']:.4f}")
    
    logger.info("\n📊 IMPROVEMENT OVER BASELINE:")
    map_improvement = ((results['map'] - baseline_results['map']) / baseline_results['map'] * 100) if baseline_results['map'] > 0 else 0
    mrr_improvement = ((results['mrr'] - baseline_results['mrr']) / baseline_results['mrr'] * 100) if baseline_results['mrr'] > 0 else 0
    
    logger.info(f"  MAP improvement: +{map_improvement:.2f}%")
    logger.info(f"  MRR improvement: +{mrr_improvement:.2f}%")
    
    for k in k_values:
        p_key = f'@{k}'
        if baseline_results['precision'][p_key] > 0:
            improvement = ((results['precision'][p_key] - baseline_results['precision'][p_key]) / 
                         baseline_results['precision'][p_key] * 100)
            logger.info(f"  P@{k} improvement: +{improvement:.2f}%")
    
    # Save results to file
    logger.info("\n" + "="*60)
    logger.info("Saving results to evaluation_results.json")
    
    import json
    output = {
        'test_config': {
            'num_test_datasets': len(test_datasets),
            'relevance_criteria': 'same_domain',
            'k_values': k_values
        },
        'ml_system': results,
        'random_baseline': baseline_results,
        'improvement': {
            'map_percent': round(map_improvement, 2),
            'mrr_percent': round(mrr_improvement, 2)
        }
    }
    
    with open('evaluation_results.json', 'w') as f:
        json.dump(output, f, indent=2)
    
    logger.info("✅ Evaluation complete!")
    
    # Close connection
    client.close()


if __name__ == "__main__":
    asyncio.run(evaluate_recommendation_system())
