"""
Dataset quality scoring system.
Analyzes documentation, metadata completeness, and text quality.
"""
from typing import Dict, Any, Optional
import logging
import spacy
import textstat
from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId
from datetime import datetime
import re

logger = logging.getLogger(__name__)

# Load spaCy model
try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    logger.warning("spaCy model not found. Run: python -m spacy download en_core_web_sm")
    nlp = None


class QualityScorer:
    """Calculate quality scores for datasets."""
    
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
    
    def analyze_text_quality(self, text: str) -> Dict[str, float]:
        """
        Analyze text quality using NLP and readability metrics.
        
        Args:
            text: Input text
            
        Returns:
            Dictionary of quality metrics
        """
        if not text or len(text) < 10:
            return {
                'readability_score': 0.0,
                'entity_count': 0,
                'sentence_count': 0,
                'word_count': 0
            }
        
        metrics = {}
        
        # Readability score (Flesch Reading Ease)
        try:
            flesch_score = textstat.flesch_reading_ease(text)
            # Normalize to 0-1 (higher is better)
            metrics['readability_score'] = min(1.0, max(0.0, flesch_score / 100))
        except:
            metrics['readability_score'] = 0.5
        
        # Word and sentence count
        metrics['word_count'] = len(text.split())
        metrics['sentence_count'] = text.count('.') + text.count('!') + text.count('?')
        
        # Named entity recognition (if spaCy available)
        if nlp:
            try:
                doc = nlp(text[:1000])  # Limit to first 1000 chars for performance
                metrics['entity_count'] = len(doc.ents)
                
                # Entity diversity (unique entity types)
                entity_types = set([ent.label_ for ent in doc.ents])
                metrics['entity_diversity'] = len(entity_types)
            except:
                metrics['entity_count'] = 0
                metrics['entity_diversity'] = 0
        else:
            metrics['entity_count'] = 0
            metrics['entity_diversity'] = 0
        
        return metrics
    
    def check_documentation_completeness(
        self,
        dataset: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Check documentation completeness.
        
        Args:
            dataset: Dataset document
            
        Returns:
            Documentation completeness metrics
        """
        checks = {
            'has_description': bool(dataset.get('description')),
            'has_license': bool(dataset.get('license')),
            'has_domain': bool(dataset.get('domain')),
            'has_modality': bool(dataset.get('modality')),
            'has_size_info': bool(dataset.get('size')),
            'has_llm_summary': bool(dataset.get('llm_summary')),
            'has_sources': False  # Will check separately
        }
        
        # Check description length
        desc_length = len(dataset.get('description', ''))
        checks['description_adequate'] = desc_length >= 50
        
        # Check if size information is complete
        size = dataset.get('size', {})
        checks['has_sample_count'] = bool(size.get('samples'))
        checks['has_file_size'] = bool(size.get('file_size_gb'))
        
        # Calculate completeness score
        total_checks = len(checks)
        passed_checks = sum(1 for v in checks.values() if v)
        completeness_score = passed_checks / total_checks
        
        return {
            'checks': checks,
            'completeness_score': completeness_score,
            'passed_checks': passed_checks,
            'total_checks': total_checks
        }
    
    async def check_source_availability(
        self,
        dataset_id: ObjectId
    ) -> Dict[str, Any]:
        """
        Check if dataset has available sources.
        
        Args:
            dataset_id: Dataset ID
            
        Returns:
            Source availability metrics
        """
        sources = await self.db.dataset_sources.find({
            'dataset_id': dataset_id
        }).to_list(length=10)
        
        return {
            'source_count': len(sources),
            'has_sources': len(sources) > 0,
            'platforms': [s.get('platform') for s in sources]
        }
    
    def calculate_metadata_richness(
        self,
        dataset: Dict[str, Any]
    ) -> float:
        """
        Calculate metadata richness score.
        
        Args:
            dataset: Dataset document
            
        Returns:
            Metadata richness score (0-1)
        """
        metadata = dataset.get('metadata', {})
        
        # Count non-empty metadata fields
        rich_fields = 0
        total_possible = 10
        
        # Check for various metadata fields
        if metadata.get('downloads', 0) > 0:
            rich_fields += 1
        if metadata.get('stars', 0) > 0:
            rich_fields += 1
        if metadata.get('citations', 0) > 0:
            rich_fields += 1
        if metadata.get('views', 0) > 0:
            rich_fields += 1
        if metadata.get('likes', 0) > 0:
            rich_fields += 1
        if metadata.get('paper_count', 0) > 0:
            rich_fields += 1
        if metadata.get('homepage'):
            rich_fields += 1
        if metadata.get('author'):
            rich_fields += 1
        if metadata.get('last_updated'):
            rich_fields += 1
        if metadata.get('version'):
            rich_fields += 1
        
        return rich_fields / total_possible
    
    async def calculate_quality_score(
        self,
        dataset_id: ObjectId
    ) -> float:
        """
        Calculate overall quality score for a dataset.
        
        Args:
            dataset_id: Dataset ID
            
        Returns:
            Quality score (0-1)
        """
        # Get dataset
        dataset = await self.db.datasets.find_one({'_id': dataset_id})
        
        if not dataset:
            return 0.0
        
        # Component scores
        scores = {}
        
        # 1. Text quality (30% weight)
        description = dataset.get('description', '')
        text_metrics = self.analyze_text_quality(description)
        
        text_score = 0.0
        if text_metrics['word_count'] >= 20:
            text_score += 0.3
        if text_metrics['word_count'] >= 50:
            text_score += 0.2
        if text_metrics['readability_score'] > 0.5:
            text_score += 0.3
        if text_metrics['entity_count'] > 0:
            text_score += 0.2
        
        scores['text_quality'] = min(1.0, text_score)
        
        # 2. Documentation completeness (30% weight)
        doc_metrics = self.check_documentation_completeness(dataset)
        scores['documentation'] = doc_metrics['completeness_score']
        
        # 3. Source availability (20% weight)
        source_metrics = await self.check_source_availability(dataset_id)
        scores['sources'] = min(1.0, source_metrics['source_count'] / 3)
        
        # 4. Metadata richness (20% weight)
        scores['metadata'] = self.calculate_metadata_richness(dataset)
        
        # Calculate weighted score
        weights = {
            'text_quality': 0.3,
            'documentation': 0.3,
            'sources': 0.2,
            'metadata': 0.2
        }
        
        quality_score = sum(scores[k] * weights[k] for k in weights.keys())
        
        logger.debug(f"Quality score for {dataset_id}: {quality_score:.3f} {scores}")
        
        return quality_score
    
    async def update_all_quality_scores(
        self,
        limit: Optional[int] = None
    ):
        """
        Update quality scores for all datasets.
        
        Args:
            limit: Optional limit on number of datasets
        """
        logger.info("Starting quality score update for all datasets")
        
        cursor = self.db.datasets.find({})
        if limit:
            cursor = cursor.limit(limit)
        
        datasets = await cursor.to_list(length=limit or 10000)
        
        updated_count = 0
        
        for dataset in datasets:
            try:
                # Calculate quality score
                quality_score = await self.calculate_quality_score(dataset['_id'])
                
                # Update dataset
                await self.db.datasets.update_one(
                    {'_id': dataset['_id']},
                    {
                        '$set': {
                            'quality_score': quality_score,
                            'updated_at': datetime.utcnow()
                        }
                    }
                )
                
                updated_count += 1
                
                if updated_count % 100 == 0:
                    logger.info(f"Updated {updated_count} datasets")
                
            except Exception as e:
                logger.error(f"Error updating quality score for {dataset['_id']}: {e}")
        
        logger.info(f"Quality score update complete: {updated_count} datasets updated")
        
        return updated_count
