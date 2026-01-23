"""
Quality scoring and prediction system for datasets.
Evaluates dataset quality based on multiple factors.
"""
import math
from typing import Dict, Any, Optional
from datetime import datetime
import re


class QualityScorer:
    """
    Multi-factor quality assessment for datasets.
    Combines completeness, documentation, metadata richness, and community validation.
    """
    
    def calculate_quality_score(self, dataset: Dict[str, Any]) -> float:
        """
        Calculate comprehensive quality score (0-1).
        
        Factors:
        - Completeness (30%): Has description, license, size info
        - Documentation (25%): Description quality and length
        - Metadata Richness (20%): Number of metadata fields
        - Community Validation (25%): Downloads, likes, usage indicators
        
        Args:
            dataset: Dataset document from MongoDB
        
        Returns:
            Quality score between 0 and 1
        """
        score = 0.0
        
        # 1. Completeness (30%)
        completeness_score = self._calculate_completeness(dataset)
        score += completeness_score * 0.30
        
        # 2. Documentation Quality (25%)
        doc_score = self._calculate_documentation_quality(dataset)
        score += doc_score * 0.25
        
        # 3. Metadata Richness (20%)
        metadata_score = self._calculate_metadata_richness(dataset)
        score += metadata_score * 0.20
        
        # 4. Community Validation (25%)
        community_score = self._calculate_community_validation(dataset)
        score += community_score * 0.25
        
        return min(1.0, max(0.0, score))
    
    def _calculate_completeness(self, dataset: Dict[str, Any]) -> float:
        """
        Check for presence of essential fields.
        """
        score = 0.0
        max_points = 10
        
        # Description (2 points)
        if dataset.get('description'):
            score += 2
        
        # License (2 points)
        if dataset.get('license'):
            score += 2
        
        # Size information (2 points)
        size = dataset.get('size', {})
        if size.get('samples') or size.get('file_size_gb'):
            score += 2
        
        # Domain classification (1 point)
        if dataset.get('domain'):
            score += 1
        
        # Modality classification (1 point)
        if dataset.get('modality'):
            score += 1
        
        # Source URL (1 point)
        if dataset.get('source', {}).get('url'):
            score += 1
        
        # Metadata (1 point)
        if dataset.get('metadata'):
            score += 1
        
        return score / max_points
    
    def _calculate_documentation_quality(self, dataset: Dict[str, Any]) -> float:
        """
        Evaluate description quality and length.
        """
        description = dataset.get('description', '')
        
        if not description:
            return 0.0
        
        score = 0.0
        
        # Length score (0-0.4)
        # Optimal length: 200-1000 characters
        desc_length = len(description)
        if desc_length >= 200:
            length_score = min(0.4, desc_length / 2500)  # Cap at 1000 chars
        else:
            length_score = (desc_length / 200) * 0.4  # Penalize short descriptions
        score += length_score
        
        # Readability indicators (0-0.3)
        # Has proper sentences (ends with punctuation)
        stripped_desc = description.strip()
        if stripped_desc and stripped_desc[-1] in '.!?':
            score += 0.1
        
        # Has multiple sentences
        if description.count('.') + description.count('!') + description.count('?') >= 2:
            score += 0.1
        
        # Not too simple (avoid single-word descriptions)
        if ' ' in description and len(description.split()) >= 10:
            score += 0.1
        
        # Structure indicators (0-0.3)
        # Has numbers (likely describes dataset size, features)
        if re.search(r'\d+', description):
            score += 0.1
        
        # Has keywords (dataset, data, contains, includes)
        keywords = ['dataset', 'data', 'contains', 'includes', 'features', 'samples', 'rows']
        if any(kw in description.lower() for kw in keywords):
            score += 0.1
        
        # Not generic (avoid "This is a dataset")
        generic_phrases = ['this is a dataset', 'dataset for', 'data for']
        is_generic = any(phrase in description.lower() for phrase in generic_phrases)
        if not is_generic:
            score += 0.1
        
        return min(1.0, score)
    
    def _calculate_metadata_richness(self, dataset: Dict[str, Any]) -> float:
        """
        Evaluate quantity and quality of metadata fields.
        """
        metadata = dataset.get('metadata', {})
        
        if not metadata:
            return 0.0
        
        score = 0.0
        
        # Number of metadata fields (0-0.5)
        field_count = len(metadata)
        field_score = min(0.5, field_count / 20)  # 20 fields = max score
        score += field_score
        
        # Quality indicators (0-0.5)
        valuable_fields = [
            'num_instances', 'num_features', 'num_classes',
            'downloads', 'likes', 'votes', 'usability_score',
            'format', 'version', 'last_updated', 'creator_name',
            'tags', 'subjects', 'authors'
        ]
        
        present_valuable = sum(1 for field in valuable_fields if field in metadata)
        score += (present_valuable / len(valuable_fields)) * 0.5
        
        return min(1.0, score)
    
    def _calculate_community_validation(self, dataset: Dict[str, Any]) -> float:
        """
        Evaluate community engagement and validation.
        """
        source_metadata = dataset.get('source', {}).get('source_metadata', {})
        
        score = 0.0
        
        # Downloads (0-0.35)
        downloads = source_metadata.get('downloads', 0)
        if downloads > 0:
            # Log scale to handle wide range
            download_score = min(0.35, math.log10(downloads + 1) / math.log10(100000))
            score += download_score
        
        # Likes/Votes (0-0.35)
        likes = source_metadata.get('likes', 0)
        votes = source_metadata.get('votes', 0)
        popularity = likes + votes
        if popularity > 0:
            popularity_score = min(0.35, math.log10(popularity + 1) / math.log10(10000))
            score += popularity_score
        
        # Views (0-0.15)
        views = source_metadata.get('views', 0)
        if views > 0:
            view_score = min(0.15, math.log10(views + 1) / math.log10(50000))
            score += view_score
        
        # Usability/Quality rating (0-0.15)
        usability = source_metadata.get('usability_score', 0)
        if usability > 0:
            # Assuming usability is 0-10 scale (Kaggle)
            score += (usability / 10.0) * 0.15
        
        return min(1.0, score)
    
    def get_quality_breakdown(self, dataset: Dict[str, Any]) -> Dict[str, float]:
        """
        Get detailed breakdown of quality components.
        Useful for explaining scores to users.
        
        Returns:
            Dictionary with component scores
        """
        return {
            'overall': self.calculate_quality_score(dataset),
            'completeness': self._calculate_completeness(dataset),
            'documentation': self._calculate_documentation_quality(dataset),
            'metadata_richness': self._calculate_metadata_richness(dataset),
            'community_validation': self._calculate_community_validation(dataset)
        }
    
    def get_quality_label(self, score: float) -> str:
        """
        Convert numeric score to human-readable label.
        
        Args:
            score: Quality score (0-1)
        
        Returns:
            Quality label string
        """
        if score >= 0.8:
            return "Excellent"
        elif score >= 0.6:
            return "Good"
        elif score >= 0.4:
            return "Fair"
        elif score >= 0.2:
            return "Poor"
        else:
            return "Very Poor"


# Global instance
quality_scorer = QualityScorer()
