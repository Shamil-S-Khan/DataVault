"""
Dataset Fitness Score Calculator.
Computes a 0-10 composite score based on multiple quality metrics.
"""
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
import logging
import re

logger = logging.getLogger(__name__)


class FitnessCalculator:
    """
    Calculate dataset fitness scores based on multiple quality dimensions.
    
    Scoring Dimensions (each 0-10, weighted):
    1. Metadata Completeness (20%) - Description, tags, author info
    2. Size Appropriateness (20%) - Reasonable size for ML tasks
    3. Documentation Quality (15%) - Description length & clarity
    4. License Clarity (15%) - Clear, well-known license
    5. Freshness (10%) - Recently updated/created
    6. Community Signals (20%) - Downloads, likes, citations
    """
    
    # Weights for each dimension
    WEIGHTS = {
        'metadata_completeness': 0.20,
        'size_appropriateness': 0.20,
        'documentation_quality': 0.15,
        'license_clarity': 0.15,
        'freshness': 0.10,
        'community_signals': 0.20,
    }
    
    # Known good licenses (MIT, Apache, etc.)
    CLEAR_LICENSES = {
        'mit', 'apache-2.0', 'apache 2.0', 'bsd-3-clause', 'bsd-2-clause',
        'cc-by-4.0', 'cc-by-sa-4.0', 'cc0-1.0', 'gpl-3.0', 'lgpl-3.0',
        'mpl-2.0', 'unlicense', 'wtfpl', 'isc', 'artistic-2.0',
        'cc-by', 'cc-by-sa', 'cc0', 'public domain', 'openrail'
    }
    
    def calculate_fitness_score(self, dataset: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate comprehensive fitness score for a dataset.
        
        Args:
            dataset: Dataset document from MongoDB
            
        Returns:
            Dictionary with overall score, breakdown, and explanation
        """
        breakdown = {}
        
        # 1. Metadata Completeness
        breakdown['metadata_completeness'] = self._score_metadata(dataset)
        
        # 2. Size Appropriateness
        breakdown['size_appropriateness'] = self._score_size(dataset)
        
        # 3. Documentation Quality
        breakdown['documentation_quality'] = self._score_documentation(dataset)
        
        # 4. License Clarity
        breakdown['license_clarity'] = self._score_license(dataset)
        
        # 5. Freshness
        breakdown['freshness'] = self._score_freshness(dataset)
        
        # 6. Community Signals
        breakdown['community_signals'] = self._score_community(dataset)
        
        # Calculate weighted average
        overall_score = sum(
            breakdown[key] * self.WEIGHTS[key]
            for key in breakdown
        )
        
        # Generate human-readable explanation
        explanation = self._generate_explanation(overall_score, breakdown, dataset)
        
        # Determine grade
        grade = self._get_grade(overall_score)
        
        return {
            'overall_score': round(overall_score, 1),
            'grade': grade,
            'breakdown': {k: round(v, 1) for k, v in breakdown.items()},
            'explanation': explanation,
            'calculated_at': datetime.utcnow().isoformat()
        }
    
    def _score_metadata(self, dataset: Dict[str, Any]) -> float:
        """Score based on metadata completeness."""
        score = 0.0
        max_score = 10.0
        
        # Has name (2 points)
        if dataset.get('canonical_name'):
            score += 2.0
        
        # Has description (2 points)
        if dataset.get('description'):
            score += 2.0
        
        # Has domain classification (1 point)
        if dataset.get('domain'):
            score += 1.0
        
        # Has modality (1 point)
        if dataset.get('modality'):
            score += 1.0
        
        # Has tags (1 point)
        metadata = dataset.get('source', {}).get('source_metadata', {})
        if metadata.get('tags') and len(metadata.get('tags', [])) > 0:
            score += 1.0
        
        # Has author info (1 point)
        if metadata.get('author'):
            score += 1.0
        
        # Has source URL (1 point)
        if dataset.get('source', {}).get('url'):
            score += 1.0
        
        # Has AI intelligence analysis (1 point bonus)
        if dataset.get('intelligence'):
            score += 1.0
        
        return min(score, max_score)
    
    def _score_size(self, dataset: Dict[str, Any]) -> float:
        """Score based on dataset size appropriateness."""
        metadata = dataset.get('source', {}).get('source_metadata', {})
        downloads = metadata.get('downloads', 0) or 0
        
        # Size info from metadata
        size_info = dataset.get('size', {})
        samples = size_info.get('samples') or metadata.get('num_examples', 0) or 0
        file_size = size_info.get('file_size_gb') or 0
        
        # If we have sample count
        if samples > 0:
            if samples < 100:
                return 3.0  # Too small
            elif samples < 1000:
                return 5.0  # Small but usable
            elif samples < 10000:
                return 7.0  # Good size
            elif samples < 100000:
                return 9.0  # Great size
            else:
                return 10.0  # Excellent
        
        # Fall back to downloads as proxy for size/quality
        if downloads > 0:
            if downloads < 100:
                return 4.0
            elif downloads < 1000:
                return 6.0
            elif downloads < 10000:
                return 7.5
            elif downloads < 100000:
                return 8.5
            else:
                return 10.0
        
        # Default if no size info
        return 5.0
    
    def _score_documentation(self, dataset: Dict[str, Any]) -> float:
        """Score based on documentation quality."""
        description = dataset.get('description', '') or ''
        score = 0.0
        
        # Length scoring (up to 5 points)
        desc_len = len(description)
        if desc_len > 500:
            score += 5.0
        elif desc_len > 200:
            score += 4.0
        elif desc_len > 100:
            score += 3.0
        elif desc_len > 50:
            score += 2.0
        elif desc_len > 0:
            score += 1.0
        
        # Quality indicators (up to 5 points)
        desc_lower = description.lower()
        
        # Contains usage instructions
        if any(word in desc_lower for word in ['use', 'usage', 'how to', 'example']):
            score += 1.0
        
        # Contains data format info
        if any(word in desc_lower for word in ['format', 'schema', 'column', 'field', 'csv', 'json']):
            score += 1.0
        
        # Contains task/purpose
        if any(word in desc_lower for word in ['task', 'purpose', 'designed for', 'intended']):
            score += 1.0
        
        # Contains size/statistics
        if any(word in desc_lower for word in ['samples', 'rows', 'images', 'examples', 'size']):
            score += 1.0
        
        # Has AI-generated summary
        intelligence = dataset.get('intelligence', {})
        if intelligence.get('summary'):
            score += 1.0
        
        return min(score, 10.0)
    
    def _score_license(self, dataset: Dict[str, Any]) -> float:
        """Score based on license clarity."""
        license_info = dataset.get('license', '') or ''
        metadata = dataset.get('source', {}).get('source_metadata', {})
        
        # Try to get license from various places
        if not license_info:
            license_info = metadata.get('license', '') or ''
        
        if not license_info:
            tags = metadata.get('tags', [])
            for tag in tags:
                if 'license:' in str(tag).lower():
                    license_info = str(tag).split(':')[-1]
                    break
        
        if not license_info:
            return 3.0  # Unknown license
        
        license_lower = license_info.lower().strip()
        
        # Check for clear licenses
        for clear_license in self.CLEAR_LICENSES:
            if clear_license in license_lower:
                # MIT and Apache are the clearest
                if 'mit' in license_lower or 'apache' in license_lower:
                    return 10.0
                elif 'cc0' in license_lower or 'public domain' in license_lower:
                    return 10.0
                elif 'cc-by' in license_lower:
                    return 9.0
                else:
                    return 8.0
        
        # Non-commercial licenses
        if 'non-commercial' in license_lower or 'nc' in license_lower:
            return 5.0
        
        # Restricted licenses
        if 'restricted' in license_lower or 'proprietary' in license_lower:
            return 2.0
        
        # Has some license but not clear
        return 4.0
    
    def _score_freshness(self, dataset: Dict[str, Any]) -> float:
        """Score based on how recently updated."""
        metadata = dataset.get('source', {}).get('source_metadata', {})
        
        # Try to get last modified date
        last_modified = metadata.get('last_modified')
        created_at = dataset.get('created_at') or dataset.get('scraped_at')
        
        date_to_check = last_modified or created_at
        
        if not date_to_check:
            return 5.0  # Default
        
        try:
            if isinstance(date_to_check, str):
                # Handle ISO format
                date_to_check = datetime.fromisoformat(date_to_check.replace('Z', '+00:00'))
            
            days_old = (datetime.utcnow() - date_to_check.replace(tzinfo=None)).days
            
            if days_old < 30:
                return 10.0  # Very fresh
            elif days_old < 90:
                return 9.0
            elif days_old < 180:
                return 8.0
            elif days_old < 365:
                return 7.0
            elif days_old < 730:
                return 5.0
            else:
                return 3.0  # Old
                
        except Exception:
            return 5.0
    
    def _score_community(self, dataset: Dict[str, Any]) -> float:
        """Score based on community signals (downloads, likes)."""
        metadata = dataset.get('source', {}).get('source_metadata', {})
        
        downloads = metadata.get('downloads', 0) or 0
        likes = metadata.get('likes', 0) or 0
        
        # Combined score
        score = 0.0
        
        # Downloads scoring (up to 6 points)
        if downloads > 1000000:
            score += 6.0
        elif downloads > 100000:
            score += 5.0
        elif downloads > 10000:
            score += 4.0
        elif downloads > 1000:
            score += 3.0
        elif downloads > 100:
            score += 2.0
        elif downloads > 0:
            score += 1.0
        
        # Likes scoring (up to 4 points)
        if likes > 1000:
            score += 4.0
        elif likes > 100:
            score += 3.0
        elif likes > 10:
            score += 2.0
        elif likes > 0:
            score += 1.0
        
        return min(score, 10.0)
    
    def _get_grade(self, score: float) -> str:
        """Convert numeric score to letter grade."""
        if score >= 9.0:
            return 'A+'
        elif score >= 8.0:
            return 'A'
        elif score >= 7.0:
            return 'B+'
        elif score >= 6.0:
            return 'B'
        elif score >= 5.0:
            return 'C+'
        elif score >= 4.0:
            return 'C'
        elif score >= 3.0:
            return 'D'
        else:
            return 'F'
    
    def _generate_explanation(
        self, 
        overall_score: float, 
        breakdown: Dict[str, float],
        dataset: Dict[str, Any]
    ) -> str:
        """Generate human-readable explanation of the score."""
        name = dataset.get('canonical_name', 'This dataset')
        grade = self._get_grade(overall_score)
        
        # Start with overall assessment
        if overall_score >= 8.0:
            explanation = f"{name} is an excellent dataset for ML projects. "
        elif overall_score >= 6.0:
            explanation = f"{name} is a good dataset with some room for improvement. "
        elif overall_score >= 4.0:
            explanation = f"{name} is usable but has notable limitations. "
        else:
            explanation = f"{name} has significant quality concerns. "
        
        # Add specific insights
        strengths = []
        weaknesses = []
        
        for dimension, score in breakdown.items():
            dim_name = dimension.replace('_', ' ').title()
            if score >= 8.0:
                strengths.append(dim_name)
            elif score < 5.0:
                weaknesses.append(dim_name)
        
        if strengths:
            explanation += f"Strengths: {', '.join(strengths)}. "
        
        if weaknesses:
            explanation += f"Areas for caution: {', '.join(weaknesses)}. "
        
        return explanation.strip()


# Singleton instance
fitness_calculator = FitnessCalculator()
