"""
ML/AI Dataset Filtering Pipeline.
Multi-stage filtering to identify high-quality datasets suitable for machine learning.
"""
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import re
import logging

logger = logging.getLogger(__name__)


class FilterResult(Enum):
    """Result of a filter stage."""
    PASS = "pass"
    FAIL = "fail"
    SOFT_FAIL = "soft_fail"  # Low score but not rejected


@dataclass
class DatasetScore:
    """Scoring result for a dataset."""
    ml_suitability: float  # 0-1 score
    modality: str  # image, text, tabular, audio, etc.
    potential_tasks: List[str]  # classification, regression, etc.
    confidence: float  # 0-1 confidence in the scoring
    filter_stages_passed: List[str]
    rejection_reason: Optional[str] = None


class MLDatasetFilter:
    """
    Multi-stage filtering pipeline for ML/AI dataset relevance.
    
    Stages:
    1. Format & Structure Filter (hard)
    2. Size & Usability Filter (hard)
    3. Metadata Keyword Filter (scoring)
    4. Domain & Task Relevance Filter (scoring)
    5. AI-Based Relevance Scoring (soft)
    """
    
    # ===== STAGE 1: Format Filter =====
    ALLOWED_FORMATS = {
        # Structured data
        'csv', 'json', 'parquet', 'tsv', 'xlsx', 'xls', 'feather', 'arrow',
        'hdf5', 'h5', 'pickle', 'pkl', 'npy', 'npz', 'tfrecord',
        # Images
        'jpg', 'jpeg', 'png', 'gif', 'bmp', 'tiff', 'webp', 'svg',
        # Audio
        'wav', 'mp3', 'flac', 'ogg', 'm4a', 'aac',
        # Video
        'mp4', 'avi', 'mov', 'mkv', 'webm',
        # Text
        'txt', 'text', 'jsonl', 'xml', 'yaml', 'yml',
    }
    
    EXCLUDED_FORMATS = {
        'pdf', 'doc', 'docx', 'ppt', 'pptx', 'html', 'htm',
        'rtf', 'odt', 'odp', 'epub', 'mobi',
    }
    
    # ===== STAGE 3: Keyword Scoring =====
    ML_POSITIVE_KEYWORDS = {
        # High value (weight 3)
        'machine learning': 3, 'deep learning': 3, 'neural network': 3,
        'training data': 3, 'benchmark': 3, 'labeled': 3, 'annotated': 3,
        'classification': 3, 'regression': 3, 'clustering': 3,
        'computer vision': 3, 'natural language': 3, 'nlp': 3,
        'image recognition': 3, 'object detection': 3, 'segmentation': 3,
        'sentiment analysis': 3, 'named entity': 3, 'speech recognition': 3,
        
        # Medium value (weight 2)
        'prediction': 2, 'forecast': 2, 'time series': 2, 'sensor': 2,
        'features': 2, 'target': 2, 'labels': 2, 'annotations': 2,
        'train': 2, 'test': 2, 'validation': 2, 'evaluation': 2,
        'model': 2, 'algorithm': 2, 'accuracy': 2, 'precision': 2,
        'dataset': 2, 'corpus': 2, 'embeddings': 2, 'vectors': 2,
        'images': 2, 'audio': 2, 'text': 2, 'tabular': 2,
        
        # Lower value (weight 1)
        'data': 1, 'records': 1, 'samples': 1, 'observations': 1,
        'structured': 1, 'attributes': 1, 'variables': 1,
        'research': 1, 'analysis': 1, 'study': 1,
    }
    
    ML_NEGATIVE_KEYWORDS = {
        # Strong negative (weight -3)
        'policy document': -3, 'legislation': -3, 'legal brief': -3,
        'compliance report': -3, 'administrative': -3, 'regulatory': -3,
        'meeting minutes': -3, 'annual report': -3, 'budget': -3,
        'procurement': -3, 'contract': -3, 'invoice': -3,
        
        # Medium negative (weight -2)
        'report': -2, 'documentation': -2, 'manual': -2, 'guide': -2,
        'memo': -2, 'newsletter': -2, 'press release': -2,
        'form': -2, 'application': -2, 'permit': -2,
        
        # Slight negative (weight -1)
        'summary': -1, 'overview': -1, 'catalog': -1,
    }
    
    # ===== STAGE 4: Domain Mapping =====
    ML_DOMAINS = {
        'computer vision': ['image', 'photo', 'picture', 'visual', 'video', 'camera', 
                           'object', 'face', 'scene', 'detection', 'recognition'],
        'natural language processing': ['text', 'language', 'nlp', 'sentiment', 'translation',
                                        'summarization', 'question answering', 'dialogue',
                                        'corpus', 'vocabulary', 'tokenize'],
        'speech and audio': ['audio', 'speech', 'voice', 'sound', 'acoustic', 'music',
                            'speaker', 'transcription', 'asr'],
        'time series': ['time series', 'temporal', 'forecast', 'prediction', 'sequence',
                       'sensor', 'iot', 'signal', 'stock', 'weather'],
        'tabular': ['tabular', 'table', 'csv', 'structured', 'features', 'columns',
                   'rows', 'records', 'database'],
        'graph': ['graph', 'network', 'nodes', 'edges', 'social', 'knowledge graph'],
        'multimodal': ['multimodal', 'image-text', 'vision-language', 'video-text'],
        'reinforcement learning': ['reinforcement', 'reward', 'agent', 'environment',
                                  'game', 'simulation', 'control'],
    }
    
    ML_TASKS = {
        'classification': ['classification', 'classify', 'categorize', 'category', 'class'],
        'regression': ['regression', 'predict', 'continuous', 'numerical'],
        'detection': ['detection', 'detect', 'locate', 'bounding box', 'localization'],
        'segmentation': ['segmentation', 'segment', 'mask', 'pixel-wise'],
        'generation': ['generation', 'generate', 'synthesis', 'create'],
        'clustering': ['clustering', 'cluster', 'grouping', 'unsupervised'],
        'retrieval': ['retrieval', 'search', 'similarity', 'matching'],
        'translation': ['translation', 'translate', 'parallel corpus'],
        'summarization': ['summarization', 'summarize', 'summary'],
        'question answering': ['question answering', 'qa', 'reading comprehension'],
    }
    
    def __init__(
        self,
        min_ml_score: float = 0.3,
        min_size_rows: int = 100,
        use_ai_scoring: bool = True
    ):
        """
        Initialize the filter.
        
        Args:
            min_ml_score: Minimum ML suitability score (0-1) to pass
            min_size_rows: Minimum number of rows/samples to consider
            use_ai_scoring: Whether to use AI-based scoring (Stage 5)
        """
        self.min_ml_score = min_ml_score
        self.min_size_rows = min_size_rows
        self.use_ai_scoring = use_ai_scoring
    
    def filter_dataset(self, dataset: Dict[str, Any]) -> Tuple[bool, DatasetScore]:
        """
        Run the full filtering pipeline on a dataset.
        
        Args:
            dataset: Dataset metadata dict
            
        Returns:
            Tuple of (should_include, score)
        """
        stages_passed = []
        
        # Stage 1: Format Filter
        if not self._stage1_format_filter(dataset):
            return False, DatasetScore(
                ml_suitability=0,
                modality='unknown',
                potential_tasks=[],
                confidence=0.9,
                filter_stages_passed=stages_passed,
                rejection_reason="Failed format filter - excluded file types"
            )
        stages_passed.append('format')
        
        # Stage 2: Size Filter
        if not self._stage2_size_filter(dataset):
            return False, DatasetScore(
                ml_suitability=0,
                modality='unknown',
                potential_tasks=[],
                confidence=0.8,
                filter_stages_passed=stages_passed,
                rejection_reason="Failed size filter - too small or metadata-only"
            )
        stages_passed.append('size')
        
        # Stage 3: Keyword Scoring
        keyword_score = self._stage3_keyword_score(dataset)
        if keyword_score < -5:  # Strong negative signal
            return False, DatasetScore(
                ml_suitability=0,
                modality='unknown',
                potential_tasks=[],
                confidence=0.7,
                filter_stages_passed=stages_passed,
                rejection_reason="Failed keyword filter - non-ML content"
            )
        stages_passed.append('keywords')
        
        # Stage 4: Domain & Task Detection
        domain, tasks = self._stage4_domain_task_detection(dataset)
        if not domain and keyword_score < 2:
            return False, DatasetScore(
                ml_suitability=0.1,
                modality='unknown',
                potential_tasks=[],
                confidence=0.6,
                filter_stages_passed=stages_passed,
                rejection_reason="No identifiable ML domain or task"
            )
        stages_passed.append('domain')
        
        # Calculate base ML suitability score
        ml_score = self._calculate_ml_score(keyword_score, domain, tasks)
        
        # Stage 5: AI-Based Scoring (optional)
        if self.use_ai_scoring and ml_score >= 0.2:
            ai_score = self._stage5_ai_scoring(dataset)
            if ai_score is not None:
                ml_score = (ml_score * 0.6) + (ai_score * 0.4)
                stages_passed.append('ai_scoring')
        
        # Determine modality
        modality = self._detect_modality(dataset, domain)
        
        # Final decision
        passed = ml_score >= self.min_ml_score
        
        return passed, DatasetScore(
            ml_suitability=ml_score,
            modality=modality,
            potential_tasks=tasks,
            confidence=0.8 if domain else 0.5,
            filter_stages_passed=stages_passed,
            rejection_reason=None if passed else f"ML score {ml_score:.2f} below threshold {self.min_ml_score}"
        )
    
    def _stage1_format_filter(self, dataset: Dict[str, Any]) -> bool:
        """Check if dataset has allowed file formats."""
        # Extract format information
        formats = self._extract_formats(dataset)
        resources = dataset.get('resources', [])
        
        # Check if any resource has excluded format
        has_excluded_only = False
        has_allowed = False
        
        for fmt in formats:
            if fmt.lower() in self.ALLOWED_FORMATS:
                has_allowed = True
            if fmt.lower() in self.EXCLUDED_FORMATS:
                has_excluded_only = True
        
        # Also check resource URLs/names
        for resource in resources:
            url = resource.get('url', '') or resource.get('name', '')
            for allowed in self.ALLOWED_FORMATS:
                if f'.{allowed}' in url.lower():
                    has_allowed = True
                    break
        
        # Pass if has any allowed format, or no format info (benefit of doubt)
        return has_allowed or (not formats and not has_excluded_only and len(resources) == 0)
    
    def _stage2_size_filter(self, dataset: Dict[str, Any]) -> bool:
        """Check if dataset meets size requirements."""
        # Check for size indicators
        num_rows = dataset.get('num_rows') or dataset.get('size', {}).get('samples')
        file_size = dataset.get('file_size_bytes') or dataset.get('size', {}).get('file_size_bytes')
        
        # If we have row count, check minimum
        if num_rows is not None:
            try:
                if int(num_rows) < self.min_size_rows:
                    return False
            except (ValueError, TypeError):
                pass
        
        # If file size is very small (< 1KB), likely metadata-only
        if file_size is not None:
            try:
                if int(file_size) < 1024:  # Less than 1KB
                    return False
            except (ValueError, TypeError):
                pass
        
        # Check for metadata-only indicators
        description = (dataset.get('description') or '').lower()
        if 'metadata only' in description or 'no data' in description:
            return False
        
        return True
    
    def _stage3_keyword_score(self, dataset: Dict[str, Any]) -> int:
        """Calculate keyword-based ML relevance score."""
        # Combine all text fields
        text = ' '.join([
            dataset.get('canonical_name', ''),
            dataset.get('name', ''),
            dataset.get('title', ''),
            dataset.get('description', ''),
            ' '.join(dataset.get('tags', []) if isinstance(dataset.get('tags'), list) else []),
            ' '.join(dataset.get('keywords', []) if isinstance(dataset.get('keywords'), list) else []),
        ]).lower()
        
        score = 0
        
        # Add positive keyword scores
        for keyword, weight in self.ML_POSITIVE_KEYWORDS.items():
            if keyword in text:
                score += weight
        
        # Add negative keyword scores
        for keyword, weight in self.ML_NEGATIVE_KEYWORDS.items():
            if keyword in text:
                score += weight  # weight is already negative
        
        return score
    
    def _stage4_domain_task_detection(
        self, 
        dataset: Dict[str, Any]
    ) -> Tuple[Optional[str], List[str]]:
        """Detect ML domain and potential tasks."""
        text = ' '.join([
            dataset.get('canonical_name', ''),
            dataset.get('name', ''),
            dataset.get('title', ''),
            dataset.get('description', ''),
        ]).lower()
        
        # Detect domain
        detected_domain = None
        max_domain_score = 0
        
        for domain, keywords in self.ML_DOMAINS.items():
            score = sum(1 for kw in keywords if kw in text)
            if score > max_domain_score:
                max_domain_score = score
                detected_domain = domain
        
        # Detect tasks
        detected_tasks = []
        for task, keywords in self.ML_TASKS.items():
            if any(kw in text for kw in keywords):
                detected_tasks.append(task)
        
        return detected_domain, detected_tasks
    
    def _stage5_ai_scoring(self, dataset: Dict[str, Any]) -> Optional[float]:
        """
        Use AI/LLM to score ML relevance.
        Returns score 0-1 or None if AI scoring unavailable.
        """
        try:
            from app.config import settings
            provider = getattr(settings, 'llm_provider', 'gemini').lower()
            
            client = None
            if provider == 'gemini':
                from app.llm.gemini_client import gemini_client
                client = gemini_client
            elif provider == 'grok':
                from app.llm.xai_client import xai_client
                client = xai_client
            else:
                from app.llm.huggingface_client import huggingface_client
                client = huggingface_client

            if not client or not client.enabled:
                return None
            
            # Create prompt for scoring
            text = f"""
Dataset Name: {dataset.get('canonical_name') or dataset.get('name', 'Unknown')}
Description: {dataset.get('description', 'No description')[:500]}

Is this dataset suitable for machine learning/AI tasks? 
Rate from 0.0 (not suitable) to 1.0 (highly suitable).
Only respond with a single decimal number.
"""
            # Wait for response
            import asyncio
            response = asyncio.run(client.generate_text(text, max_tokens=10, temperature=0.1))
            
            if response:
                try:
                    # Extract number using regex
                    match = re.search(r"(\d+\.\d+|\d+)", response)
                    if match:
                        return float(match.group(1))
                except (ValueError, TypeError):
                    pass
            return None
            
        except Exception as e:
            logger.warning(f"AI scoring failed: {e}")
            return None
    
    def _calculate_ml_score(
        self, 
        keyword_score: int, 
        domain: Optional[str], 
        tasks: List[str]
    ) -> float:
        """Calculate normalized ML suitability score."""
        # Base score from keywords (normalize -10 to +20 -> 0 to 1)
        normalized_keyword = max(0, min(1, (keyword_score + 10) / 30))
        
        # Domain bonus
        domain_bonus = 0.2 if domain else 0
        
        # Task bonus
        task_bonus = min(0.2, len(tasks) * 0.05)
        
        # Combined score
        score = (normalized_keyword * 0.6) + domain_bonus + task_bonus
        
        return min(1.0, score)
    
    def _detect_modality(self, dataset: Dict[str, Any], domain: Optional[str]) -> str:
        """Detect the primary data modality."""
        if domain:
            modality_map = {
                'computer vision': 'image',
                'natural language processing': 'text',
                'speech and audio': 'audio',
                'time series': 'tabular',
                'tabular': 'tabular',
                'graph': 'graph',
                'multimodal': 'multimodal',
                'reinforcement learning': 'simulation',
            }
            return modality_map.get(domain, 'unknown')
        
        # Fallback: check formats
        formats = self._extract_formats(dataset)
        if any(f in ['jpg', 'jpeg', 'png', 'gif', 'bmp'] for f in formats):
            return 'image'
        if any(f in ['wav', 'mp3', 'flac', 'ogg'] for f in formats):
            return 'audio'
        if any(f in ['csv', 'json', 'parquet', 'tsv'] for f in formats):
            return 'tabular'
        if any(f in ['txt', 'text', 'jsonl'] for f in formats):
            return 'text'
        
        return 'unknown'
    
    def _extract_formats(self, dataset: Dict[str, Any]) -> List[str]:
        """Extract file formats from dataset metadata."""
        formats = []
        
        # Direct format field
        if 'format' in dataset:
            fmt = dataset['format']
            if isinstance(fmt, str):
                formats.append(fmt.lower())
            elif isinstance(fmt, list):
                formats.extend([f.lower() for f in fmt])
        
        # Resources field (common in CKAN APIs)
        for resource in dataset.get('resources', []):
            if 'format' in resource:
                formats.append(resource['format'].lower())
            if 'url' in resource:
                # Extract extension from URL
                url = resource['url']
                match = re.search(r'\.([a-zA-Z0-9]+)(?:\?|$)', url)
                if match:
                    formats.append(match.group(1).lower())
        
        return formats
    
    def filter_batch(
        self, 
        datasets: List[Dict[str, Any]], 
        max_results: int = 10000
    ) -> List[Tuple[Dict[str, Any], DatasetScore]]:
        """
        Filter a batch of datasets and return top results.
        
        Args:
            datasets: List of dataset dicts
            max_results: Maximum number to return
            
        Returns:
            List of (dataset, score) tuples, sorted by ML suitability
        """
        results = []
        
        for dataset in datasets:
            passed, score = self.filter_dataset(dataset)
            if passed:
                results.append((dataset, score))
        
        # Sort by ML suitability score
        results.sort(key=lambda x: x[1].ml_suitability, reverse=True)
        
        # Return top results
        return results[:max_results]


# Global filter instance
ml_filter = MLDatasetFilter()
