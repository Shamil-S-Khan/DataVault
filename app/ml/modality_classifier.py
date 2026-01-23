"""
ML-based modality classifier using zero-shot classification.
Replaces keyword-based detection with transformer models.
"""
from transformers import pipeline
import torch
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class ModalityClassifier:
    """
    ML-based modality classification using zero-shot learning.
    More accurate than keyword matching.
    """
    
    MODALITY_LABELS = [
        "image data",
        "text data",
        "audio data",
        "video data",
        "tabular data",
        "time series data",
        "graph data",
        "geospatial data"
    ]
    
    MODALITY_MAP = {
        "image data": "image",
        "text data": "text",
        "audio data": "audio",
        "video data": "video",
        "tabular data": "tabular",
        "time series data": "timeseries",
        "graph data": "graph",
        "geospatial data": "geospatial"
    }
    
    def __init__(self, model_name: str = "facebook/bart-large-mnli"):
        """
        Initialize zero-shot classifier.
        
        Args:
            model_name: HuggingFace model for zero-shot classification
        """
        self.classifier = None
        self.model_name = model_name
        self._initialized = False
    
    def _lazy_load(self):
        """Lazy load model to avoid startup delays."""
        if not self._initialized:
            logger.info(f"Loading zero-shot classifier: {self.model_name}")
            device = 0 if torch.cuda.is_available() else -1
            self.classifier = pipeline(
                "zero-shot-classification",
                model=self.model_name,
                device=device
            )
            self._initialized = True
            logger.info("Classifier loaded successfully")
    
    def classify(
        self,
        dataset_name: str,
        description: str,
        threshold: float = 0.3
    ) -> Dict[str, float]:
        """
        Classify dataset modality using zero-shot learning.
        
        Args:
            dataset_name: Name of the dataset
            description: Dataset description
            threshold: Minimum confidence threshold
        
        Returns:
            Dictionary with modality and confidence scores
        """
        self._lazy_load()
        
        # Combine name and description for better context
        text = f"{dataset_name}. {description}"[:500]  # Truncate for efficiency
        
        # Run zero-shot classification
        result = self.classifier(
            text,
            candidate_labels=self.MODALITY_LABELS,
            multi_label=True  # Allow multiple modalities
        )
        
        # Format results
        scores = {}
        primary_modality = None
        primary_confidence = 0.0
        
        for label, score in zip(result['labels'], result['scores']):
            modality = self.MODALITY_MAP[label]
            scores[modality] = float(score)
            
            if score >= threshold and score > primary_confidence:
                primary_modality = modality
                primary_confidence = score
        
        # Fallback to tabular if uncertain
        if primary_modality is None:
            primary_modality = "tabular"
            primary_confidence = 0.5
        
        return {
            'modality': primary_modality,
            'confidence': primary_confidence,
            'all_scores': scores
        }
    
    def classify_batch(
        self,
        datasets: List[Dict[str, str]],
        threshold: float = 0.3
    ) -> List[Dict[str, float]]:
        """
        Classify multiple datasets in batch (more efficient).
        
        Args:
            datasets: List of dicts with 'name' and 'description' keys
            threshold: Confidence threshold
        
        Returns:
            List of classification results
        """
        self._lazy_load()
        
        results = []
        for dataset in datasets:
            result = self.classify(
                dataset.get('name', ''),
                dataset.get('description', ''),
                threshold
            )
            results.append(result)
        
        return results


class KeywordModalityDetector:
    """
    Fast keyword-based fallback detector.
    Used when ML model is unavailable or for quick initial classification.
    """
    
    KEYWORDS = {
        'image': [
            'image', 'images', 'photo', 'picture', 'visual', 'jpeg', 'jpg', 'png',
            'pixel', 'computer vision', 'cv', 'object detection', 'segmentation',
            'classification', 'facial', 'x-ray', 'mri', 'ct scan', 'satellite'
        ],
        'text': [
            'text', 'nlp', 'natural language', 'corpus', 'document', 'sentiment',
            'language', 'word', 'sentence', 'paragraph', 'article', 'review',
            'translation', 'tokenization', 'embedding'
        ],
        'audio': [
            'audio', 'sound', 'speech', 'music', 'acoustic', 'wav', 'mp3',
            'voice', 'speaker', 'phoneme', 'mel', 'spectrogram'
        ],
        'video': [
            'video', 'mp4', 'avi', 'frame', 'temporal', 'motion', 'action recognition',
            'movie', 'clip'
        ],
        'timeseries': [
            'time series', 'temporal', 'sequential', 'forecast', 'stock price',
            'weather', 'sensor', 'iot', 'time-series', 'timeseries'
        ],
        'graph': [
            'graph', 'network', 'node', 'edge', 'social network', 'citation',
            'knowledge graph', 'ontology', 'relationship'
        ],
        'geospatial': [
            'geospatial', 'gis', 'geographic', 'location', 'gps', 'coordinate',
            'latitude', 'longitude', 'map', 'spatial'
        ]
    }
    
    @staticmethod
    def detect(dataset_name: str, description: str) -> str:
        """
        Quick keyword-based detection.
        
        Args:
            dataset_name: Dataset name
            description: Dataset description
        
        Returns:
            Detected modality (defaults to 'tabular')
        """
        text = f"{dataset_name} {description}".lower()
        
        # Count keyword matches for each modality
        scores = {}
        for modality, keywords in KeywordModalityDetector.KEYWORDS.items():
            score = sum(1 for kw in keywords if kw in text)
            scores[modality] = score
        
        # Return modality with highest score
        if scores:
            best_modality = max(scores, key=scores.get)
            if scores[best_modality] > 0:
                return best_modality
        
        return 'tabular'


# Global instances
ml_classifier = ModalityClassifier()
keyword_detector = KeywordModalityDetector()
