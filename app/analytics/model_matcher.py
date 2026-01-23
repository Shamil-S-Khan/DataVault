"""
Model Architecture Matcher.
Recommends suitable model architectures based on dataset characteristics.
"""
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class ModelRecommendation:
    """A model architecture recommendation."""
    name: str
    architecture: str
    category: str
    description: str
    why_suitable: str
    typical_use: str
    complexity: str  # 'low', 'medium', 'high'
    training_time: str  # 'fast', 'medium', 'slow'
    resources: str  # 'low', 'medium', 'high'
    icon: str  # emoji for UI


# Model database organized by task and modality
MODEL_DATABASE = {
    # Computer Vision Models
    'image_classification': [
        ModelRecommendation(
            name='ResNet-50',
            architecture='Convolutional Neural Network',
            category='Vision',
            description='Deep residual network with skip connections',
            why_suitable='Industry standard for image classification, proven on ImageNet',
            typical_use='General image classification, transfer learning base',
            complexity='medium',
            training_time='medium',
            resources='medium',
            icon='🖼️'
        ),
        ModelRecommendation(
            name='EfficientNet-B4',
            architecture='Convolutional Neural Network',
            category='Vision',
            description='Optimized CNN with compound scaling',
            why_suitable='Better accuracy/efficiency tradeoff than ResNet',
            typical_use='Production classification with resource constraints',
            complexity='medium',
            training_time='medium',
            resources='low',
            icon='⚡'
        ),
        ModelRecommendation(
            name='Vision Transformer (ViT)',
            architecture='Transformer',
            category='Vision',
            description='Transformer architecture for images',
            why_suitable='State-of-the-art accuracy, especially with large datasets',
            typical_use='Large-scale classification, when accuracy is paramount',
            complexity='high',
            training_time='slow',
            resources='high',
            icon='🔮'
        ),
    ],
    'object_detection': [
        ModelRecommendation(
            name='YOLO v8',
            architecture='Single-Stage Detector',
            category='Vision',
            description='Real-time object detection',
            why_suitable='Fast inference, good accuracy, easy to deploy',
            typical_use='Real-time detection, video analysis',
            complexity='medium',
            training_time='medium',
            resources='medium',
            icon='🎯'
        ),
        ModelRecommendation(
            name='Faster R-CNN',
            architecture='Two-Stage Detector',
            category='Vision',
            description='Region-based CNN with proposals',
            why_suitable='High accuracy, better for small objects',
            typical_use='When accuracy matters more than speed',
            complexity='high',
            training_time='slow',
            resources='high',
            icon='🔍'
        ),
    ],
    'image_segmentation': [
        ModelRecommendation(
            name='U-Net',
            architecture='Encoder-Decoder CNN',
            category='Vision',
            description='Skip-connection architecture for segmentation',
            why_suitable='Excellent for medical/satellite imagery',
            typical_use='Medical imaging, semantic segmentation',
            complexity='medium',
            training_time='medium',
            resources='medium',
            icon='🩻'
        ),
        ModelRecommendation(
            name='Mask R-CNN',
            architecture='Instance Segmentation',
            category='Vision',
            description='Extends Faster R-CNN for instance segmentation',
            why_suitable='Instance-level segmentation with detection',
            typical_use='Instance segmentation, panoptic segmentation',
            complexity='high',
            training_time='slow',
            resources='high',
            icon='🎭'
        ),
    ],
    # NLP Models
    'text_classification': [
        ModelRecommendation(
            name='BERT',
            architecture='Transformer Encoder',
            category='NLP',
            description='Bidirectional encoder representations',
            why_suitable='Strong contextual understanding, fine-tuning friendly',
            typical_use='Sentiment analysis, topic classification',
            complexity='medium',
            training_time='medium',
            resources='medium',
            icon='📝'
        ),
        ModelRecommendation(
            name='DistilBERT',
            architecture='Distilled Transformer',
            category='NLP',
            description='Smaller, faster BERT variant',
            why_suitable='60% faster than BERT, retains 97% accuracy',
            typical_use='Production NLP with latency constraints',
            complexity='low',
            training_time='fast',
            resources='low',
            icon='💨'
        ),
        ModelRecommendation(
            name='RoBERTa',
            architecture='Transformer Encoder',
            category='NLP',
            description='Robustly optimized BERT pretraining',
            why_suitable='Often outperforms BERT on classification',
            typical_use='When you need best classification accuracy',
            complexity='medium',
            training_time='medium',
            resources='medium',
            icon='🏆'
        ),
    ],
    'question_answering': [
        ModelRecommendation(
            name='BERT-QA',
            architecture='Transformer Encoder',
            category='NLP',
            description='BERT fine-tuned for extractive QA',
            why_suitable='Strong span extraction for factual questions',
            typical_use='FAQ systems, document QA',
            complexity='medium',
            training_time='medium',
            resources='medium',
            icon='❓'
        ),
        ModelRecommendation(
            name='T5',
            architecture='Encoder-Decoder Transformer',
            category='NLP',
            description='Text-to-text framework',
            why_suitable='Handles generative and extractive QA',
            typical_use='Complex QA, multi-task learning',
            complexity='high',
            training_time='slow',
            resources='high',
            icon='🔄'
        ),
    ],
    'text_generation': [
        ModelRecommendation(
            name='GPT-2',
            architecture='Transformer Decoder',
            category='NLP',
            description='Autoregressive language model',
            why_suitable='Good balance of quality and trainability',
            typical_use='Text completion, creative writing',
            complexity='medium',
            training_time='medium',
            resources='medium',
            icon='✍️'
        ),
        ModelRecommendation(
            name='LLaMA 2',
            architecture='Transformer Decoder',
            category='NLP',
            description='Meta open-source LLM',
            why_suitable='Open weights, competitive with GPT-3.5',
            typical_use='Chatbots, instruction following',
            complexity='high',
            training_time='slow',
            resources='high',
            icon='🦙'
        ),
    ],
    'named_entity_recognition': [
        ModelRecommendation(
            name='BERT-NER',
            architecture='Transformer + Token Classification',
            category='NLP',
            description='BERT with token classification head',
            why_suitable='Strong contextual entity recognition',
            typical_use='Information extraction, entity tagging',
            complexity='medium',
            training_time='medium',
            resources='medium',
            icon='🏷️'
        ),
    ],
    # Audio Models
    'speech_recognition': [
        ModelRecommendation(
            name='Whisper',
            architecture='Encoder-Decoder Transformer',
            category='Audio',
            description='OpenAI multilingual ASR',
            why_suitable='Robust, multilingual, handles noise well',
            typical_use='Transcription, translation',
            complexity='medium',
            training_time='medium',
            resources='medium',
            icon='🎤'
        ),
        ModelRecommendation(
            name='Wav2Vec 2.0',
            architecture='Self-Supervised Transformer',
            category='Audio',
            description='Self-supervised speech representations',
            why_suitable='Excellent for low-resource languages',
            typical_use='Speech recognition with limited data',
            complexity='high',
            training_time='slow',
            resources='high',
            icon='📻'
        ),
    ],
    'audio_classification': [
        ModelRecommendation(
            name='Audio Spectrogram Transformer',
            architecture='Vision Transformer',
            category='Audio',
            description='ViT applied to spectrograms',
            why_suitable='Leverages vision transformer success for audio',
            typical_use='Sound classification, music tagging',
            complexity='medium',
            training_time='medium',
            resources='medium',
            icon='🔊'
        ),
    ],
    # Tabular Models
    'tabular_classification': [
        ModelRecommendation(
            name='XGBoost',
            architecture='Gradient Boosted Trees',
            category='Tabular',
            description='Optimized gradient boosting',
            why_suitable='Best for structured data, handles missing values',
            typical_use='Classification on tabular data',
            complexity='low',
            training_time='fast',
            resources='low',
            icon='🌳'
        ),
        ModelRecommendation(
            name='LightGBM',
            architecture='Gradient Boosted Trees',
            category='Tabular',
            description='Microsoft optimized gradient boosting',
            why_suitable='Faster than XGBoost, good for large datasets',
            typical_use='Large-scale tabular classification',
            complexity='low',
            training_time='fast',
            resources='low',
            icon='⚡'
        ),
        ModelRecommendation(
            name='TabNet',
            architecture='Attentive Neural Network',
            category='Tabular',
            description='Attention-based tabular learning',
            why_suitable='Interpretable, competitive with boosting',
            typical_use='When interpretability matters',
            complexity='medium',
            training_time='medium',
            resources='medium',
            icon='🧠'
        ),
    ],
    'tabular_regression': [
        ModelRecommendation(
            name='XGBoost Regressor',
            architecture='Gradient Boosted Trees',
            category='Tabular',
            description='XGBoost for regression tasks',
            why_suitable='Handles non-linear relationships well',
            typical_use='Price prediction, value estimation',
            complexity='low',
            training_time='fast',
            resources='low',
            icon='📈'
        ),
        ModelRecommendation(
            name='CatBoost',
            architecture='Gradient Boosted Trees',
            category='Tabular',
            description='Yandex categorical boosting',
            why_suitable='Excellent handling of categorical features',
            typical_use='Mixed numeric/categorical data',
            complexity='low',
            training_time='fast',
            resources='low',
            icon='🐱'
        ),
    ],
}


class ModelMatcher:
    """Match datasets to suitable model architectures."""
    
    # Task normalization mapping
    TASK_MAPPING = {
        # Image tasks
        'image-classification': 'image_classification',
        'image classification': 'image_classification',
        'classification': 'image_classification',  # with image modality
        'object-detection': 'object_detection',
        'object detection': 'object_detection',
        'detection': 'object_detection',
        'image-segmentation': 'image_segmentation',
        'image segmentation': 'image_segmentation',
        'segmentation': 'image_segmentation',
        'semantic-segmentation': 'image_segmentation',
        
        # NLP tasks
        'text-classification': 'text_classification',
        'text classification': 'text_classification',
        'sentiment-analysis': 'text_classification',
        'sentiment analysis': 'text_classification',
        'question-answering': 'question_answering',
        'question answering': 'question_answering',
        'qa': 'question_answering',
        'text-generation': 'text_generation',
        'text generation': 'text_generation',
        'language-modeling': 'text_generation',
        'named-entity-recognition': 'named_entity_recognition',
        'ner': 'named_entity_recognition',
        'token-classification': 'named_entity_recognition',
        
        # Audio tasks
        'automatic-speech-recognition': 'speech_recognition',
        'speech-recognition': 'speech_recognition',
        'asr': 'speech_recognition',
        'audio-classification': 'audio_classification',
        'audio classification': 'audio_classification',
        
        # Tabular tasks
        'tabular-classification': 'tabular_classification',
        'tabular classification': 'tabular_classification',
        'tabular-regression': 'tabular_regression',
        'tabular regression': 'tabular_regression',
        'regression': 'tabular_regression',  # with tabular modality
    }
    
    # Modality to default task mapping
    MODALITY_DEFAULTS = {
        'image': 'image_classification',
        'text': 'text_classification',
        'audio': 'speech_recognition',
        'tabular': 'tabular_classification',
        'video': 'image_classification',
    }
    
    def get_recommendations(
        self, 
        dataset: Dict[str, Any],
        limit: int = 5
    ) -> Dict[str, Any]:
        """
        Get model recommendations for a dataset.
        
        Args:
            dataset: Dataset document from MongoDB
            limit: Maximum number of recommendations
            
        Returns:
            Dictionary with recommendations and reasoning
        """
        # Extract dataset info
        intelligence = dataset.get('intelligence', {})
        tasks = intelligence.get('tasks', [])
        modality = (dataset.get('modality') or '').lower()
        domain = (dataset.get('domain') or '').lower()
        size = dataset.get('size', {}).get('samples')
        
        # Determine the primary task
        normalized_task = None
        original_task = None
        
        # Try to match from intelligence tasks
        for task in tasks:
            task_lower = task.lower()
            if task_lower in self.TASK_MAPPING:
                normalized_task = self.TASK_MAPPING[task_lower]
                original_task = task
                break
        
        # Fall back to modality-based default
        if not normalized_task and modality in self.MODALITY_DEFAULTS:
            normalized_task = self.MODALITY_DEFAULTS[modality]
        
        # Get recommendations
        recommendations = []
        if normalized_task and normalized_task in MODEL_DATABASE:
            recommendations = MODEL_DATABASE[normalized_task][:limit]
        
        # If no specific recommendations, suggest general models based on modality
        if not recommendations:
            if modality == 'image':
                recommendations = MODEL_DATABASE['image_classification'][:limit]
            elif modality in ['text', 'nlp']:
                recommendations = MODEL_DATABASE['text_classification'][:limit]
            elif modality == 'audio':
                recommendations = MODEL_DATABASE['speech_recognition'][:limit]
            else:
                recommendations = MODEL_DATABASE['tabular_classification'][:limit]
        
        # Adjust recommendations based on dataset size
        size_notes = self._get_size_guidance(size)
        
        # Format recommendations
        formatted = []
        for rec in recommendations:
            formatted.append({
                'name': rec.name,
                'architecture': rec.architecture,
                'category': rec.category,
                'description': rec.description,
                'why_suitable': rec.why_suitable,
                'typical_use': rec.typical_use,
                'complexity': rec.complexity,
                'training_time': rec.training_time,
                'resources': rec.resources,
                'icon': rec.icon
            })
        
        return {
            'detected_task': original_task or normalized_task,
            'normalized_task': normalized_task,
            'modality': modality or 'unknown',
            'recommendations': formatted,
            'size_guidance': size_notes,
            'count': len(formatted)
        }
    
    def _get_size_guidance(self, size: Optional[int]) -> str:
        """Get guidance based on dataset size."""
        if not size:
            return "Unknown dataset size. Model recommendations are general."
        
        if size < 1000:
            return "Small dataset (<1K samples). Consider transfer learning, data augmentation, or simpler models to avoid overfitting."
        elif size < 10000:
            return "Medium dataset (1K-10K samples). Most models should work well with proper regularization."
        elif size < 100000:
            return "Large dataset (10K-100K samples). You can train larger models effectively."
        else:
            return "Very large dataset (>100K samples). Deep models like transformers will benefit from this scale."


# Singleton instance
model_matcher = ModelMatcher()
