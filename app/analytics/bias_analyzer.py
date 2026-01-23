"""
Bias & Fairness Analyzer.
Analyzes datasets for potential bias and class imbalance issues.
"""
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import logging
import math

logger = logging.getLogger(__name__)


@dataclass
class BiasWarning:
    """A bias/fairness warning."""
    severity: str  # 'low', 'medium', 'high'
    category: str  # 'class_imbalance', 'representation', 'data_quality'
    title: str
    description: str
    recommendation: str


class BiasAnalyzer:
    """
    Analyze datasets for potential bias and fairness issues.
    
    Checks for:
    1. Class imbalance (if label distribution available)
    2. Sample size concerns
    3. Common bias patterns based on domain/task
    4. Representation warnings
    """
    
    # Domain-specific bias considerations
    DOMAIN_BIAS_CONSIDERATIONS = {
        'computer vision': [
            {
                'title': 'Demographic Representation',
                'description': 'Vision datasets may underrepresent certain demographics, skin tones, or geographic regions.',
                'recommendation': 'Evaluate performance across demographic groups before deployment.',
                'severity': 'medium'
            },
            {
                'title': 'Lighting & Environment Bias',
                'description': 'Training data may not represent all lighting conditions or environments.',
                'recommendation': 'Test model on diverse real-world conditions.',
                'severity': 'low'
            }
        ],
        'nlp': [
            {
                'title': 'Language Representation',
                'description': 'Text datasets often over-represent certain dialects, regions, or demographics.',
                'recommendation': 'Ensure training data includes diverse linguistic patterns.',
                'severity': 'medium'
            },
            {
                'title': 'Gender & Identity Bias',
                'description': 'NLP models can inherit societal biases from training text.',
                'recommendation': 'Use debiasing techniques and evaluate for harmful stereotypes.',
                'severity': 'high'
            }
        ],
        'medical': [
            {
                'title': 'Patient Population Bias',
                'description': 'Medical datasets may not represent all patient demographics equally.',
                'recommendation': 'Validate on diverse patient populations before clinical use.',
                'severity': 'high'
            },
            {
                'title': 'Geographic Healthcare Bias',
                'description': 'Data may reflect healthcare systems of specific regions.',
                'recommendation': 'Consider local validation for different healthcare contexts.',
                'severity': 'medium'
            }
        ],
        'audio': [
            {
                'title': 'Accent & Language Bias',
                'description': 'Speech datasets often underrepresent accents and non-native speakers.',
                'recommendation': 'Test recognition accuracy across accent groups.',
                'severity': 'medium'
            }
        ],
        'general': [
            {
                'title': 'Temporal Bias',
                'description': 'Data collected at a specific time may not represent current patterns.',
                'recommendation': 'Consider how patterns may have changed since data collection.',
                'severity': 'low'
            }
        ]
    }
    
    def analyze(self, dataset: Dict[str, Any]) -> Dict[str, Any]:
        """
        Perform comprehensive bias analysis on a dataset.
        
        Args:
            dataset: Dataset document from MongoDB
            
        Returns:
            Bias analysis results with warnings and recommendations
        """
        warnings = []
        metrics = {}
        
        # 1. Analyze size concerns
        size_warnings = self._analyze_size(dataset)
        warnings.extend(size_warnings)
        
        # 2. Domain-specific bias considerations
        domain_warnings = self._analyze_domain_bias(dataset)
        warnings.extend(domain_warnings)
        
        # 3. Task-specific considerations
        task_warnings = self._analyze_task_bias(dataset)
        warnings.extend(task_warnings)
        
        # 4. Calculate overall bias risk score
        risk_score = self._calculate_risk_score(warnings)
        
        # 5. Generate summary
        summary = self._generate_summary(warnings, risk_score, dataset)
        
        return {
            'risk_score': risk_score,
            'risk_level': self._get_risk_level(risk_score),
            'warnings': [
                {
                    'severity': w.severity,
                    'category': w.category,
                    'title': w.title,
                    'description': w.description,
                    'recommendation': w.recommendation
                }
                for w in warnings
            ],
            'warning_count': {
                'high': sum(1 for w in warnings if w.severity == 'high'),
                'medium': sum(1 for w in warnings if w.severity == 'medium'),
                'low': sum(1 for w in warnings if w.severity == 'low')
            },
            'summary': summary,
            'analyzed_aspects': ['size', 'domain', 'task', 'representation']
        }
    
    def _analyze_size(self, dataset: Dict[str, Any]) -> List[BiasWarning]:
        """Analyze dataset size for bias implications."""
        warnings = []
        
        size = dataset.get('size', {}).get('samples')
        metadata = dataset.get('source', {}).get('source_metadata', {})
        
        if not size:
            # Try to infer from downloads as proxy
            downloads = metadata.get('downloads', 0) or 0
            size = metadata.get('num_examples')
        
        if size and size < 1000:
            warnings.append(BiasWarning(
                severity='high',
                category='data_quality',
                title='Very Small Dataset',
                description=f'Only {size} samples. Small datasets often fail to capture population diversity.',
                recommendation='Consider data augmentation or finding additional data sources.'
            ))
        elif size and size < 10000:
            warnings.append(BiasWarning(
                severity='medium',
                category='data_quality',
                title='Limited Sample Size',
                description=f'Dataset has {size} samples, which may not fully represent population variance.',
                recommendation='Validate model carefully on held-out data with demographic splits.'
            ))
        
        return warnings
    
    def _analyze_domain_bias(self, dataset: Dict[str, Any]) -> List[BiasWarning]:
        """Analyze domain-specific bias considerations."""
        warnings = []
        
        domain = (dataset.get('domain') or '').lower()
        modality = (dataset.get('modality') or '').lower()
        
        # Map to our consideration categories
        domain_key = None
        if 'vision' in domain or 'image' in modality:
            domain_key = 'computer vision'
        elif 'nlp' in domain or 'text' in modality or 'language' in domain:
            domain_key = 'nlp'
        elif 'medical' in domain or 'health' in domain:
            domain_key = 'medical'
        elif 'audio' in modality or 'speech' in domain:
            domain_key = 'audio'
        else:
            domain_key = 'general'
        
        considerations = self.DOMAIN_BIAS_CONSIDERATIONS.get(domain_key, [])
        
        for c in considerations:
            warnings.append(BiasWarning(
                severity=c['severity'],
                category='representation',
                title=c['title'],
                description=c['description'],
                recommendation=c['recommendation']
            ))
        
        return warnings
    
    def _analyze_task_bias(self, dataset: Dict[str, Any]) -> List[BiasWarning]:
        """Analyze task-specific bias considerations."""
        warnings = []
        
        intelligence = dataset.get('intelligence', {})
        tasks = intelligence.get('tasks', [])
        
        task_str = ' '.join(tasks).lower() if tasks else ''
        
        # Classification tasks
        if 'classification' in task_str:
            warnings.append(BiasWarning(
                severity='medium',
                category='class_imbalance',
                title='Classification Task',
                description='Classification tasks can amplify biases in underrepresented classes.',
                recommendation='Check class distribution and consider stratified sampling or class weights.'
            ))
        
        # Face/person detection
        if any(word in task_str for word in ['face', 'person', 'human', 'facial']):
            warnings.append(BiasWarning(
                severity='high',
                category='representation',
                title='Human-Centric Task',
                description='Tasks involving human recognition have documented demographic bias risks.',
                recommendation='Perform thorough fairness testing across demographic groups.'
            ))
        
        # Sentiment analysis
        if 'sentiment' in task_str:
            warnings.append(BiasWarning(
                severity='medium',
                category='representation',
                title='Sentiment Analysis',
                description='Sentiment models may perform differently across cultural expressions.',
                recommendation='Validate across different cultural and demographic contexts.'
            ))
        
        return warnings
    
    def _calculate_risk_score(self, warnings: List[BiasWarning]) -> float:
        """Calculate overall bias risk score (0-100)."""
        if not warnings:
            return 10.0  # Base risk for unknown
        
        # Weight by severity
        weights = {'high': 30, 'medium': 15, 'low': 5}
        
        total_score = sum(weights.get(w.severity, 5) for w in warnings)
        
        # Cap at 100
        return min(100.0, total_score)
    
    def _get_risk_level(self, score: float) -> str:
        """Convert score to risk level."""
        if score >= 70:
            return 'high'
        elif score >= 40:
            return 'medium'
        else:
            return 'low'
    
    def _generate_summary(
        self, 
        warnings: List[BiasWarning], 
        risk_score: float,
        dataset: Dict[str, Any]
    ) -> str:
        """Generate human-readable summary."""
        name = dataset.get('canonical_name', 'This dataset')
        risk_level = self._get_risk_level(risk_score)
        
        high_count = sum(1 for w in warnings if w.severity == 'high')
        medium_count = sum(1 for w in warnings if w.severity == 'medium')
        
        if risk_level == 'high':
            summary = f"{name} has significant bias considerations that require attention before deployment. "
        elif risk_level == 'medium':
            summary = f"{name} has moderate bias risks that should be evaluated. "
        else:
            summary = f"{name} has low identified bias risks, but standard fairness practices should still apply. "
        
        if high_count > 0:
            summary += f"There are {high_count} high-priority concerns to address. "
        
        return summary.strip()


# Singleton instance
bias_analyzer = BiasAnalyzer()
