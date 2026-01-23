"""
Tier-3 Synthetic Data Suitability Analyzer.

A research-grade benefit-vs-risk decision system that answers:
"Should synthetic data be used for this dataset, and is it safe?"

Architecture:
    Final Score = Base + Benefit Factors − Risk Factors

Features:
- Class imbalance detection
- Minority class analysis  
- Domain safety layer (medical, biometric, legal, finance)
- Synthetic-on-synthetic risk detection
- Annotation uncertainty flags
- Human-readable explanations
"""
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import logging
import re

logger = logging.getLogger(__name__)


class RiskLevel(Enum):
    """Risk levels for synthetic data usage."""
    SAFE = "safe"
    CAUTION = "caution"
    HIGH_RISK = "high_risk"
    PROHIBITED = "prohibited"


@dataclass
class BenefitFactor:
    """A factor that increases synthetic data benefit."""
    name: str
    points: float
    explanation: str
    icon: str = "✔"


@dataclass
class RiskFactor:
    """A factor that decreases synthetic data suitability."""
    name: str
    points: float
    explanation: str
    severity: str = "warning"  # 'info', 'warning', 'critical'
    icon: str = "⚠"
    requires_human_validation: bool = False


@dataclass
class SyntheticDecision:
    """Complete synthetic data decision output."""
    score: float
    verdict: str
    risk_level: RiskLevel
    benefits: List[BenefitFactor]
    risks: List[RiskFactor]
    recommendations: List[Dict[str, Any]]
    explanation: str
    requires_human_validation: bool
    caution_flags: List[str]


class Tier3SyntheticAnalyzer:
    """
    Research-grade Synthetic Data Suitability Analyzer.
    
    Uses benefit-vs-risk scoring with explainability.
    
    Score Formula:
        BASE_SCORE = 50
        Final = clamp(BASE + sum(benefits) - sum(risks), 0, 100)
    """
    
    BASE_SCORE = 50.0
    
    # High-risk domains where synthetic data requires extra caution
    HIGH_RISK_DOMAINS = {
        'medical': {'penalty': 20, 'requires_validation': True},
        'healthcare': {'penalty': 20, 'requires_validation': True},
        'clinical': {'penalty': 25, 'requires_validation': True},
        'radiology': {'penalty': 25, 'requires_validation': True},
        'pathology': {'penalty': 25, 'requires_validation': True},
        'biometric': {'penalty': 20, 'requires_validation': True},
        'facial_recognition': {'penalty': 15, 'requires_validation': True},
        'face recognition': {'penalty': 15, 'requires_validation': True},
        'face detection': {'penalty': 15, 'requires_validation': True},
        'fingerprint': {'penalty': 20, 'requires_validation': True},
        'legal': {'penalty': 15, 'requires_validation': True},
        'law': {'penalty': 15, 'requires_validation': True},
        'finance': {'penalty': 15, 'requires_validation': True},
        'financial': {'penalty': 15, 'requires_validation': True},
        'fraud': {'penalty': 15, 'requires_validation': True},
        'credit': {'penalty': 15, 'requires_validation': True},
        'autonomous': {'penalty': 20, 'requires_validation': True},
        'self-driving': {'penalty': 25, 'requires_validation': True},
        'safety-critical': {'penalty': 25, 'requires_validation': True},
    }
    
    # Keywords indicating synthetic/generated data
    SYNTHETIC_INDICATORS = [
        'synthetic', 'generated', 'artificial', 'simulated',
        'gan', 'diffusion', 'augmented', 'fake', 'deepfake'
    ]
    
    # Thresholds for class imbalance
    IMBALANCE_THRESHOLDS = {
        'severe': 10.0,    # max/min > 10:1
        'moderate': 5.0,   # max/min > 5:1  
        'mild': 2.0,       # max/min > 2:1
    }
    
    # Minority class thresholds
    MINORITY_THRESHOLDS = {
        'critical': 50,    # < 50 samples
        'severe': 100,     # < 100 samples
        'low': 500,        # < 500 samples
    }
    
    # Size-based benefit points
    SIZE_BENEFITS = {
        'critical': 30,   # < 500 samples
        'tiny': 25,       # < 1000 samples
        'small': 18,      # < 10000 samples
        'medium': 8,      # < 100000 samples
        'large': 0,       # >= 100000 samples
    }
    
    def analyze(self, dataset: Dict[str, Any]) -> Dict[str, Any]:
        """
        Perform comprehensive synthetic data suitability analysis.
        
        Returns a complete decision with benefits, risks, and explanations.
        """
        # Extract metadata
        name = dataset.get('canonical_name', 'Unknown Dataset')
        description = (dataset.get('description') or '').lower()
        domain = (dataset.get('domain') or '').lower()
        modality = (dataset.get('modality') or '').lower()
        task = (dataset.get('task') or '').lower()
        size = dataset.get('size', {}).get('samples') or 0
        
        # Extract class distribution if available
        class_distribution = self._extract_class_distribution(dataset)
        
        # Calculate benefits and risks
        benefits = self._calculate_benefits(
            size=size,
            modality=modality,
            task=task,
            class_distribution=class_distribution,
            description=description
        )
        
        risks = self._calculate_risks(
            domain=domain,
            description=description,
            name=name,
            class_distribution=class_distribution,
            size=size
        )
        
        # Calculate final score
        benefit_points = sum(b.points for b in benefits)
        risk_points = sum(r.points for r in risks)
        
        raw_score = self.BASE_SCORE + benefit_points - risk_points
        final_score = max(0.0, min(100.0, raw_score))
        
        # Determine verdict and risk level
        requires_human_validation = any(r.requires_human_validation for r in risks)
        risk_level = self._determine_risk_level(risks, final_score)
        verdict = self._generate_verdict(final_score, risk_level, requires_human_validation)
        
        # Generate caution flags
        caution_flags = self._generate_caution_flags(risks)
        
        # Get technique recommendations
        data_type = self._determine_data_type(modality, domain)
        recommendations = self._get_recommendations(data_type, size, class_distribution)
        
        # Generate comprehensive explanation
        explanation = self._generate_explanation(
            benefits=benefits,
            risks=risks,
            final_score=final_score,
            risk_level=risk_level,
            data_type=data_type
        )
        
        decision = SyntheticDecision(
            score=final_score,
            verdict=verdict,
            risk_level=risk_level,
            benefits=benefits,
            risks=risks,
            recommendations=recommendations,
            explanation=explanation,
            requires_human_validation=requires_human_validation,
            caution_flags=caution_flags
        )
        
        return self._decision_to_dict(decision, benefit_points, risk_points, data_type)
    
    def _extract_class_distribution(self, dataset: Dict[str, Any]) -> Optional[Dict[str, int]]:
        """
        Extract class distribution from dataset metadata if available.
        
        Looks in:
        - dataset['class_distribution']
        - dataset['label_counts']
        - dataset['statistics']['classes']
        """
        # Try direct class distribution
        if 'class_distribution' in dataset:
            return dataset['class_distribution']
        
        # Try label counts
        if 'label_counts' in dataset:
            return dataset['label_counts']
        
        # Try statistics
        stats = dataset.get('statistics', {})
        if 'classes' in stats:
            return stats['classes']
        
        # Try to infer from features/labels
        features = dataset.get('size', {}).get('features')
        if features and isinstance(features, dict):
            if 'num_classes' in features:
                # We know class count but not distribution
                return {'_num_classes': features['num_classes']}
        
        return None
    
    def _calculate_benefits(
        self,
        size: int,
        modality: str,
        task: str,
        class_distribution: Optional[Dict[str, int]],
        description: str
    ) -> List[BenefitFactor]:
        """Calculate all benefit factors for synthetic data."""
        benefits = []
        
        # 1. Size-based benefit (smaller = more benefit)
        if size > 0:
            if size < 500:
                benefits.append(BenefitFactor(
                    name="Critical Data Scarcity",
                    points=30,
                    explanation=f"Dataset has only {size:,} samples — synthetic augmentation is critical",
                    icon="🚨"
                ))
            elif size < 1000:
                benefits.append(BenefitFactor(
                    name="Severely Limited Dataset",
                    points=25,
                    explanation=f"Dataset has only {size:,} samples — strong candidate for augmentation",
                    icon="⚡"
                ))
            elif size < 10000:
                benefits.append(BenefitFactor(
                    name="Small Dataset",
                    points=18,
                    explanation=f"Dataset has {size:,} samples — augmentation recommended",
                    icon="✔"
                ))
            elif size < 100000:
                benefits.append(BenefitFactor(
                    name="Moderate Dataset Size",
                    points=8,
                    explanation=f"Dataset has {size:,} samples — selective augmentation may help",
                    icon="✔"
                ))
        else:
            # Unknown size - give moderate benefit
            benefits.append(BenefitFactor(
                name="Unknown Dataset Size",
                points=10,
                explanation="Dataset size unknown — augmentation may be beneficial",
                icon="❓"
            ))
        
        # 2. Class imbalance detection
        if class_distribution and len(class_distribution) > 1:
            imbalance_info = self._analyze_imbalance(class_distribution)
            
            if imbalance_info['ratio'] >= self.IMBALANCE_THRESHOLDS['severe']:
                benefits.append(BenefitFactor(
                    name="Severe Class Imbalance",
                    points=25,
                    explanation=f"Class imbalance ratio is {imbalance_info['ratio']:.1f}:1 — synthetic minority oversampling strongly recommended",
                    icon="⚖️"
                ))
            elif imbalance_info['ratio'] >= self.IMBALANCE_THRESHOLDS['moderate']:
                benefits.append(BenefitFactor(
                    name="Moderate Class Imbalance",
                    points=15,
                    explanation=f"Class imbalance ratio is {imbalance_info['ratio']:.1f}:1 — consider targeted augmentation",
                    icon="⚖️"
                ))
            elif imbalance_info['ratio'] >= self.IMBALANCE_THRESHOLDS['mild']:
                benefits.append(BenefitFactor(
                    name="Mild Class Imbalance",
                    points=8,
                    explanation=f"Class imbalance ratio is {imbalance_info['ratio']:.1f}:1 — augmentation may improve minority class performance",
                    icon="⚖️"
                ))
        
        # 3. Minority class sample count
        if class_distribution:
            minority_info = self._analyze_minority_classes(class_distribution)
            
            if minority_info['min_count'] > 0:
                if minority_info['min_count'] < self.MINORITY_THRESHOLDS['critical']:
                    benefits.append(BenefitFactor(
                        name="Critical Minority Class",
                        points=20,
                        explanation=f"Minority class has only {minority_info['min_count']} samples — synthetic generation essential",
                        icon="🔴"
                    ))
                elif minority_info['min_count'] < self.MINORITY_THRESHOLDS['severe']:
                    benefits.append(BenefitFactor(
                        name="Underrepresented Minority Class",
                        points=15,
                        explanation=f"Minority class has only {minority_info['min_count']} samples — augmentation recommended",
                        icon="🟠"
                    ))
                elif minority_info['min_count'] < self.MINORITY_THRESHOLDS['low']:
                    benefits.append(BenefitFactor(
                        name="Low Minority Class Samples",
                        points=8,
                        explanation=f"Minority class has {minority_info['min_count']} samples — targeted augmentation may help",
                        icon="🟡"
                    ))
        
        # 4. Task-based benefits
        if 'classification' in task and class_distribution:
            benefits.append(BenefitFactor(
                name="Classification Task",
                points=5,
                explanation="Classification tasks benefit from balanced class representation",
                icon="🎯"
            ))
        
        if 'detection' in task:
            benefits.append(BenefitFactor(
                name="Detection Task",
                points=8,
                explanation="Object detection benefits significantly from data augmentation",
                icon="🔍"
            ))
        
        if 'segmentation' in task:
            benefits.append(BenefitFactor(
                name="Segmentation Task",
                points=7,
                explanation="Semantic segmentation benefits from augmented training data",
                icon="🖼️"
            ))
        
        # 5. Modality-based benefits
        if 'image' in modality or 'vision' in modality:
            benefits.append(BenefitFactor(
                name="Visual Data Modality",
                points=8,
                explanation="Image data has well-established augmentation techniques",
                icon="📷"
            ))
        elif 'audio' in modality or 'speech' in modality:
            benefits.append(BenefitFactor(
                name="Audio Data Modality",
                points=6,
                explanation="Audio data benefits from time-stretch and noise augmentation",
                icon="🔊"
            ))
        elif 'text' in modality:
            benefits.append(BenefitFactor(
                name="Text Data Modality",
                points=5,
                explanation="Text data can use back-translation and paraphrasing",
                icon="📝"
            ))
        
        return benefits
    
    def _calculate_risks(
        self,
        domain: str,
        description: str,
        name: str,
        class_distribution: Optional[Dict[str, int]],
        size: int
    ) -> List[RiskFactor]:
        """Calculate all risk factors for synthetic data."""
        risks = []
        combined_text = f"{domain} {description} {name}".lower()
        
        # 1. High-risk domain detection
        for risk_domain, config in self.HIGH_RISK_DOMAINS.items():
            if risk_domain in combined_text:
                risks.append(RiskFactor(
                    name=f"High-Risk Domain: {risk_domain.title()}",
                    points=config['penalty'],
                    explanation=f"Synthetic data in {risk_domain} domain requires expert validation",
                    severity="critical",
                    icon="🚨",
                    requires_human_validation=config['requires_validation']
                ))
                break  # Only add one domain risk
        
        # 2. Synthetic-on-synthetic risk
        is_synthetic = any(ind in combined_text for ind in self.SYNTHETIC_INDICATORS)
        if is_synthetic:
            risks.append(RiskFactor(
                name="Synthetic-on-Synthetic Risk",
                points=25,
                explanation="Dataset may already contain synthetic data — adding more risks compounding artifacts",
                severity="critical",
                icon="🔄",
                requires_human_validation=True
            ))
        
        # 3. Well-balanced dataset (reduces need for synthetic)
        if class_distribution and len(class_distribution) > 1:
            imbalance_info = self._analyze_imbalance(class_distribution)
            if imbalance_info['ratio'] < 1.5:
                risks.append(RiskFactor(
                    name="Already Well-Balanced",
                    points=15,
                    explanation=f"Class ratio is only {imbalance_info['ratio']:.2f}:1 — synthetic augmentation may not be necessary",
                    severity="info",
                    icon="✅"
                ))
        
        # 4. Large dataset (less need for synthetic)
        if size >= 1000000:
            risks.append(RiskFactor(
                name="Large Dataset Available",
                points=20,
                explanation=f"Dataset has {size:,} samples — synthetic data likely unnecessary",
                severity="info",
                icon="📊"
            ))
        elif size >= 100000:
            risks.append(RiskFactor(
                name="Substantial Dataset Size",
                points=10,
                explanation=f"Dataset has {size:,} samples — synthetic data may only provide marginal benefit",
                severity="info",
                icon="📊"
            ))
        
        # 5. Label noise indicators
        noise_indicators = ['noisy', 'crowdsource', 'weak label', 'uncertain', 'annotation quality']
        for indicator in noise_indicators:
            if indicator in combined_text:
                risks.append(RiskFactor(
                    name="Potential Label Noise",
                    points=12,
                    explanation="Dataset may have annotation uncertainty — synthetic data could amplify noise",
                    severity="warning",
                    icon="🔊",
                    requires_human_validation=True
                ))
                break
        
        # 6. Privacy-sensitive data
        privacy_indicators = ['personal', 'private', 'pii', 'phi', 'hipaa', 'gdpr']
        for indicator in privacy_indicators:
            if indicator in combined_text:
                risks.append(RiskFactor(
                    name="Privacy-Sensitive Data",
                    points=15,
                    explanation="Dataset contains privacy-sensitive information — synthetic generation may leak private data",
                    severity="critical",
                    icon="🔒",
                    requires_human_validation=True
                ))
                break
        
        return risks
    
    def _analyze_imbalance(self, class_distribution: Dict[str, int]) -> Dict[str, Any]:
        """Analyze class imbalance from distribution."""
        if not class_distribution or '_num_classes' in class_distribution:
            return {'ratio': 1.0, 'balanced': True}
        
        counts = list(class_distribution.values())
        if not counts or len(counts) < 2:
            return {'ratio': 1.0, 'balanced': True}
        
        max_count = max(counts)
        min_count = min(c for c in counts if c > 0) if any(c > 0 for c in counts) else 1
        
        ratio = max_count / min_count if min_count > 0 else float('inf')
        
        return {
            'ratio': ratio,
            'max_count': max_count,
            'min_count': min_count,
            'balanced': ratio < 2.0,
            'num_classes': len(counts)
        }
    
    def _analyze_minority_classes(self, class_distribution: Dict[str, int]) -> Dict[str, Any]:
        """Analyze minority class statistics."""
        if not class_distribution or '_num_classes' in class_distribution:
            return {'min_count': 0, 'num_minority': 0}
        
        counts = list(class_distribution.values())
        if not counts:
            return {'min_count': 0, 'num_minority': 0}
        
        min_count = min(counts)
        median_count = sorted(counts)[len(counts) // 2]
        
        # Count classes with fewer than 20% of median
        threshold = median_count * 0.2
        num_minority = sum(1 for c in counts if c < threshold)
        
        return {
            'min_count': min_count,
            'num_minority': num_minority,
            'median_count': median_count
        }
    
    def _determine_risk_level(self, risks: List[RiskFactor], score: float) -> RiskLevel:
        """Determine overall risk level."""
        critical_risks = sum(1 for r in risks if r.severity == "critical")
        warning_risks = sum(1 for r in risks if r.severity == "warning")
        
        if critical_risks >= 2 or score < 20:
            return RiskLevel.HIGH_RISK
        elif critical_risks >= 1 or warning_risks >= 2:
            return RiskLevel.CAUTION
        elif warning_risks >= 1 or score < 40:
            return RiskLevel.CAUTION
        else:
            return RiskLevel.SAFE
    
    def _generate_verdict(
        self, 
        score: float, 
        risk_level: RiskLevel,
        requires_human_validation: bool
    ) -> str:
        """Generate human-readable verdict."""
        if risk_level == RiskLevel.HIGH_RISK:
            return "Not Recommended — High Risk"
        elif requires_human_validation:
            if score >= 60:
                return "Recommended with Expert Validation"
            else:
                return "Use with Caution — Expert Review Required"
        elif risk_level == RiskLevel.CAUTION:
            if score >= 50:
                return "Potentially Beneficial — Review Risks"
            else:
                return "Limited Benefit — Consider Alternatives"
        else:
            if score >= 75:
                return "Highly Recommended"
            elif score >= 50:
                return "Recommended"
            elif score >= 30:
                return "Optional — Marginal Benefit"
            else:
                return "Not Recommended — Limited Value"
    
    def _generate_caution_flags(self, risks: List[RiskFactor]) -> List[str]:
        """Generate caution flag messages."""
        flags = []
        for risk in risks:
            if risk.severity in ("critical", "warning"):
                flags.append(f"{risk.icon} {risk.explanation}")
        return flags
    
    def _determine_data_type(self, modality: str, domain: str) -> str:
        """Determine primary data type."""
        if 'image' in modality or 'vision' in domain:
            return 'image'
        elif 'text' in modality or 'nlp' in domain or 'language' in domain:
            return 'text'
        elif 'audio' in modality or 'speech' in domain:
            return 'audio'
        elif 'tabular' in modality or 'structured' in modality:
            return 'tabular'
        else:
            return 'general'
    
    def _get_recommendations(
        self, 
        data_type: str, 
        size: int,
        class_distribution: Optional[Dict[str, int]]
    ) -> List[Dict[str, Any]]:
        """Get context-aware augmentation recommendations."""
        recommendations = []
        
        # Priority recommendations based on class imbalance
        if class_distribution:
            imbalance = self._analyze_imbalance(class_distribution)
            if imbalance['ratio'] > 2:
                recommendations.append({
                    'technique': 'SMOTE / Minority Oversampling',
                    'priority': 'critical',
                    'reason': f"Address {imbalance['ratio']:.1f}:1 class imbalance",
                    'impact': 'High'
                })
        
        # Modality-specific recommendations
        if data_type == 'image':
            recommendations.extend([
                {'technique': 'Geometric Transforms', 'priority': 'high', 'reason': 'Rotation, flip, crop', 'impact': '10-20%'},
                {'technique': 'Color Augmentation', 'priority': 'medium', 'reason': 'Jitter, normalize', 'impact': '5-15%'},
                {'technique': 'Mixup/CutMix', 'priority': 'medium', 'reason': 'Improve generalization', 'impact': '5-10%'},
            ])
        elif data_type == 'text':
            recommendations.extend([
                {'technique': 'Back-Translation', 'priority': 'high', 'reason': 'Generate paraphrases', 'impact': '10-20%'},
                {'technique': 'Word Dropout', 'priority': 'medium', 'reason': 'Regularization', 'impact': '5-10%'},
            ])
        elif data_type == 'audio':
            recommendations.extend([
                {'technique': 'SpecAugment', 'priority': 'high', 'reason': 'Mask time/freq', 'impact': '10-15%'},
                {'technique': 'Time Stretch', 'priority': 'medium', 'reason': 'Speed variation', 'impact': '5-10%'},
            ])
        elif data_type == 'tabular':
            recommendations.extend([
                {'technique': 'SMOTE', 'priority': 'high', 'reason': 'Synthetic oversampling', 'impact': '15-30%'},
                {'technique': 'Feature Noise', 'priority': 'medium', 'reason': 'Regularization', 'impact': '5-10%'},
            ])
        
        return recommendations[:5]
    
    def _generate_explanation(
        self,
        benefits: List[BenefitFactor],
        risks: List[RiskFactor],
        final_score: float,
        risk_level: RiskLevel,
        data_type: str
    ) -> str:
        """Generate comprehensive human-readable explanation."""
        lines = []
        
        # Benefit summary
        if benefits:
            lines.append("**Why synthetic data may help:**")
            for b in benefits:
                lines.append(f"  {b.icon} {b.explanation}")
        
        # Risk summary
        critical_risks = [r for r in risks if r.severity == "critical"]
        warning_risks = [r for r in risks if r.severity == "warning"]
        
        if critical_risks:
            lines.append("")
            lines.append("**Critical risks identified:**")
            for r in critical_risks:
                lines.append(f"  {r.icon} {r.explanation}")
        
        if warning_risks:
            lines.append("")
            lines.append("**Cautions:**")
            for r in warning_risks:
                lines.append(f"  {r.icon} {r.explanation}")
        
        # Validation requirement
        if any(r.requires_human_validation for r in risks):
            lines.append("")
            lines.append("⚠️ **Human validation required** before using synthetic data")
        
        return "\n".join(lines)
    
    def _decision_to_dict(
        self, 
        decision: SyntheticDecision,
        benefit_points: float,
        risk_points: float,
        data_type: str
    ) -> Dict[str, Any]:
        """Convert decision to API response format."""
        return {
            'status': 'success',
            'score': round(decision.score, 1),
            'verdict': decision.verdict,
            'risk_level': decision.risk_level.value,
            'data_type': data_type,
            
            # Score breakdown
            'score_breakdown': {
                'base': self.BASE_SCORE,
                'benefit_points': round(benefit_points, 1),
                'risk_points': round(risk_points, 1),
                'formula': f"{self.BASE_SCORE} + {round(benefit_points, 1)} - {round(risk_points, 1)} = {round(decision.score, 1)}"
            },
            
            # Benefits
            'benefits': [
                {
                    'name': b.name,
                    'points': b.points,
                    'explanation': b.explanation,
                    'icon': b.icon
                }
                for b in decision.benefits
            ],
            
            # Risks
            'risks': [
                {
                    'name': r.name,
                    'points': r.points,
                    'explanation': r.explanation,
                    'severity': r.severity,
                    'icon': r.icon,
                    'requires_validation': r.requires_human_validation
                }
                for r in decision.risks
            ],
            
            # Recommendations
            'recommendations': decision.recommendations,
            
            # Explanation
            'explanation': decision.explanation,
            'caution_flags': decision.caution_flags,
            'requires_human_validation': decision.requires_human_validation,
            
            # Summary counts
            'benefit_count': len(decision.benefits),
            'risk_count': len(decision.risks),
            'critical_risk_count': sum(1 for r in decision.risks if r.severity == "critical")
        }


# Singleton instance
synthetic_analyzer = Tier3SyntheticAnalyzer()
