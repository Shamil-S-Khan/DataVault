"""
Global Quality Index (GQI) Composite Scorer.

Multi-dimensional quality scoring system that combines:
  - Structural Clarity (S_struct): 20%
  - Representational Entropy (S_ent): 20%
  - Academic & Authority Signal (S_auth): 20%
  - Refined Operational Fitness (S_fit): 40%

Final score is modulated by a Label Reliability multiplier (0.8x – 1.2x)
and augmented with Synthetic Resilience context.
"""
import math
import re
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

from app.analytics.fitness_calculator import fitness_calculator
from app.analytics.synthetic_analyzer import synthetic_analyzer

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WEIGHTS = {
    'structural_clarity': 0.20,
    'representational_entropy': 0.20,
    'academic_authority': 0.20,
    'operational_fitness': 0.40,
}

# Label reliability keywords
HIGH_TRUST_KEYWORDS = [
    'expert-reviewed', 'expert reviewed', 'clinician-verified',
    'clinician verified', 'double-blind', 'double blind',
    'peer-reviewed', 'peer reviewed', 'gold standard',
    'manually annotated', 'manually curated', 'human-annotated',
    'human annotated', 'verified labels', 'quality-checked',
]

LOW_TRUST_KEYWORDS = [
    'scraped', 'unverified', 'raw dump', 'raw data dump',
    'auto-generated', 'auto generated', 'noisy labels',
    'crowd-sourced', 'crowdsourced', 'web crawl', 'web-crawled',
    'machine-generated', 'machine generated', 'uncurated',
]

# Academic / benchmark signal keywords
BENCHMARK_KEYWORDS = [
    'sota', 'state-of-the-art', 'state of the art',
    'benchmark', 'gold standard', 'leaderboard',
    'widely used', 'seminal', 'foundational',
    'competition', 'challenge dataset', 'reference dataset',
]

GRADE_THRESHOLDS: List[Tuple[float, str]] = [
    (0.90, 'A+'),
    (0.80, 'A'),
    (0.70, 'B+'),
    (0.60, 'B'),
    (0.50, 'C+'),
    (0.40, 'C'),
    (0.30, 'D'),
    (0.00, 'F'),
]


# ---------------------------------------------------------------------------
# Sub-score calculators
# ---------------------------------------------------------------------------

def _compute_structural_clarity(dataset: Dict[str, Any]) -> float:
    """
    S_struct: ratio of meaningfully-typed fields to total fields.

    Tier 1 – AI-analyzed intelligence fields exist.
    Tier 2 – HuggingFace-style feature list in source_metadata.
    Tier 3 – Presence-check on 5 core structural fields.
    """
    # --- Tier 1: intelligence.fields -----------------------------------------
    intelligence = dataset.get('intelligence') or {}
    fields = intelligence.get('fields')

    if fields and isinstance(fields, list) and len(fields) > 0:
        meaningful_roles = {'input', 'label', 'target', 'output'}
        mapped = sum(
            1 for f in fields
            if isinstance(f, dict) and f.get('role', '').lower() in meaningful_roles
        )
        total = len(fields)
        return min(1.0, mapped / max(total, 1))

    # --- Tier 2: source metadata features ------------------------------------
    source_meta = dataset.get('source', {}).get('source_metadata', {})
    features = source_meta.get('features') or source_meta.get('columns')
    if features and isinstance(features, (list, dict)):
        count = len(features) if isinstance(features, list) else len(features.keys())
        return min(1.0, count / 10)

    # --- Tier 3: basic structural presence -----------------------------------
    checks = [
        bool(dataset.get('description')),
        bool(dataset.get('domain')),
        bool(dataset.get('modality')),
        bool(dataset.get('size', {}).get('samples') or dataset.get('size', {}).get('file_size_gb')),
        bool(dataset.get('license')),
    ]
    return sum(checks) / len(checks)


def _compute_representational_entropy(dataset: Dict[str, Any]) -> float:
    """
    S_ent: Normalized Shannon Entropy of class distribution.

    H = -Σ(p_i × log(p_i)) / log(N)

    Returns 0.5 (neutral) when no class distribution is available.
    """
    # Try to find class distribution from various metadata locations
    class_dist: Optional[Dict[str, int]] = None

    # Direct fields
    for key in ('class_distribution', 'label_counts'):
        if key in dataset and isinstance(dataset[key], dict):
            class_dist = dataset[key]
            break

    # Inside statistics
    if class_dist is None:
        stats = dataset.get('statistics', {})
        if 'classes' in stats and isinstance(stats['classes'], dict):
            class_dist = stats['classes']

    # Inside metadata
    if class_dist is None:
        metadata = dataset.get('metadata', {})
        for key in ('class_distribution', 'label_counts'):
            if key in metadata and isinstance(metadata[key], dict):
                class_dist = metadata[key]
                break

    if not class_dist or len(class_dist) < 2:
        return 0.5  # neutral default

    # Filter sentinel keys
    counts = [v for k, v in class_dist.items() if not k.startswith('_') and isinstance(v, (int, float)) and v > 0]

    if len(counts) < 2:
        return 0.5

    total = sum(counts)
    n = len(counts)

    entropy = -sum((c / total) * math.log(c / total) for c in counts)
    max_entropy = math.log(n)

    if max_entropy == 0:
        return 0.5

    return min(1.0, entropy / max_entropy)


def _compute_academic_authority(dataset: Dict[str, Any]) -> float:
    """
    S_auth: Academic and authority signal.

    Base score from community metrics (downloads, citations, likes) on a
    logarithmic scale, boosted by benchmark-keyword presence.
    """
    source_meta = dataset.get('source', {}).get('source_metadata', {})
    metadata = dataset.get('metadata', {})

    # Collect engagement numbers
    downloads = source_meta.get('downloads', 0) or metadata.get('downloads', 0) or 0
    citations = source_meta.get('citations', 0) or metadata.get('citations', 0) or 0
    likes = source_meta.get('likes', 0) or metadata.get('likes', 0) or 0

    # Combined engagement on log scale → 0..1
    combined = downloads + (citations * 100) + (likes * 10)
    if combined > 0:
        # log10(1,000,000) ≈ 6 → full score
        base_score = min(1.0, math.log10(combined + 1) / 6.0)
    else:
        base_score = 0.0

    # Benchmark keyword boost
    text = ' '.join([
        dataset.get('description', ''),
        dataset.get('llm_summary', ''),
        str(dataset.get('intelligence', {}).get('summary', '')),
    ]).lower()

    keyword_hits = sum(1 for kw in BENCHMARK_KEYWORDS if kw in text)
    if keyword_hits >= 3:
        multiplier = 1.3
    elif keyword_hits >= 1:
        multiplier = 1.15
    else:
        multiplier = 1.0

    return min(1.0, base_score * multiplier)


def _compute_operational_fitness(dataset: Dict[str, Any]) -> float:
    """
    S_fit: Existing fitness calculator score normalized to 0-1.
    """
    try:
        result = fitness_calculator.calculate_fitness_score(dataset)
        return min(1.0, max(0.0, result['overall_score'] / 10.0))
    except Exception as e:
        logger.warning(f"Fitness calculation failed, falling back: {e}")
        return 0.5


# ---------------------------------------------------------------------------
# Label Reliability Multiplier
# ---------------------------------------------------------------------------

def _compute_label_reliability(dataset: Dict[str, Any]) -> Tuple[float, List[str], List[str]]:
    """
    Scan the description for trust signals.
    Returns (multiplier, high_signals_found, low_signals_found).
    Multiplier is clamped to [0.8, 1.2].
    """
    text = (dataset.get('description', '') or '').lower()
    llm_summary = (dataset.get('llm_summary', '') or '').lower()
    combined = f"{text} {llm_summary}"

    high_hits = [kw for kw in HIGH_TRUST_KEYWORDS if kw in combined]
    low_hits = [kw for kw in LOW_TRUST_KEYWORDS if kw in combined]

    # Each unique hit adjusts by ±0.04
    adjustment = (len(high_hits) * 0.04) - (len(low_hits) * 0.04)
    multiplier = max(0.8, min(1.2, 1.0 + adjustment))

    return multiplier, high_hits, low_hits


# ---------------------------------------------------------------------------
# Grade helper
# ---------------------------------------------------------------------------

def _score_to_grade(score: float) -> str:
    for threshold, grade in GRADE_THRESHOLDS:
        if score >= threshold:
            return grade
    return 'F'


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class CompositeScorer:
    """Calculate the Global Quality Index for a dataset."""

    def calculate_gqi(self, dataset: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate the full GQI for a single dataset document.

        Args:
            dataset: MongoDB dataset document (dict).

        Returns:
            Complete GQI result dict with score, grade, breakdown,
            label_trust, synthetic_resilience, and explanation.
        """
        # --- Sub-scores -------------------------------------------------------
        s_struct = _compute_structural_clarity(dataset)
        s_ent = _compute_representational_entropy(dataset)
        s_auth = _compute_academic_authority(dataset)
        s_fit = _compute_operational_fitness(dataset)

        breakdown = {
            'structural_clarity': round(s_struct, 4),
            'representational_entropy': round(s_ent, 4),
            'academic_authority': round(s_auth, 4),
            'operational_fitness': round(s_fit, 4),
        }

        raw_score = sum(breakdown[k] * WEIGHTS[k] for k in WEIGHTS)

        # --- Label reliability ------------------------------------------------
        lr_multiplier, high_signals, low_signals = _compute_label_reliability(dataset)
        final_score = max(0.0, min(1.0, raw_score * lr_multiplier))

        grade = _score_to_grade(final_score)

        # --- Synthetic resilience context -------------------------------------
        try:
            synth_result = synthetic_analyzer.analyze(dataset)
            synthetic_context = {
                'verdict': synth_result.get('verdict', 'Unknown'),
                'score': synth_result.get('score', 0),
                'risk_level': synth_result.get('risk_level', 'unknown'),
            }
        except Exception as e:
            logger.warning(f"Synthetic analysis failed: {e}")
            synthetic_context = {
                'verdict': 'Unavailable',
                'score': 0,
                'risk_level': 'unknown',
            }

        # --- Utility note for high-scarcity datasets --------------------------
        utility_note = None
        if synthetic_context.get('score', 0) >= 70 and final_score < 0.5:
            utility_note = (
                "This dataset has low raw quality, but high synthetic augmentation "
                "potential. Consider it a strong candidate for data augmentation pipelines."
            )

        # --- Explanation ------------------------------------------------------
        explanation = self._generate_explanation(
            final_score, grade, breakdown, lr_multiplier, dataset
        )

        return {
            'score': round(final_score, 4),
            'grade': grade,
            'raw_score': round(raw_score, 4),
            'label_reliability_multiplier': round(lr_multiplier, 2),
            'breakdown': breakdown,
            'label_trust': {
                'high_signals': high_signals,
                'low_signals': low_signals,
            },
            'synthetic_resilience': synthetic_context,
            'utility_note': utility_note,
            'explanation': explanation,
            'calculated_at': datetime.utcnow().isoformat(),
        }

    # ------------------------------------------------------------------
    # Explanation generator
    # ------------------------------------------------------------------

    @staticmethod
    def _generate_explanation(
        score: float,
        grade: str,
        breakdown: Dict[str, float],
        lr_multiplier: float,
        dataset: Dict[str, Any],
    ) -> str:
        name = dataset.get('canonical_name') or dataset.get('display_name') or 'This dataset'
        parts: List[str] = []

        # Overall
        if score >= 0.80:
            parts.append(f"{name} is a production-grade dataset with excellent quality signals.")
        elif score >= 0.60:
            parts.append(f"{name} is a good dataset suitable for most ML workflows.")
        elif score >= 0.40:
            parts.append(f"{name} is tutorial-grade — usable for learning but verify before production use.")
        else:
            parts.append(f"{name} has significant quality gaps that limit its practical utility.")

        # Highlight strongest / weakest dimension
        sorted_dims = sorted(breakdown.items(), key=lambda x: x[1], reverse=True)
        best_name, best_val = sorted_dims[0]
        worst_name, worst_val = sorted_dims[-1]

        dim_labels = {
            'structural_clarity': 'Structural Clarity',
            'representational_entropy': 'Class Balance',
            'academic_authority': 'Academic Authority',
            'operational_fitness': 'Operational Fitness',
        }

        if best_val >= 0.7:
            parts.append(f"Strongest signal: {dim_labels.get(best_name, best_name)} ({best_val:.0%}).")
        if worst_val < 0.4:
            parts.append(f"Weakest signal: {dim_labels.get(worst_name, worst_name)} ({worst_val:.0%}).")

        # Label reliability note
        if lr_multiplier > 1.0:
            parts.append("Label annotations show high-trust signals.")
        elif lr_multiplier < 1.0:
            parts.append("Label annotations show low-trust signals — verify before critical use.")

        return ' '.join(parts)


# Singleton
composite_scorer = CompositeScorer()
