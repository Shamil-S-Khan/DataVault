"""
Metadata-Only Tier 1 Quality Scorer  (v2)
==========================================

This module is the **sole source of truth** for the dataset quality API.
It calculates an accurate proxy for data quality using only the metrics
stored in the database — no remote file downloading or streaming.

DEPRECATION NOTICE
------------------
``app.ml.quality_scoring`` is DEPRECATED and must NOT be imported by any
new code.  All quality scoring must go through this module
(``app.ml.quality_scorer``).  The deprecated module will be removed in a
future release.

Architecture
------------
Five scoring pillars, dynamically weighted:

    ┌─────────────────────┬──────────────┐
    │ Pillar              │ Base Weight  │
    ├─────────────────────┼──────────────┤
    │ Completeness        │ 0.25         │
    │ Documentation       │ 0.20         │
    │ Metadata Richness   │ 0.15         │
    │ Format & Schema     │ 0.20         │
    │ Community Validation│ 0.20         │
    └─────────────────────┴──────────────┘

Cold-start fix
--------------
For datasets younger than 30 days the Community Validation weight is
linearly scaled down toward zero and the surplus is proportionally
redistributed across the remaining pillars so brand-new datasets are
not penalised for lacking downloads / likes.
"""

import math
import re
from datetime import datetime
from typing import Any, Dict, Optional


# ── Weight constants ─────────────────────────────────────────────────────

BASE_WEIGHTS: Dict[str, float] = {
    "completeness": 0.25,
    "documentation": 0.20,
    "metadata_richness": 0.15,
    "format_schema": 0.20,
    "community_validation": 0.20,
}

COLD_START_DAYS = 30  # datasets younger than this get dynamic weighting


# ── Format tier tables ───────────────────────────────────────────────────

AI_READY_FORMATS = frozenset({
    "parquet", "jsonl", "arrow", "feather", "tfrecord",
    "hdf5", "h5", "npy", "npz", "safetensors", "petastorm",
    "lance", "csv", "tsv",
})

ACCEPTABLE_FORMATS = frozenset({
    "json", "xml", "avro", "orc", "sqlite", "db",
    "xlsx", "xls", "pickle", "pkl", "msgpack",
})

OPAQUE_FORMATS = frozenset({
    "zip", "rar", "7z", "tar", "gz", "bz2", "xz", "zst",
    "exe", "bin", "dat", "pdf", "doc", "docx", "ppt", "pptx",
})


class QualityScorer:
    """
    Metadata-Only Tier 1 quality assessment for datasets.

    Combines completeness, documentation, metadata richness,
    format / schema profile, and community validation into a single
    0–1 score with dynamic cold-start weighting.
    """

    # ── helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        """Coerce *value* to float, returning *default* on failure."""
        if value is None:
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _resolve_dataset_age_days(dataset: Dict[str, Any]) -> Optional[float]:
        """
        Return the dataset's age in fractional days, preferring the
        earliest publication-style timestamp when available.

        Priority:
            metadata.published_at / metadata.publication_date /
            metadata.last_updated  →  top-level created_at.
        """
        now = datetime.utcnow()
        candidates: list = []

        # Publication-style dates stored in metadata
        meta = dataset.get("metadata") or {}
        for key in ("published_at", "publication_date", "last_updated"):
            val = meta.get(key)
            if isinstance(val, datetime):
                candidates.append(val)
            elif isinstance(val, str):
                for fmt in (
                    "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%dT%H:%M:%SZ",
                    "%Y-%m-%d",
                ):
                    try:
                        candidates.append(datetime.strptime(val, fmt))
                        break
                    except ValueError:
                        continue

        # Top-level created_at (set at ingest time)
        created = dataset.get("created_at")
        if isinstance(created, datetime):
            candidates.append(created)

        if not candidates:
            return None  # unknown age

        earliest = min(candidates)
        delta = now - earliest
        return max(0.0, delta.total_seconds() / 86_400)

    def _get_effective_weights(
        self, dataset: Dict[str, Any]
    ) -> Dict[str, float]:
        """
        Return pillar weights, redistributing the community weight for
        datasets younger than ``COLD_START_DAYS``.
        """
        age_days = self._resolve_dataset_age_days(dataset)

        # Unknown age or older than threshold → base weights
        if age_days is None or age_days > COLD_START_DAYS:
            return dict(BASE_WEIGHTS)

        # Linearly ramp community weight: 0 (brand-new) → full (at threshold)
        t = age_days / COLD_START_DAYS
        community_w = BASE_WEIGHTS["community_validation"] * t
        surplus = BASE_WEIGHTS["community_validation"] - community_w

        non_community = {
            k: v for k, v in BASE_WEIGHTS.items() if k != "community_validation"
        }
        nc_total = sum(non_community.values())

        weights = {
            k: v + surplus * (v / nc_total) for k, v in non_community.items()
        }
        weights["community_validation"] = community_w
        return weights

    # ── public API ────────────────────────────────────────────────────

    def calculate_quality_score(self, dataset: Dict[str, Any]) -> float:
        """
        Calculate comprehensive quality score (0-1).

        Pillars (base weights, adjusted dynamically for cold-start):
          - Completeness      (0.25)
          - Documentation     (0.20)
          - Metadata Richness (0.15) — descriptive fields only
          - Format & Schema   (0.20) — file format, density, schema
          - Community Valid.   (0.20) — log-scaled downloads/likes/views

        Args:
            dataset: Dataset document from MongoDB.

        Returns:
            Quality score clamped to [0, 1].
        """
        scores = {
            "completeness": self._calculate_completeness(dataset),
            "documentation": self._calculate_documentation_quality(dataset),
            "metadata_richness": self._calculate_metadata_richness(dataset),
            "format_schema": self._calculate_format_schema(dataset),
            "community_validation": self._calculate_community_validation(dataset),
        }

        weights = self._get_effective_weights(dataset)
        overall = sum(scores[k] * weights[k] for k in weights)
        return min(1.0, max(0.0, overall))

    def get_quality_breakdown(self, dataset: Dict[str, Any]) -> Dict[str, Any]:
        """
        Detailed breakdown of quality components — used by the API to
        explain scores to users.

        Returns dict with keys:
            overall, completeness, documentation, metadata_richness,
            format_schema, community_validation, weights, is_cold_start.
        """
        scores = {
            "completeness": self._calculate_completeness(dataset),
            "documentation": self._calculate_documentation_quality(dataset),
            "metadata_richness": self._calculate_metadata_richness(dataset),
            "format_schema": self._calculate_format_schema(dataset),
            "community_validation": self._calculate_community_validation(dataset),
        }

        weights = self._get_effective_weights(dataset)
        overall = sum(scores[k] * weights[k] for k in weights)
        overall = min(1.0, max(0.0, overall))

        age_days = self._resolve_dataset_age_days(dataset)
        is_cold_start = age_days is not None and age_days <= COLD_START_DAYS

        return {
            "overall": overall,
            **scores,
            "weights": weights,
            "is_cold_start": is_cold_start,
        }

    def get_quality_label(self, score: float) -> str:
        """Human-readable label for a 0–1 quality score."""
        if score >= 0.80:
            return "Excellent"
        if score >= 0.60:
            return "Good"
        if score >= 0.40:
            return "Fair"
        if score >= 0.20:
            return "Poor"
        return "Very Poor"

    # ── pillar implementations ────────────────────────────────────────

    # 1. Completeness (0-1)
    def _calculate_completeness(self, dataset: Dict[str, Any]) -> float:
        """Presence of essential catalogue fields."""
        score = 0.0
        max_points = 10

        # Description (2 pts)
        if dataset.get("description"):
            score += 2

        # License (2 pts)
        if dataset.get("license"):
            score += 2

        # Size information (2 pts)
        size = dataset.get("size") or {}
        if size.get("samples") or size.get("file_size_gb") or size.get("file_size_bytes"):
            score += 2

        # Domain classification (1 pt)
        if dataset.get("domain"):
            score += 1

        # Modality classification (1 pt)
        if dataset.get("modality"):
            score += 1

        # Source URL (1 pt)
        if (dataset.get("source") or {}).get("url"):
            score += 1

        # Metadata (1 pt)
        if dataset.get("metadata"):
            score += 1

        return score / max_points

    # 2. Documentation quality (0-1)
    def _calculate_documentation_quality(self, dataset: Dict[str, Any]) -> float:
        """Evaluate description quality and length."""
        description = dataset.get("description", "") or ""
        if not description:
            return 0.0

        score = 0.0

        # Length score (0-0.4) — optimal ≥ 200 chars, caps at ~2500
        length = len(description)
        if length >= 200:
            score += min(0.4, length / 2500)
        else:
            score += (length / 200) * 0.4

        # Readability (0-0.3)
        stripped = description.strip()
        if stripped and stripped[-1] in ".!?":
            score += 0.1
        if description.count(".") + description.count("!") + description.count("?") >= 2:
            score += 0.1
        if " " in description and len(description.split()) >= 10:
            score += 0.1

        # Structure (0-0.3)
        if re.search(r"\d+", description):
            score += 0.1
        keywords = ["dataset", "data", "contains", "includes", "features", "samples", "rows"]
        if any(kw in description.lower() for kw in keywords):
            score += 0.1
        generic = ["this is a dataset", "dataset for", "data for"]
        if not any(p in description.lower() for p in generic):
            score += 0.1

        return min(1.0, score)

    # 3. Metadata richness (0-1) — descriptive fields only, NO traffic metrics
    def _calculate_metadata_richness(self, dataset: Dict[str, Any]) -> float:
        """
        Evaluate quantity and quality of *descriptive* metadata.

        Community / traffic metrics (downloads, likes, votes, views,
        usability_score) are deliberately excluded here to avoid
        double-counting with the Community Validation pillar.
        """
        metadata = dataset.get("metadata") or {}
        if not metadata:
            return 0.0

        score = 0.0
        community_keys = {
            "downloads", "likes", "votes", "views", "usability_score"
        }
        descriptive_metadata = {
            key: value for key, value in metadata.items() if key not in community_keys
        }

        # ── breadth: raw field count (0-0.5)
        field_count = len(descriptive_metadata)
        score += min(0.5, field_count / 20)

        # ── depth: presence of high-value descriptive fields (0-0.5)
        descriptive_fields = [
            "num_instances", "num_features", "num_classes",
            "format", "version", "last_updated", "creator_name",
            "tags", "subjects", "authors", "language",
            "task_type", "license_id",
        ]
        present = sum(1 for f in descriptive_fields if f in metadata)
        score += (present / len(descriptive_fields)) * 0.5

        return min(1.0, score)

    # 4. Format & Schema (0-1) — database-only
    def _calculate_format_schema(self, dataset: Dict[str, Any]) -> float:
        """
        Evaluate the dataset's physical profile from stored DB fields.

        Sub-dimensions:
          * Format tier   (0-0.40)
          * Data density  (0-0.30)
          * Schema detail (0-0.30)
        """
        score = 0.0

        # ── 4a. Format tier (0-0.40) ────────────────────────────────
        formats = self._extract_formats(dataset)
        if formats:
            best = self._best_format_score(formats)
            score += best * 0.40
        else:
            # No format info → neutral, don't penalise
            score += 0.20

        # ── 4b. Data density heuristic (0-0.30) ─────────────────────
        size = dataset.get("size") or {}
        file_bytes = self._safe_float(
            size.get("file_size_bytes")
            or self._safe_float(size.get("file_size_gb")) * 1_073_741_824
        )
        num_rows = self._safe_float(
            size.get("samples")
            or (dataset.get("metadata") or {}).get("num_instances")
        )

        if file_bytes > 0 and num_rows > 0:
            bytes_per_row = file_bytes / num_rows
            # Reasonable density: 50 B – 50 KB per row → full marks
            if 50 <= bytes_per_row <= 50_000:
                score += 0.30
            elif bytes_per_row < 50:
                # Suspiciously tiny rows (likely stub / empty)
                score += 0.10
            elif bytes_per_row <= 500_000:
                # Somewhat bloated but usable
                score += 0.20
            else:
                # Very bloated / binary blobs
                score += 0.05
        elif file_bytes > 0 or num_rows > 0:
            # Partial info → modest credit
            score += 0.15
        else:
            # No size info at all → neutral
            score += 0.15

        # ── 4c. Schema / feature detail (0-0.30) ────────────────────
        meta = dataset.get("metadata") or {}
        schema_signals = 0
        total_schema_checks = 5

        if meta.get("num_features") or meta.get("num_instances"):
            schema_signals += 1
        if meta.get("num_classes"):
            schema_signals += 1
        if meta.get("features") or meta.get("columns") or meta.get("schema"):
            schema_signals += 1
        if meta.get("task_type") or meta.get("task"):
            schema_signals += 1
        if meta.get("target_column") or meta.get("label_column"):
            schema_signals += 1

        score += (schema_signals / total_schema_checks) * 0.30

        return min(1.0, score)

    # ── format helpers ────────────────────────────────────────────────

    @staticmethod
    def _extract_formats(dataset: Dict[str, Any]) -> set:
        """
        Collect all format tokens from various metadata locations,
        returning a set of lowercase extension-style strings.
        """
        tokens: set = set()
        meta = dataset.get("metadata") or {}

        # metadata.format (single string – OpenML style)
        fmt = meta.get("format")
        if isinstance(fmt, str) and fmt:
            tokens.add(fmt.strip().lower().lstrip("."))

        # metadata.formats (list – Zenodo / Data.gov style)
        fmts = meta.get("formats")
        if isinstance(fmts, list):
            for f in fmts:
                if isinstance(f, str) and f:
                    tokens.add(f.strip().lower().lstrip("."))

        # metadata.file_type / file_format
        for key in ("file_type", "file_format"):
            val = meta.get(key)
            if isinstance(val, str) and val:
                tokens.add(val.strip().lower().lstrip("."))

        return tokens

    @staticmethod
    def _best_format_score(formats: set) -> float:
        """Return the highest format-tier score from a set of formats."""
        best = 0.0
        for fmt in formats:
            if fmt in AI_READY_FORMATS:
                return 1.0  # can't do better
            if fmt in ACCEPTABLE_FORMATS:
                best = max(best, 0.6)
            elif fmt in OPAQUE_FORMATS:
                best = max(best, 0.2)
            else:
                best = max(best, 0.4)  # unknown → neutral
        return best

    # 5. Community validation (0-1) — log-scaled, unchanged math
    def _calculate_community_validation(self, dataset: Dict[str, Any]) -> float:
        """
        Evaluate community engagement and validation via log-scaled
        downloads, likes/votes, views, and platform usability rating.
        """
        source_metadata = (
            (dataset.get("source") or {}).get("source_metadata") or {}
        )
        score = 0.0

        # Downloads (0-0.35)
        downloads = self._safe_float(source_metadata.get("downloads"))
        if downloads > 0:
            score += min(0.35, math.log10(downloads + 1) / math.log10(100_000))

        # Likes / Votes (0-0.35)
        likes = self._safe_float(source_metadata.get("likes"))
        votes = self._safe_float(source_metadata.get("votes"))
        popularity = likes + votes
        if popularity > 0:
            score += min(0.35, math.log10(popularity + 1) / math.log10(10_000))

        # Views (0-0.15)
        views = self._safe_float(source_metadata.get("views"))
        if views > 0:
            score += min(0.15, math.log10(views + 1) / math.log10(50_000))

        # Usability / quality rating (0-0.15)
        usability = self._safe_float(source_metadata.get("usability_score"))
        if usability > 0:
            score += (min(usability, 10.0) / 10.0) * 0.15

        return min(1.0, score)


# Global singleton
quality_scorer = QualityScorer()
