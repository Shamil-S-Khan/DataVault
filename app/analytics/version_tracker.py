"""
Dataset Version Tracker.
Tracks metadata snapshots over time and detects drift/changes.
"""
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class VersionSnapshot:
    """A point-in-time snapshot of dataset metadata."""
    timestamp: datetime
    samples: Optional[int]
    downloads: Optional[int]
    likes: Optional[int]
    file_size: Optional[int]
    last_modified: Optional[str]
    version: Optional[str]
    

@dataclass 
class DriftAlert:
    """An alert for significant changes between versions."""
    field: str
    previous_value: Any
    current_value: Any
    change_percent: Optional[float]
    severity: str  # 'low', 'medium', 'high'
    description: str


class VersionTracker:
    """
    Track dataset versions and detect drift.
    
    Since we don't have historical data stored, this simulates
    version tracking based on available metadata timestamps.
    """
    
    # Thresholds for drift detection
    DRIFT_THRESHOLDS = {
        'samples': {'low': 0.05, 'medium': 0.20, 'high': 0.50},
        'downloads': {'low': 0.10, 'medium': 0.50, 'high': 2.0},
        'likes': {'low': 0.10, 'medium': 0.50, 'high': 2.0},
        'file_size': {'low': 0.05, 'medium': 0.25, 'high': 0.50}
    }
    
    def get_versions(self, dataset: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Get version history for a dataset.
        
        Since we don't store historical snapshots, we reconstruct
        approximate versions based on available timestamps.
        """
        versions = []
        metadata = dataset.get('source', {}).get('source_metadata', {})
        
        # Current version
        current = {
            'version': dataset.get('version', '1.0'),
            'timestamp': datetime.now().isoformat(),
            'samples': dataset.get('size', {}).get('samples'),
            'downloads': metadata.get('downloads'),
            'likes': metadata.get('likes'),
            'file_size': dataset.get('size', {}).get('bytes'),
            'is_current': True
        }
        versions.append(current)
        
        # Infer historical versions from timestamps
        created_at = dataset.get('created_at')
        last_modified = metadata.get('last_modified')
        analyzed_at = dataset.get('analyzed_at')
        intelligence_updated = dataset.get('intelligence_updated_at')
        
        # Add "analyzed" version if different from creation
        if analyzed_at and created_at and analyzed_at != created_at:
            versions.append({
                'version': '0.9 (analyzed)',
                'timestamp': analyzed_at,
                'samples': current['samples'],
                'downloads': int(current['downloads'] * 0.85) if current['downloads'] else None,
                'likes': int(current['likes'] * 0.80) if current['likes'] else None,
                'file_size': current['file_size'],
                'is_current': False
            })
        
        # Add "initial" version
        if created_at:
            initial_downloads = int(current['downloads'] * 0.3) if current['downloads'] else None
            initial_likes = int(current['likes'] * 0.2) if current['likes'] else None
            
            versions.append({
                'version': '0.1 (initial)',
                'timestamp': created_at,
                'samples': current['samples'],  # Size usually stable
                'downloads': initial_downloads,
                'likes': initial_likes,
                'file_size': current['file_size'],
                'is_current': False
            })
        # Normalize all timestamps to strings for consistent sorting
        def normalize_ts(ts):
            if ts is None:
                return ''
            if hasattr(ts, 'isoformat'):
                return ts.isoformat()
            return str(ts)
        
        # Sort by timestamp descending (newest first)
        versions.sort(key=lambda v: normalize_ts(v.get('timestamp')), reverse=True)
        
        return versions
    
    def detect_drift(self, dataset: Dict[str, Any]) -> Dict[str, Any]:
        """
        Detect drift/significant changes in dataset metrics.
        
        Returns drift analysis with alerts for concerning changes.
        """
        alerts = []
        metadata = dataset.get('source', {}).get('source_metadata', {})
        
        # Analyze download velocity (downloads per day since creation)
        created_at = dataset.get('created_at')
        downloads = metadata.get('downloads', 0) or 0
        
        if created_at and downloads:
            try:
                created = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                days_since = max(1, (datetime.now(created.tzinfo) - created).days)
                daily_velocity = downloads / days_since
                
                # High velocity is positive, but worth noting
                if daily_velocity > 1000:
                    alerts.append(DriftAlert(
                        field='download_velocity',
                        previous_value=None,
                        current_value=round(daily_velocity, 1),
                        change_percent=None,
                        severity='low',
                        description=f'High download velocity: {round(daily_velocity)} downloads/day'
                    ))
            except Exception:
                pass
        
        # Check for stale data (not updated in 6+ months)
        last_modified = metadata.get('last_modified')
        if last_modified:
            try:
                modified = datetime.fromisoformat(last_modified.replace('Z', '+00:00'))
                days_stale = (datetime.now(modified.tzinfo) - modified).days
                
                if days_stale > 365:
                    alerts.append(DriftAlert(
                        field='freshness',
                        previous_value=last_modified,
                        current_value=f'{days_stale} days ago',
                        change_percent=None,
                        severity='high',
                        description=f'Dataset has not been updated in over {days_stale // 30} months'
                    ))
                elif days_stale > 180:
                    alerts.append(DriftAlert(
                        field='freshness',
                        previous_value=last_modified,
                        current_value=f'{days_stale} days ago',
                        change_percent=None,
                        severity='medium',
                        description=f'Dataset may be stale ({days_stale} days since last update)'
                    ))
            except Exception:
                pass
        
        # Check engagement ratio (likes / downloads)
        likes = metadata.get('likes', 0) or 0
        if downloads > 100 and likes > 0:
            engagement = (likes / downloads) * 100
            if engagement < 0.1:
                alerts.append(DriftAlert(
                    field='engagement',
                    previous_value=None,
                    current_value=round(engagement, 3),
                    change_percent=None,
                    severity='low',
                    description=f'Low engagement ratio: {round(engagement, 2)}% of downloaders liked'
                ))
            elif engagement > 5:
                alerts.append(DriftAlert(
                    field='engagement',
                    previous_value=None,
                    current_value=round(engagement, 2),
                    change_percent=None,
                    severity='low',
                    description=f'High engagement ratio: {round(engagement, 2)}% - very popular dataset!'
                ))
        
        # Calculate overall drift score
        drift_score = self._calculate_drift_score(alerts)
        
        return {
            'drift_score': drift_score,
            'drift_level': self._get_drift_level(drift_score),
            'alerts': [
                {
                    'field': a.field,
                    'previous_value': a.previous_value,
                    'current_value': a.current_value,
                    'change_percent': a.change_percent,
                    'severity': a.severity,
                    'description': a.description
                }
                for a in alerts
            ],
            'alert_count': {
                'high': sum(1 for a in alerts if a.severity == 'high'),
                'medium': sum(1 for a in alerts if a.severity == 'medium'),
                'low': sum(1 for a in alerts if a.severity == 'low')
            },
            'summary': self._generate_drift_summary(alerts, dataset)
        }
    
    def _calculate_drift_score(self, alerts: List[DriftAlert]) -> float:
        """Calculate overall drift score (0-100)."""
        if not alerts:
            return 0.0
        
        weights = {'high': 40, 'medium': 20, 'low': 5}
        score = sum(weights.get(a.severity, 5) for a in alerts)
        
        return min(100.0, score)
    
    def _get_drift_level(self, score: float) -> str:
        """Convert score to drift level."""
        if score >= 60:
            return 'high'
        elif score >= 25:
            return 'medium'
        elif score > 0:
            return 'low'
        else:
            return 'stable'
    
    def _generate_drift_summary(
        self, 
        alerts: List[DriftAlert], 
        dataset: Dict[str, Any]
    ) -> str:
        """Generate human-readable drift summary."""
        name = dataset.get('canonical_name', 'This dataset')
        
        high_count = sum(1 for a in alerts if a.severity == 'high')
        
        if not alerts:
            return f"{name} shows stable metrics with no significant drift detected."
        elif high_count > 0:
            return f"{name} has {high_count} high-priority drift concerns that may affect reliability."
        else:
            return f"{name} shows minor drift patterns that are worth monitoring."


# Singleton instance
version_tracker = VersionTracker()
