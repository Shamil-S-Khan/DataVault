"""
License Analyzer for Dataset Safety Classification.
Determines commercial usability and provides clear guidance.
"""
from typing import Dict, Any, Optional, List
from enum import Enum
from dataclasses import dataclass
import logging
import re

logger = logging.getLogger(__name__)


class LicenseCategory(str, Enum):
    """License classification categories."""
    COMMERCIAL_SAFE = "commercial_safe"
    ATTRIBUTION_REQUIRED = "attribution_required"
    SHARE_ALIKE = "share_alike"
    NON_COMMERCIAL = "non_commercial"
    RESTRICTED = "restricted"
    UNKNOWN = "unknown"


@dataclass
class LicenseInfo:
    """Structured license information."""
    name: str
    category: LicenseCategory
    commercial_use: bool
    attribution_required: bool
    share_alike: bool
    description: str
    implications: str
    color: str  # For UI: green, yellow, orange, red


# Comprehensive license database
LICENSE_DATABASE = {
    # Fully permissive (commercial safe)
    'mit': LicenseInfo(
        name='MIT License',
        category=LicenseCategory.COMMERCIAL_SAFE,
        commercial_use=True,
        attribution_required=True,
        share_alike=False,
        description='Very permissive open source license',
        implications='You can use this dataset for any purpose including commercial. Just include the license notice.',
        color='green'
    ),
    'apache-2.0': LicenseInfo(
        name='Apache 2.0',
        category=LicenseCategory.COMMERCIAL_SAFE,
        commercial_use=True,
        attribution_required=True,
        share_alike=False,
        description='Permissive license with patent protection',
        implications='Commercial use allowed. Include attribution and license. Provides patent rights.',
        color='green'
    ),
    'bsd-3-clause': LicenseInfo(
        name='BSD 3-Clause',
        category=LicenseCategory.COMMERCIAL_SAFE,
        commercial_use=True,
        attribution_required=True,
        share_alike=False,
        description='Permissive BSD license',
        implications='Commercial use allowed with attribution. Cannot use project name for endorsement.',
        color='green'
    ),
    'bsd-2-clause': LicenseInfo(
        name='BSD 2-Clause',
        category=LicenseCategory.COMMERCIAL_SAFE,
        commercial_use=True,
        attribution_required=True,
        share_alike=False,
        description='Simplified BSD license',
        implications='Commercial use allowed with attribution.',
        color='green'
    ),
    'cc0-1.0': LicenseInfo(
        name='CC0 1.0 (Public Domain)',
        category=LicenseCategory.COMMERCIAL_SAFE,
        commercial_use=True,
        attribution_required=False,
        share_alike=False,
        description='Public domain dedication',
        implications='No restrictions. Use for any purpose without attribution.',
        color='green'
    ),
    'unlicense': LicenseInfo(
        name='Unlicense',
        category=LicenseCategory.COMMERCIAL_SAFE,
        commercial_use=True,
        attribution_required=False,
        share_alike=False,
        description='Public domain equivalent',
        implications='No restrictions whatsoever.',
        color='green'
    ),
    'openrail': LicenseInfo(
        name='OpenRAIL',
        category=LicenseCategory.COMMERCIAL_SAFE,
        commercial_use=True,
        attribution_required=True,
        share_alike=False,
        description='AI-focused open license',
        implications='Commercial use allowed. Designed for AI/ML with responsible use clauses.',
        color='green'
    ),
    
    # Attribution required (still commercial safe)
    'cc-by-4.0': LicenseInfo(
        name='CC BY 4.0',
        category=LicenseCategory.ATTRIBUTION_REQUIRED,
        commercial_use=True,
        attribution_required=True,
        share_alike=False,
        description='Creative Commons Attribution',
        implications='Commercial use allowed. Must give credit to the creator.',
        color='green'
    ),
    
    # Share-alike (commercial with conditions)
    'cc-by-sa-4.0': LicenseInfo(
        name='CC BY-SA 4.0',
        category=LicenseCategory.SHARE_ALIKE,
        commercial_use=True,
        attribution_required=True,
        share_alike=True,
        description='Creative Commons Attribution ShareAlike',
        implications='Commercial use allowed, but derivatives must use same license.',
        color='yellow'
    ),
    'gpl-3.0': LicenseInfo(
        name='GPL 3.0',
        category=LicenseCategory.SHARE_ALIKE,
        commercial_use=True,
        attribution_required=True,
        share_alike=True,
        description='GNU General Public License',
        implications='Commercial use allowed, but all derivative works must be GPL-licensed.',
        color='yellow'
    ),
    'lgpl-3.0': LicenseInfo(
        name='LGPL 3.0',
        category=LicenseCategory.SHARE_ALIKE,
        commercial_use=True,
        attribution_required=True,
        share_alike=True,
        description='GNU Lesser GPL',
        implications='More permissive than GPL. Can link without full GPL requirements.',
        color='yellow'
    ),
    
    # Non-commercial
    'cc-by-nc-4.0': LicenseInfo(
        name='CC BY-NC 4.0',
        category=LicenseCategory.NON_COMMERCIAL,
        commercial_use=False,
        attribution_required=True,
        share_alike=False,
        description='Creative Commons Non-Commercial',
        implications='⚠️ NO COMMERCIAL USE. Research and personal use only.',
        color='orange'
    ),
    'cc-by-nc-sa-4.0': LicenseInfo(
        name='CC BY-NC-SA 4.0',
        category=LicenseCategory.NON_COMMERCIAL,
        commercial_use=False,
        attribution_required=True,
        share_alike=True,
        description='Creative Commons NC ShareAlike',
        implications='⚠️ NO COMMERCIAL USE. Derivatives must share alike.',
        color='orange'
    ),
    
    # Restricted
    'proprietary': LicenseInfo(
        name='Proprietary',
        category=LicenseCategory.RESTRICTED,
        commercial_use=False,
        attribution_required=True,
        share_alike=False,
        description='Proprietary license',
        implications='⛔ RESTRICTED. Check specific terms before any use.',
        color='red'
    ),
}


class LicenseAnalyzer:
    """Analyze and classify dataset licenses."""
    
    # Patterns to detect license types
    LICENSE_PATTERNS = [
        (r'\bmit\b', 'mit'),
        (r'apache[\s-]?2\.?0?', 'apache-2.0'),
        (r'bsd[\s-]?3', 'bsd-3-clause'),
        (r'bsd[\s-]?2', 'bsd-2-clause'),
        (r'cc0|cc[\s-]?zero|public[\s-]?domain', 'cc0-1.0'),
        (r'unlicense', 'unlicense'),
        (r'openrail', 'openrail'),
        (r'cc[\s-]?by[\s-]?nc[\s-]?sa', 'cc-by-nc-sa-4.0'),
        (r'cc[\s-]?by[\s-]?nc', 'cc-by-nc-4.0'),
        (r'cc[\s-]?by[\s-]?sa', 'cc-by-sa-4.0'),
        (r'cc[\s-]?by(?![\s-]?nc)(?![\s-]?sa)', 'cc-by-4.0'),
        (r'gpl[\s-]?3', 'gpl-3.0'),
        (r'lgpl', 'lgpl-3.0'),
        (r'proprietary|restricted|confidential', 'proprietary'),
    ]
    
    def analyze_license(self, dataset: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze dataset license and return safety classification.
        
        Args:
            dataset: Dataset document from MongoDB
            
        Returns:
            Dictionary with license analysis results
        """
        # Extract license string from various sources
        license_str = self._extract_license(dataset)
        
        # Try to identify the license
        license_key = self._identify_license(license_str)
        
        if license_key and license_key in LICENSE_DATABASE:
            info = LICENSE_DATABASE[license_key]
            return {
                'detected_license': info.name,
                'raw_license': license_str,
                'category': info.category.value,
                'commercial_use': info.commercial_use,
                'attribution_required': info.attribution_required,
                'share_alike': info.share_alike,
                'description': info.description,
                'implications': info.implications,
                'color': info.color,
                'confidence': 'high'
            }
        
        # Unknown license
        return {
            'detected_license': license_str if license_str else 'Unknown',
            'raw_license': license_str,
            'category': LicenseCategory.UNKNOWN.value,
            'commercial_use': None,
            'attribution_required': None,
            'share_alike': None,
            'description': 'License could not be identified',
            'implications': '⚠️ Unknown license. Review the original source before use.',
            'color': 'gray',
            'confidence': 'low'
        }
    
    def _extract_license(self, dataset: Dict[str, Any]) -> Optional[str]:
        """Extract license string from dataset."""
        # Direct license field
        license_str = dataset.get('license')
        if license_str:
            return str(license_str)
        
        # From source metadata
        metadata = dataset.get('source', {}).get('source_metadata', {})
        license_str = metadata.get('license')
        if license_str:
            return str(license_str)
        
        # From tags
        tags = metadata.get('tags', [])
        for tag in tags:
            tag_str = str(tag).lower()
            if 'license:' in tag_str:
                return tag_str.split('license:')[-1].strip()
        
        return None
    
    def _identify_license(self, license_str: Optional[str]) -> Optional[str]:
        """Identify license key from string."""
        if not license_str:
            return None
        
        license_lower = license_str.lower()
        
        # Check patterns
        for pattern, license_key in self.LICENSE_PATTERNS:
            if re.search(pattern, license_lower):
                return license_key
        
        # Direct match in database
        for key in LICENSE_DATABASE:
            if key in license_lower:
                return key
        
        return None
    
    def get_safety_badge(self, dataset: Dict[str, Any]) -> Dict[str, str]:
        """
        Get simplified safety badge for UI display.
        
        Returns:
            {
                'status': 'safe' | 'caution' | 'restricted' | 'unknown',
                'label': 'Commercial Safe' | 'Non-Commercial' | etc.,
                'color': 'green' | 'yellow' | 'orange' | 'red' | 'gray'
            }
        """
        analysis = self.analyze_license(dataset)
        category = analysis.get('category')
        
        if category == LicenseCategory.COMMERCIAL_SAFE.value:
            return {'status': 'safe', 'label': 'Commercial Safe', 'color': 'green'}
        elif category == LicenseCategory.ATTRIBUTION_REQUIRED.value:
            return {'status': 'safe', 'label': 'Attribution Required', 'color': 'green'}
        elif category == LicenseCategory.SHARE_ALIKE.value:
            return {'status': 'caution', 'label': 'Share-Alike Required', 'color': 'yellow'}
        elif category == LicenseCategory.NON_COMMERCIAL.value:
            return {'status': 'restricted', 'label': 'Non-Commercial Only', 'color': 'orange'}
        elif category == LicenseCategory.RESTRICTED.value:
            return {'status': 'restricted', 'label': 'Restricted', 'color': 'red'}
        else:
            return {'status': 'unknown', 'label': 'License Unknown', 'color': 'gray'}
    
    def can_use_commercially(self, dataset: Dict[str, Any]) -> Dict[str, Any]:
        """
        Quick check: Can this dataset be used commercially?
        
        Returns:
            {
                'allowed': True | False | None (unknown),
                'conditions': [...],
                'warning': str or None
            }
        """
        analysis = self.analyze_license(dataset)
        
        commercial = analysis.get('commercial_use')
        conditions = []
        warning = None
        
        if analysis.get('attribution_required'):
            conditions.append('Attribution required')
        
        if analysis.get('share_alike'):
            conditions.append('Derivatives must use same license')
        
        if commercial is False:
            warning = 'This dataset explicitly prohibits commercial use.'
        elif commercial is None:
            warning = 'License could not be identified. Verify before commercial use.'
        
        return {
            'allowed': commercial,
            'conditions': conditions,
            'warning': warning
        }


# Singleton instance
license_analyzer = LicenseAnalyzer()
