"""
Data.gov dataset scraper.
API: CKAN API at https://catalog.data.gov/api/3/
Massive repository - uses strict ML filtering to reduce 300K+ to ~5K-15K.
"""
from typing import Dict, List, Any
import logging
from app.scrapers.base import BaseScraper
from app.scrapers.ml_filter import ml_filter

logger = logging.getLogger(__name__)


class DataGovScraper(BaseScraper):
    """Scraper for Data.gov using CKAN API."""
    
    BASE_URL = "https://catalog.data.gov/api/3"
    
    # ML-focused search queries to pre-filter the massive catalog
    ML_QUERIES = [
        'machine learning',
        'artificial intelligence',
        'training dataset',
        'classification data',
        'prediction model',
        'deep learning',
        'neural network',
        'computer vision',
        'natural language',
        'sensor data time series',
        'labeled data annotations',
        'benchmark evaluation',
    ]
    
    # Tags that indicate ML relevance
    ML_RELEVANT_TAGS = [
        'machine-learning', 'ai', 'artificial-intelligence',
        'data-science', 'analytics', 'prediction',
        'classification', 'regression', 'clustering',
    ]
    
    def __init__(self):
        super().__init__("datagov")
        self.session.headers.update({
            'Accept': 'application/json',
        })
    
    async def fetch_datasets(self) -> List[Dict[str, Any]]:
        """Fetch ML-relevant datasets from Data.gov."""
        all_datasets = {}  # Dedupe by ID
        
        logger.info("Starting Data.gov scraping with ML-focused queries...")
        
        # First, search by ML-specific queries
        for query in self.ML_QUERIES:
            try:
                datasets_from_query = await self._search_datasets(query, max_results=1000)
                for ds in datasets_from_query:
                    ds_id = ds.get('source', {}).get('platform_id', '')
                    if ds_id and ds_id not in all_datasets:
                        all_datasets[ds_id] = ds
                
                logger.info(f"Data.gov query '{query}': found {len(datasets_from_query)} datasets")
                
            except Exception as e:
                logger.error(f"Data.gov query '{query}' failed: {e}")
        
        # Then, search by ML-relevant tags
        for tag in self.ML_RELEVANT_TAGS:
            try:
                datasets_from_tag = await self._search_by_tag(tag, max_results=500)
                for ds in datasets_from_tag:
                    ds_id = ds.get('source', {}).get('platform_id', '')
                    if ds_id and ds_id not in all_datasets:
                        all_datasets[ds_id] = ds
                
            except Exception as e:
                logger.error(f"Data.gov tag '{tag}' failed: {e}")
        
        logger.info(f"Data.gov scraping complete: {len(all_datasets)} unique ML datasets")
        return list(all_datasets.values())
    
    async def _search_datasets(self, query: str, max_results: int = 1000) -> List[Dict[str, Any]]:
        """Search datasets by query string."""
        datasets = []
        start = 0
        rows = 100
        
        while start < max_results:
            try:
                url = f"{self.BASE_URL}/action/package_search"
                params = {
                    'q': query,
                    'start': start,
                    'rows': rows,
                    'fq': 'res_format:(CSV OR JSON OR XML OR TSV)',  # Only structured data
                }
                
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if not data.get('success'):
                    break
                
                results = data.get('result', {}).get('results', [])
                if not results:
                    break
                
                for item in results:
                    normalized = self.normalize_dataset(item)
                    if normalized:
                        # Apply strict ML filter
                        passed, score = ml_filter.filter_dataset(normalized)
                        if passed and score.ml_suitability >= 0.35:  # Higher threshold for Data.gov
                            normalized['ml_score'] = score.ml_suitability
                            normalized['detected_modality'] = score.modality
                            normalized['detected_tasks'] = score.potential_tasks
                            datasets.append(normalized)
                
                if len(results) < rows:
                    break
                
                start += rows
                
            except Exception as e:
                logger.error(f"Data.gov search error at start={start}: {e}")
                break
        
        return datasets
    
    async def _search_by_tag(self, tag: str, max_results: int = 500) -> List[Dict[str, Any]]:
        """Search datasets by tag."""
        datasets = []
        start = 0
        rows = 100
        
        while start < max_results:
            try:
                url = f"{self.BASE_URL}/action/package_search"
                params = {
                    'fq': f'tags:{tag} AND res_format:(CSV OR JSON)',
                    'start': start,
                    'rows': rows,
                }
                
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if not data.get('success'):
                    break
                
                results = data.get('result', {}).get('results', [])
                if not results:
                    break
                
                for item in results:
                    normalized = self.normalize_dataset(item)
                    if normalized:
                        passed, score = ml_filter.filter_dataset(normalized)
                        if passed and score.ml_suitability >= 0.35:
                            normalized['ml_score'] = score.ml_suitability
                            normalized['detected_modality'] = score.modality
                            normalized['detected_tasks'] = score.potential_tasks
                            datasets.append(normalized)
                
                if len(results) < rows:
                    break
                
                start += rows
                
            except Exception as e:
                logger.error(f"Data.gov tag search error: {e}")
                break
        
        return datasets
    
    def normalize_dataset(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize CKAN package to standard format."""
        try:
            name = raw_data.get('title', '') or raw_data.get('name', '')
            if not name:
                return None
            
            package_id = raw_data.get('id', '')
            
            # Extract resources (files)
            resources = raw_data.get('resources', [])
            formats = [r.get('format', '').lower() for r in resources if r.get('format')]
            
            # Calculate total size
            total_size = sum(r.get('size', 0) or 0 for r in resources)
            
            # Extract metadata
            source_metadata = {
                'package_id': package_id,
                'organization': raw_data.get('organization', {}).get('title') if raw_data.get('organization') else None,
                'tags': [t.get('name') for t in raw_data.get('tags', [])],
                'groups': [g.get('title') for g in raw_data.get('groups', [])],
                'formats': formats,
                'num_resources': len(resources),
                'total_size_bytes': total_size,
                'metadata_created': raw_data.get('metadata_created'),
                'metadata_modified': raw_data.get('metadata_modified'),
                'license_id': raw_data.get('license_id'),
                'license_title': raw_data.get('license_title'),
            }
            
            description = raw_data.get('notes', '') or ''
            
            # Infer domain and modality
            tags = source_metadata.get('tags', [])
            domain = self._infer_domain(name, description, tags)
            modality = self._infer_modality(name, description, formats)
            
            return self.create_standard_dataset(
                name=name,
                description=description[:2000],
                url=f"https://catalog.data.gov/dataset/{raw_data.get('name', package_id)}",
                platform_id=package_id,
                domain=domain,
                modality=modality,
                metadata=source_metadata,
                license=source_metadata.get('license_title'),
                file_size_gb=total_size / (1024**3) if total_size else None
            )
            
        except Exception as e:
            logger.error(f"Error normalizing Data.gov dataset: {e}")
            return None
    
    def _infer_domain(self, name: str, description: str, tags: List[str]) -> str:
        """Infer domain from metadata."""
        text = f"{name} {description} {' '.join(tags)}".lower()
        
        if any(kw in text for kw in ['health', 'medical', 'disease', 'hospital']):
            return 'Healthcare'
        elif any(kw in text for kw in ['climate', 'weather', 'environment', 'air quality']):
            return 'Environmental Science'
        elif any(kw in text for kw in ['finance', 'economic', 'market', 'stock']):
            return 'Finance'
        elif any(kw in text for kw in ['transport', 'traffic', 'vehicle', 'road']):
            return 'Transportation'
        elif any(kw in text for kw in ['education', 'school', 'student', 'university']):
            return 'Education'
        elif any(kw in text for kw in ['energy', 'power', 'electricity', 'solar']):
            return 'Energy'
        else:
            return 'General'
    
    def _infer_modality(self, name: str, description: str, formats: List[str]) -> str:
        """Infer modality from metadata and file formats."""
        text = f"{name} {description}".lower()
        formats_str = ' '.join(formats).lower()
        
        # Check file formats first
        if any(fmt in formats_str for fmt in ['jpg', 'jpeg', 'png', 'gif', 'tiff', 'geotiff']):
            return 'image'
        elif any(fmt in formats_str for fmt in ['mp3', 'wav', 'ogg', 'audio']):
            return 'audio'
        elif any(fmt in formats_str for fmt in ['mp4', 'avi', 'video']):
            return 'video'
        elif any(fmt in formats_str for fmt in ['shp', 'geojson', 'kml']):
            return 'geospatial'
        
        # Check content keywords
        if any(kw in text for kw in ['image', 'photo', 'satellite', 'aerial']):
            return 'image'
        elif any(kw in text for kw in ['time series', 'temporal', 'sequential']):
            return 'timeseries'
        elif any(kw in text for kw in ['text', 'document', 'corpus']):
            return 'text'
        else:
            return 'tabular'  # Default for Data.gov
