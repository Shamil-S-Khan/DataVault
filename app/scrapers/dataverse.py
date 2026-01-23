"""
Harvard Dataverse scraper.
API: https://dataverse.harvard.edu/api/
Major academic data repository.
"""
from typing import Dict, List, Any
import logging
from app.scrapers.base import BaseScraper
from app.scrapers.ml_filter import ml_filter

logger = logging.getLogger(__name__)


class HarvardDataverseScraper(BaseScraper):
    """Scraper for Harvard Dataverse."""
    
    BASE_URL = "https://dataverse.harvard.edu/api"
    
    # ML-focused search queries
    ML_QUERIES = [
        'machine learning',
        'deep learning dataset',
        'neural network training',
        'computer vision',
        'natural language processing',
        'classification benchmark',
        'image recognition',
        'text classification',
        'audio dataset',
        'sensor data',
    ]
    
    def __init__(self):
        super().__init__("harvard_dataverse")
        self.session.headers.update({
            'Accept': 'application/json',
        })
    
    async def fetch_datasets(self) -> List[Dict[str, Any]]:
        """Fetch ML-relevant datasets from Harvard Dataverse."""
        all_datasets = {}
        
        logger.info("Starting Harvard Dataverse scraping...")
        
        for query in self.ML_QUERIES:
            try:
                datasets_from_query = await self._search_datasets(query, max_results=500)
                for ds in datasets_from_query:
                    ds_id = ds.get('source', {}).get('platform_id', '')
                    if ds_id and ds_id not in all_datasets:
                        all_datasets[ds_id] = ds
                
                logger.info(f"Dataverse query '{query}': {len(datasets_from_query)} datasets")
                
            except Exception as e:
                logger.error(f"Dataverse query '{query}' failed: {e}")
        
        logger.info(f"Harvard Dataverse complete: {len(all_datasets)} unique datasets")
        return list(all_datasets.values())
    
    async def _search_datasets(self, query: str, max_results: int = 500) -> List[Dict[str, Any]]:
        """Search datasets by query."""
        datasets = []
        start = 0
        per_page = 100
        
        while start < max_results:
            try:
                url = f"{self.BASE_URL}/search"
                params = {
                    'q': query,
                    'type': 'dataset',
                    'start': start,
                    'per_page': per_page,
                }
                
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                items = data.get('data', {}).get('items', [])
                if not items:
                    break
                
                for item in items:
                    normalized = self.normalize_dataset(item)
                    if normalized:
                        passed, score = ml_filter.filter_dataset(normalized)
                        if passed:
                            normalized['ml_score'] = score.ml_suitability
                            normalized['detected_modality'] = score.modality
                            normalized['detected_tasks'] = score.potential_tasks
                            datasets.append(normalized)
                
                if len(items) < per_page:
                    break
                
                start += per_page
                
            except Exception as e:
                logger.error(f"Dataverse search error: {e}")
                break
        
        return datasets
    
    def normalize_dataset(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize Dataverse dataset to standard format."""
        try:
            name = raw_data.get('name', '')
            if not name:
                return None
            
            global_id = raw_data.get('global_id', '')
            url = raw_data.get('url', '')
            
            # Extract metadata
            source_metadata = {
                'global_id': global_id,
                'publisher': raw_data.get('publisher'),
                'published_at': raw_data.get('published_at'),
                'subjects': raw_data.get('subjects', []),
                'authors': raw_data.get('authors', []),
                'citation': raw_data.get('citationHtml'),
            }
            
            description = raw_data.get('description', '') or ''
            subjects = source_metadata.get('subjects', [])
            
            # Infer domain and modality
            domain = self._infer_domain(name, description, subjects)
            modality = self._infer_modality(name, description, subjects)
            
            return self.create_standard_dataset(
                name=name,
                description=description[:2000],
                url=url or f"https://dataverse.harvard.edu/dataset.xhtml?persistentId={global_id}",
                platform_id=global_id,
                domain=domain,
                modality=modality,
                metadata=source_metadata
            )
            
        except Exception as e:
            logger.error(f"Error normalizing Dataverse dataset: {e}")
            return None
    
    def _infer_domain(self, name: str, description: str, subjects: List[str]) -> str:
        """Infer domain from metadata."""
        text = f"{name} {description} {' '.join(subjects)}".lower()
        
        if any(kw in text for kw in ['social', 'survey', 'poll', 'opinion']):
            return 'Social Sciences'
        elif any(kw in text for kw in ['medical', 'health', 'clinical', 'disease']):
            return 'Healthcare'
        elif any(kw in text for kw in ['environment', 'climate', 'ecology']):
            return 'Environmental Science'
        elif any(kw in text for kw in ['economic', 'finance', 'market']):
            return 'Economics'
        elif any(kw in text for kw in ['biology', 'genomic', 'gene']):
            return 'Bioinformatics'
        else:
            return 'General'
    
    def _infer_modality(self, name: str, description: str, subjects: List[str]) -> str:
        """Infer modality from metadata."""
        text = f"{name} {description} {' '.join(subjects)}".lower()
        
        if any(kw in text for kw in ['image', 'photo', 'picture', 'visual', 'scan', 'x-ray']):
            return 'image'
        elif any(kw in text for kw in ['audio', 'sound', 'speech', 'acoustic']):
            return 'audio'
        elif any(kw in text for kw in ['video', 'movie', 'film']):
            return 'video'
        elif any(kw in text for kw in ['text', 'corpus', 'language', 'nlp', 'document']):
            return 'text'
        elif any(kw in text for kw in ['time series', 'temporal', 'sequential']):
            return 'timeseries'
        elif any(kw in text for kw in ['graph', 'network']):
            return 'graph'
        else:
            return 'tabular'
