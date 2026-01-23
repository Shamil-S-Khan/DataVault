"""
OpenML dataset scraper.
API: https://www.openml.org/api/v1/
Public API with no authentication required for dataset listing.
"""
from typing import Dict, List, Any
import logging
from app.scrapers.base import BaseScraper
from app.scrapers.ml_filter import ml_filter

logger = logging.getLogger(__name__)


class OpenMLScraper(BaseScraper):
    """Scraper for OpenML datasets."""
    
    BASE_URL = "https://www.openml.org/api/v1/json"
    
    def __init__(self):
        super().__init__("openml")
        self.session.headers.update({
            'Accept': 'application/json',
        })
    
    async def fetch_datasets(self) -> List[Dict[str, Any]]:
        """Fetch datasets from OpenML API."""
        datasets = []
        offset = 0
        limit = 100  # OpenML pagination limit
        max_datasets = 10000  # Safety limit
        
        logger.info("Starting OpenML scraping...")
        
        while offset < max_datasets:
            try:
                url = f"{self.BASE_URL}/data/list"
                params = {
                    'offset': offset,
                    'limit': limit,
                    'status': 'active',
                }
                
                response = self.session.get(url, params=params, timeout=30)
                
                if response.status_code == 412:
                    logger.info("OpenML: No more datasets available")
                    break
                
                response.raise_for_status()
                data = response.json()
                
                dataset_list = data.get('data', {}).get('dataset', [])
                if not dataset_list:
                    break
                
                for item in dataset_list:
                    normalized = self.normalize_dataset(item)
                    if normalized:
                        # OpenML datasets are inherently ML-suitable, so we include all
                        # with basic metadata enrichment
                        normalized['ml_score'] = 0.7  # Default good score for ML platform
                        normalized['detected_modality'] = 'tabular'  # OpenML is primarily tabular
                        normalized['detected_tasks'] = ['classification', 'regression']
                        datasets.append(normalized)
                
                logger.info(f"OpenML offset {offset}: {len(dataset_list)} fetched, {len(datasets)} total collected")
                
                if len(dataset_list) < limit:
                    break
                
                # Stop at 2000 datasets for reasonable scraping time
                if len(datasets) >= 2000:
                    logger.info("OpenML: Reached 2000 dataset limit")
                    break
                
                offset += limit
                
            except Exception as e:
                logger.error(f"OpenML API error at offset {offset}: {e}")
                break
        
        logger.info(f"OpenML scraping complete: {len(datasets)} datasets collected")
        return datasets

    
    def normalize_dataset(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize OpenML dataset to standard format."""
        try:
            dataset_id = raw_data.get('did')
            name = raw_data.get('name', '')
            if not name or not dataset_id:
                return None
            
            # Create unique canonical_name with platform prefix
            # Format: openml-{id}-{sanitized_name}
            sanitized_name = name.lower().replace(' ', '-').replace('/', '-')[:50]
            canonical_name = f"openml-{dataset_id}-{sanitized_name}"
            
            # Extract OpenML-specific metadata from the 'quality' list
            qualities = {q['name']: q['value'] for q in raw_data.get('quality', [])}
            
            metadata = {
                'downloads': 0,  # OpenML API doesn't provide this in the main listing
                'likes': 0,      # OpenML API doesn't provide this
                'num_instances': int(float(qualities.get('NumberOfInstances', 0))),
                'num_features': int(float(qualities.get('NumberOfFeatures', 0))),
                'num_classes': int(float(qualities.get('NumberOfClasses', 0))),
                'num_missing': int(float(qualities.get('NumberOfMissingValues', 0))),
                'format': raw_data.get('format', 'arff'),
                'version': raw_data.get('version'),
                'uploader': raw_data.get('uploader'),
                'upload_date': raw_data.get('upload_date'),
                'status': raw_data.get('status'),
            }
            
            # Get description from raw data, or create informative one
            description = raw_data.get('description', '')
            if not description or len(description) < 20:
                description = f"OpenML dataset '{name}' with {metadata['num_instances']} instances and {metadata['num_features']} features."
                if metadata['num_classes'] > 0:
                    description += f" Classification task with {metadata['num_classes']} classes."
            
            # Infer domain and modality based on task type or features
            domain = self._infer_domain(raw_data)
            modality = self._infer_modality(name, description, metadata)
            
            # Estimate size
            samples = metadata['num_instances']
            
            dataset = self.create_standard_dataset(
                name=canonical_name,
                description=description[:2000],
                url=f"https://www.openml.org/d/{dataset_id}",
                platform_id=str(dataset_id),
                domain=domain,
                modality=modality,
                metadata=metadata,
                samples=samples,
                trend_score=0.3  # Better baseline since popularity metrics are missing
            )
            
            # Add display_name to preserve original title
            dataset['display_name'] = name
            
            return dataset
            
        except Exception as e:
            logger.error(f"Error normalizing OpenML dataset: {e}")
            return None
    
    def _infer_domain(self, raw_data: Dict[str, Any]) -> str:
        """Infer domain from OpenML metadata."""
        name = (raw_data.get('name', '') or '').lower()
        
        if any(kw in name for kw in ['image', 'mnist', 'cifar', 'vision']):
            return 'Computer Vision'
        elif any(kw in name for kw in ['text', 'nlp', 'sentiment', 'tweet']):
            return 'Natural Language Processing'
        elif any(kw in name for kw in ['time', 'series', 'temporal', 'stock']):
            return 'Time Series'
        else:
            return 'Tabular'
    
    def _infer_modality(self, name: str, description: str, metadata: Dict[str, Any]) -> str:
        """Infer modality from OpenML metadata."""
        text = f"{name} {description}".lower()
        
        # OpenML is primarily tabular, but check for special cases
        if any(kw in text for kw in ['image', 'pixel', 'mnist', 'cifar', 'vision']):
            return 'image'
        elif any(kw in text for kw in ['text', 'word', 'document', 'nlp']):
            return 'text'
        elif any(kw in text for kw in ['time series', 'temporal', 'sequential']):
            return 'timeseries'
        else:
            return 'tabular'  # Default for OpenML
