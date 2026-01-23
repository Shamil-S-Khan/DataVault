"""
Zenodo dataset scraper.
API: https://zenodo.org/api/
Official REST API for scientific datasets.
"""
from typing import Dict, List, Any
import logging
from app.scrapers.base import BaseScraper
from app.scrapers.ml_filter import ml_filter

logger = logging.getLogger(__name__)


class ZenodoScraper(BaseScraper):
    """Scraper for Zenodo datasets."""
    
    BASE_URL = "https://zenodo.org/api"
    
    # ML/AI focused search queries to pre-filter
    ML_QUERIES = [
        'machine learning dataset',
        'deep learning',
        'neural network training',
        'computer vision benchmark',
        'nlp corpus',
        'image classification',
        'natural language processing',
        'speech recognition dataset',
        'text classification',
        'object detection',
        'sentiment analysis',
        'time series forecasting',
    ]
    
    def __init__(self):
        super().__init__("zenodo")
        self.session.headers.update({
            'Accept': 'application/json',
        })
    
    async def fetch_datasets(self) -> List[Dict[str, Any]]:
        """Fetch ML-relevant datasets from Zenodo."""
        all_datasets = {}  # Use dict to dedupe by DOI
        
        logger.info("Starting Zenodo scraping with ML-focused queries...")
        
        for query in self.ML_QUERIES:
            try:
                datasets_from_query = await self._fetch_query(query)
                for ds in datasets_from_query:
                    doi = ds.get('source', {}).get('platform_id', '')
                    if doi and doi not in all_datasets:
                        all_datasets[doi] = ds
                
                logger.info(f"Zenodo query '{query}': found {len(datasets_from_query)} datasets")
                
            except Exception as e:
                logger.error(f"Zenodo query '{query}' failed: {e}")
        
        logger.info(f"Zenodo scraping complete: {len(all_datasets)} unique datasets")
        return list(all_datasets.values())
    
    async def _fetch_query(self, query: str, max_results: int = 1000) -> List[Dict[str, Any]]:
        """Fetch datasets for a specific query."""
        datasets = []
        page = 1
        per_page = 10  # Zenodo API limits unauthenticated requests to 10 per page
        
        while len(datasets) < max_results:
            try:
                url = f"{self.BASE_URL}/records"
                params = {
                    'q': f'{query} AND resource_type.type:dataset',  # Use resource_type filter
                    'page': page,
                    'size': per_page,
                    'sort': 'bestmatch',
                }
                
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                hits = data.get('hits', {}).get('hits', [])
                if not hits:
                    break
                
                for item in hits:
                    normalized = self.normalize_dataset(item)
                    if normalized:
                        # Since we searched with ML queries, datasets are inherently ML-suitable
                        normalized['ml_score'] = 0.6  # Good score for ML-query results
                        _, modality = self._infer_domain_modality(
                            normalized.get('canonical_name', ''),
                            normalized.get('description', ''),
                            normalized.get('source', {}).get('source_metadata', {}).get('keywords', [])
                        )
                        normalized['detected_modality'] = modality
                        normalized['detected_tasks'] = ['classification', 'regression']
                        datasets.append(normalized)
                
                if len(hits) < per_page:
                    break
                
                page += 1
                
            except Exception as e:
                logger.error(f"Zenodo fetch error on page {page}: {e}")
                break
        
        return datasets

    
    def normalize_dataset(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize Zenodo record to standard format."""
        try:
            metadata = raw_data.get('metadata', {})
            
            title = metadata.get('title', '')
            if not title:
                return None
            
            doi = raw_data.get('doi', '') or metadata.get('doi', '')
            record_id = raw_data.get('id')
            
            # Create unique canonical_name with platform prefix
            # Prefer DOI for uniqueness, fallback to record_id
            if doi:
                # DOI format: 10.5281/zenodo.1234567 -> zenodo-10.5281-zenodo.1234567
                sanitized_doi = doi.replace('/', '-').replace('.', '-')[:80]
                canonical_name = f"zenodo-{sanitized_doi}"
            else:
                # Fallback to record_id with sanitized title
                sanitized_title = title.lower().replace(' ', '-').replace('/', '-')[:50]
                canonical_name = f"zenodo-{record_id}-{sanitized_title}"
            
            # Extract rich metadata
            source_metadata = {
                'doi': doi,
                'record_id': record_id,
                'version': metadata.get('version'),
                'publication_date': metadata.get('publication_date'),
                'access_right': metadata.get('access_right'),
                'license': metadata.get('license', {}).get('id') if isinstance(metadata.get('license'), dict) else metadata.get('license'),
                'creators': [c.get('name') for c in metadata.get('creators', [])],
                'keywords': metadata.get('keywords', []),
                'communities': [c.get('id') for c in metadata.get('communities', [])],
                'downloads': raw_data.get('stats', {}).get('downloads', 0),
                'views': raw_data.get('stats', {}).get('views', 0),
            }
            
            # Get file info for format detection
            files = raw_data.get('files', [])
            file_formats = [f.get('type', '').lower() for f in files if f.get('type')]
            total_size = sum(f.get('size', 0) for f in files)
            num_files = len(files)
            
            source_metadata['formats'] = file_formats
            source_metadata['total_size_bytes'] = total_size
            source_metadata['num_files'] = num_files
            
            description = metadata.get('description', '')
            if not description:
                description = f"Zenodo dataset: {title}"
            
            # Try to extract row count from description or metadata
            samples = self._extract_row_count(description, title)
            
            # Infer domain and modality
            keywords = metadata.get('keywords', [])
            domain, modality = self._infer_domain_modality(title, description, keywords)
            
            dataset = self.create_standard_dataset(
                name=canonical_name,
                description=self._clean_html(description)[:2000],
                url=f"https://zenodo.org/record/{record_id}",
                platform_id=doi or str(record_id),
                domain=domain,
                modality=modality,
                metadata=source_metadata,
                license=source_metadata.get('license'),
                file_size_gb=total_size / (1024**3) if total_size else None,
                samples=samples
            )
            
            # Add display_name to preserve original title
            dataset['display_name'] = title
            
            return dataset
            
        except Exception as e:
            logger.error(f"Error normalizing Zenodo record: {e}")
            return None
    
    def _clean_html(self, text: str) -> str:
        """Remove HTML tags from text."""
        import re
        clean = re.sub(r'<[^>]+>', '', text)
        return clean.strip()
    
    def _extract_row_count(self, description: str, title: str) -> int:
        """Extract row/sample count from description or title."""
        import re
        text = f"{title} {description}".lower()
        
        # Look for patterns like "1000 samples", "10k rows", "1M instances"
        patterns = [
            r'(\d+[\.,]?\d*)[\s]*(million|m|k|thousand)?[\s]*(samples|rows|instances|examples|records|observations)',
            r'(\d+[\.,]?\d*)[\s]*(million|m|k)?[\s]*data[\s]*(points|samples)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                num = float(match.group(1).replace(',', ''))
                multiplier = match.group(2)
                
                if multiplier in ['million', 'm']:
                    num *= 1_000_000
                elif multiplier in ['k', 'thousand']:
                    num *= 1_000
                
                return int(num)
        
        return None
    
    def _infer_domain_modality(self, title: str, description: str, keywords: List[str]) -> tuple:
        """Infer domain and modality from metadata."""
        text = f"{title} {description} {' '.join(keywords)}".lower()
        
        if any(kw in text for kw in ['image', 'vision', 'photo', 'picture', 'video']):
            return 'Computer Vision', 'image'
        elif any(kw in text for kw in ['text', 'nlp', 'language', 'corpus', 'annotation']):
            return 'Natural Language Processing', 'text'
        elif any(kw in text for kw in ['audio', 'speech', 'sound', 'music', 'acoustic']):
            return 'Audio', 'audio'
        elif any(kw in text for kw in ['time series', 'temporal', 'sensor', 'iot']):
            return 'Time Series', 'tabular'
        elif any(kw in text for kw in ['genomic', 'biology', 'medical', 'health']):
            return 'Bioinformatics', 'tabular'
        else:
            return 'General', 'tabular'
