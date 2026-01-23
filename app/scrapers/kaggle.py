"""
Kaggle dataset scraper using official Kaggle API.
Requires API credentials from kaggle.com
"""
from typing import Dict, List, Any
import logging
from app.scrapers.base import BaseScraper
from app.config import settings

logger = logging.getLogger(__name__)


class KaggleScraper(BaseScraper):
    """Scraper for Kaggle datasets."""
    
    BASE_URL = "https://www.kaggle.com/api/v1"
    
    def __init__(self):
        super().__init__("kaggle")
        
        # Set up authentication if credentials are available
        if settings.kaggle_username and settings.kaggle_key:
            self.session.auth = (settings.kaggle_username, settings.kaggle_key)
    
    async def fetch_datasets(self) -> List[Dict[str, Any]]:
        """Fetch datasets from Kaggle API."""
        datasets = []
        page = 1
        page_size = 20  # Kaggle API only returns 20 per page regardless of pageSize param
        max_pages = 50  # Safety limit (50 pages * 20 = 1000 datasets)
        
        logger.info("Starting Kaggle scraping...")
        
        while page <= max_pages:
            try:
                url = f"{self.BASE_URL}/datasets/list"
                params = {
                    'page': page,
                    'pageSize': page_size,
                    'sortBy': 'hottest'
                }
                
                response = self.make_request(url, params=params)
                data = response.json()
                
                if not data:
                    logger.info(f"Kaggle: No more datasets at page {page}")
                    break
                
                for item in data:
                    normalized = self.normalize_dataset(item)
                    if normalized:
                        datasets.append(normalized)
                
                logger.info(f"Kaggle page {page}: {len(data)} fetched, {len(datasets)} total")
                
                # Limit to first 1000 datasets
                if len(datasets) >= 1000:
                    logger.info("Kaggle: Reached 1000 dataset limit")
                    break
                
                # If we got fewer than expected, we've reached the end
                if len(data) < page_size:
                    logger.info(f"Kaggle: Reached end at page {page}")
                    break
                
                page += 1
                
            except Exception as e:
                logger.error(f"Error fetching Kaggle datasets page {page}: {e}")
                break
        
        logger.info(f"Kaggle scraping complete: {len(datasets)} datasets")
        return datasets
    
    def normalize_dataset(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize Kaggle dataset to standard format."""
        try:
            ref = raw_data.get('ref', '')
            if not ref:
                return None
            
            title = raw_data.get('title', ref.split('/')[-1])
            
            # Create unique canonical_name with platform prefix
            # ref format: owner/dataset-name
            canonical_name = f"kaggle-{ref.replace('/', '-')}"
            
            # Extract metadata
            metadata = {
                'downloads': raw_data.get('downloadCount', 0),
                'votes': raw_data.get('voteCount', 0),
                'usability_score': raw_data.get('usabilityRating', 0),
                'view_count': raw_data.get('viewCount', 0),
                'subtitle': raw_data.get('subtitle'),
                'creator_name': raw_data.get('creatorName'),
                'license_name': raw_data.get('licenseName'),
                'last_updated': raw_data.get('lastUpdated')
            }
            
            # Get file size
            file_size_bytes = raw_data.get('totalBytes', 0)
            file_size_gb = file_size_bytes / (1024 ** 3) if file_size_bytes else None
            
            # Get description with fallback to subtitle or title
            description = raw_data.get('description', '') or raw_data.get('subtitle', '') or title
            if len(description) < 20:
                description = f"{title} - Kaggle dataset by {metadata['creator_name']}"
            
            # Infer modality from title and description
            modality = self._infer_modality(title, description)
            
            dataset = self.create_standard_dataset(
                name=canonical_name,
                description=description[:2000],
                url=f"https://www.kaggle.com/datasets/{ref}",
                platform_id=ref,
                license=raw_data.get('licenseName'),
                file_size_gb=file_size_gb,
                modality=modality,
                metadata=metadata,
                samples=None  # Kaggle doesn't provide row count in listing API
            )
            
            # Add display_name to preserve original title
            dataset['display_name'] = title
            
            return dataset
            
        except Exception as e:
            logger.error(f"Error normalizing Kaggle dataset: {e}")
            return None
    
    def _infer_modality(self, title: str, description: str) -> str:
        """Infer dataset modality from title and description."""
        text = f"{title} {description}".lower()
        
        # Check for specific modality keywords
        if any(kw in text for kw in ['image', 'photo', 'picture', 'vision', 'x-ray', 'xray', 'scan', 'mnist', 'cifar', 'imagenet', 'coco']):
            return 'image'
        elif any(kw in text for kw in ['audio', 'sound', 'speech', 'music', 'voice', 'acoustic']):
            return 'audio'
        elif any(kw in text for kw in ['video', 'movie', 'clip', 'frame']):
            return 'video'
        elif any(kw in text for kw in ['text', 'nlp', 'language', 'corpus', 'tweet', 'comment', 'review', 'sentiment']):
            return 'text'
        elif any(kw in text for kw in ['time series', 'temporal', 'sequential', 'timeseries']):
            return 'timeseries'
        elif any(kw in text for kw in ['graph', 'network', 'node', 'edge']):
            return 'graph'
        else:
            return 'tabular'  # Default
