"""
HuggingFace Datasets scraper.
Uses the datasets library to list and fetch metadata.
"""
from typing import Dict, List, Any
import logging
from app.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)


class HuggingFaceScraper(BaseScraper):
    """Scraper for HuggingFace Datasets."""
    
    BASE_URL = "https://huggingface.co/api/datasets"
    
    def __init__(self):
        super().__init__("huggingface")
    
    async def fetch_datasets(self) -> List[Dict[str, Any]]:
        """Fetch datasets from HuggingFace API."""
        datasets = []
        
        try:
            # Fetch dataset list
            response = self.make_request(self.BASE_URL)
            data = response.json()
            
            for item in data:
                normalized = self.normalize_dataset(item)
                if normalized:
                    datasets.append(normalized)
            
        except Exception as e:
            logger.error(f"Error fetching HuggingFace datasets: {e}")
        
        return datasets
    
    def normalize_dataset(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize HuggingFace dataset to standard format."""
        try:
            dataset_id = raw_data.get('id', '')
            if not dataset_id:
                return None
            
            # Extract metadata
            metadata = {
                'downloads': raw_data.get('downloads', 0),
                'likes': raw_data.get('likes', 0),
                'tags': raw_data.get('tags', []),
                'author': raw_data.get('author'),
                'last_modified': raw_data.get('lastModified')
            }
            
            # Get description from card data if available
            description = raw_data.get('description', '')
            if not description and 'cardData' in raw_data:
                description = raw_data['cardData'].get('description', '')
            
            # If still no description, create one from metadata
            if not description or len(description) < 20:
                task_tags = [t for t in tags if 'task_categories:' in t or any(x in t for x in ['classification', 'detection', 'segmentation', 'translation'])]
                if task_tags:
                    description = f"HuggingFace dataset for {', '.join(task_tags[:3])}"
                else:
                    description = f"HuggingFace dataset: {dataset_id}"
            
            # Infer domain and modality from tags
            tags = raw_data.get('tags', [])
            domain = self._infer_domain_from_tags(tags)
            modality = self._infer_modality_from_tags(tags)
            
            # Extract size info - check if 'size' is directly available
            # HuggingFace provides 'size' (in bytes) or we can calculate from siblings
            file_size_gb = None
            samples = None
            
            # Try to get size from direct field (bytes)
            if 'size' in raw_data and raw_data['size']:
                try:
                    size_bytes = int(raw_data['size'])
                    file_size_gb = size_bytes / (1024 ** 3)  # Convert bytes to GB
                except (ValueError, TypeError):
                    pass
            
            # Try to get from cardData
            if file_size_gb is None and 'cardData' in raw_data:
                card_data = raw_data.get('cardData', {})
                if 'size_categories' in card_data:
                    # Parse size categories like "1M<n<10M", "10K<n<100K"
                    size_cat = card_data['size_categories']
                    if isinstance(size_cat, list) and size_cat:
                        size_cat = size_cat[0]
                    samples = self._parse_size_category(size_cat)
            
            return self.create_standard_dataset(
                name=dataset_id,  # Use dataset_id as canonical_name (already has platform prefix)
                description=description,
                url=f"https://huggingface.co/datasets/{dataset_id}",
                platform_id=dataset_id,
                domain=domain,
                modality=modality,
                metadata=metadata,
                file_size_gb=file_size_gb,
                samples=samples
            )
            # Note: HuggingFace dataset_id already serves as both canonical and display name
            
        except Exception as e:
            logger.error(f"Error normalizing HuggingFace dataset: {e}")
            return None
    
    def _parse_size_category(self, size_cat: str) -> int:
        """Parse HuggingFace size category to approximate sample count."""
        if not size_cat or not isinstance(size_cat, str):
            return None
        
        # Common patterns: "1K<n<10K", "10M<n<100M", "n<1K"
        size_cat = size_cat.upper()
        
        multipliers = {'K': 1000, 'M': 1000000, 'B': 1000000000}
        
        try:
            # Extract numbers from patterns like "1K<n<10K"
            import re
            numbers = re.findall(r'(\d+)([KMB])?', size_cat)
            if numbers:
                # Take the first number as minimum estimate
                num, suffix = numbers[0]
                multiplier = multipliers.get(suffix, 1) if suffix else 1
                return int(num) * multiplier
        except Exception:
            pass
        
        return None
    
    def _infer_domain_from_tags(self, tags: List[str]) -> str:
        """Infer domain from HuggingFace tags."""
        tags_lower = [tag.lower() for tag in tags]
        
        if any(tag in tags_lower for tag in ['image-classification', 'object-detection', 'image-segmentation']):
            return 'Computer Vision'
        elif any(tag in tags_lower for tag in ['text-classification', 'translation', 'summarization', 'question-answering']):
            return 'Natural Language Processing'
        elif any(tag in tags_lower for tag in ['audio-classification', 'automatic-speech-recognition']):
            return 'Audio'
        elif any(tag in tags_lower for tag in ['tabular-classification', 'tabular-regression']):
            return 'Tabular'
        else:
            return 'General'
    
    def _infer_modality_from_tags(self, tags: List[str]) -> str:
        """Infer modality from HuggingFace tags."""
        tags_lower = [tag.lower() for tag in tags]
        
        if 'image' in ' '.join(tags_lower):
            return 'image'
        elif 'text' in ' '.join(tags_lower):
            return 'text'
        elif 'audio' in ' '.join(tags_lower):
            return 'audio'
        elif 'video' in ' '.join(tags_lower):
            return 'video'
        elif 'tabular' in ' '.join(tags_lower):
            return 'tabular'
        else:
            return 'multimodal'
