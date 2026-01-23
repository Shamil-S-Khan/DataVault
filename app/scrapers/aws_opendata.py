"""
AWS Open Data Registry scraper.
Source: https://registry.opendata.aws/
Uses the public YAML/JSON registry files.
"""
from typing import Dict, List, Any
import logging
from app.scrapers.base import BaseScraper
from app.scrapers.ml_filter import ml_filter

logger = logging.getLogger(__name__)


class AWSOpenDataScraper(BaseScraper):
    """Scraper for AWS Open Data Registry."""
    
    # AWS maintains a GitHub repo with all dataset metadata as YAML files
    GITHUB_API_URL = "https://api.github.com/repos/awslabs/open-data-registry/contents/datasets"
    RAW_CONTENT_URL = "https://raw.githubusercontent.com/awslabs/open-data-registry/main/datasets"
    
    def __init__(self):
        super().__init__("aws_opendata")
        self.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'DataVault/1.0',
        })
    
    async def fetch_datasets(self) -> List[Dict[str, Any]]:
        """Fetch datasets from AWS Open Data Registry via GitHub."""
        datasets = []
        
        logger.info("Starting AWS Open Data scraping via GitHub API...")
        
        try:
            # Fetch the list of YAML files in the datasets directory
            response = self.session.get(self.GITHUB_API_URL, timeout=30)
            response.raise_for_status()
            files = response.json()
            
            # Filter for YAML files only  
            yaml_files = [f for f in files if f.get('name', '').endswith('.yaml')]
            logger.info(f"AWS Open Data: Found {len(yaml_files)} dataset files")
            
            # Limit to avoid excessive API calls
            max_datasets = 500
            for i, file_info in enumerate(yaml_files[:max_datasets]):
                try:
                    filename = file_info.get('name', '')
                    slug = filename.replace('.yaml', '')
                    
                    # Create a basic dataset entry from filename
                    # (Parsing full YAML would require additional requests)
                    name = slug.replace('-', ' ').title()
                    
                    normalized = self._create_dataset_from_slug(slug, name)
                    if normalized:
                        # AWS Open Data has good ML datasets, assign reasonable default
                        normalized['ml_score'] = 0.5
                        # Note: modality is already set by _infer_domain_modality in _create_dataset_from_slug
                        normalized['detected_tasks'] = []
                        datasets.append(normalized)
                        
                except Exception as e:
                    logger.warning(f"Error processing AWS dataset {file_info.get('name')}: {e}")
                    continue
            
            logger.info(f"AWS Open Data: {len(datasets)} datasets collected")
            
        except Exception as e:
            logger.error(f"AWS Open Data GitHub API error: {e}")
        
        return datasets
    
    def _create_dataset_from_slug(self, slug: str, name: str) -> Dict[str, Any]:
        """Create a dataset entry from the slug and name."""
        canonical_name = f"aws-{slug}"
        
        return self.create_standard_dataset(
            name=canonical_name,
            description=f"AWS Open Data dataset: {name}. View full details at https://registry.opendata.aws/{slug}",
            url=f"https://registry.opendata.aws/{slug}",
            platform_id=slug,
            domain='General',
            modality='tabular',
            metadata={
                'display_name': name,
                'source_type': 'aws_opendata',
            }
        )
    
    def normalize_dataset(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize AWS Open Data dataset to standard format."""
        try:
            name = raw_data.get('Name', '') or raw_data.get('name', '')
            if not name:
                return None
            
            slug = raw_data.get('Slug', '') or name.lower().replace(' ', '-')
            
            # Create unique canonical_name with platform prefix
            canonical_name = f"aws-{slug}"
            
            # Extract metadata
            metadata = {
                'managed_by': raw_data.get('ManagedBy'),
                'update_frequency': raw_data.get('UpdateFrequency'),
                'license': raw_data.get('License'),
                'documentation': raw_data.get('Documentation'),
                'aws_region': raw_data.get('Region'),
                'resources': raw_data.get('Resources', []),
            }
            
            # Build description from multiple fields
            description = raw_data.get('Description', '')
            if not description:
                description = f"AWS Open Data: {name}"
            
            # Extract tags
            tags = raw_data.get('Tags', []) or []
            
            # Infer domain and modality
            domain, modality = self._infer_domain_modality(name, description, tags)
            
            dataset = self.create_standard_dataset(
                name=canonical_name,
                description=description[:2000],  # Limit description length
                url=f"https://registry.opendata.aws/{slug}",
                platform_id=slug,
                domain=domain,
                modality=modality,
                metadata=metadata,
                license=metadata.get('license')
            )
            
            # Add display_name to preserve original title
            dataset['display_name'] = name
            
            return dataset
            
        except Exception as e:
            logger.error(f"Error normalizing AWS Open Data dataset: {e}")
            return None
    
    def _infer_domain_modality(self, name: str, description: str, tags: List[str]) -> tuple:
        """Infer domain and modality from metadata."""
        text = f"{name} {description} {' '.join(tags)}".lower()
        
        # Domain detection
        if any(kw in text for kw in ['satellite', 'imagery', 'image', 'geospatial', 'vision']):
            domain = 'Computer Vision'
            modality = 'image'
        elif any(kw in text for kw in ['text', 'nlp', 'language', 'corpus', 'wikipedia']):
            domain = 'Natural Language Processing'
            modality = 'text'
        elif any(kw in text for kw in ['genomic', 'genetic', 'dna', 'protein', 'biology']):
            domain = 'Bioinformatics'
            modality = 'tabular'
        elif any(kw in text for kw in ['weather', 'climate', 'forecast', 'temporal']):
            domain = 'Time Series'
            modality = 'tabular'
        elif any(kw in text for kw in ['audio', 'speech', 'sound', 'music']):
            domain = 'Audio'
            modality = 'audio'
        else:
            domain = 'General'
            modality = 'tabular'
        
        return domain, modality
