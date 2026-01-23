"""
Papers with Code dataset scraper.
Uses API with HTML scraping fallback when API is unavailable.
"""
from typing import Dict, List, Any, Optional
import logging
import time
import re
from app.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)


class PapersWithCodeScraper(BaseScraper):
    """Scraper for Papers with Code datasets."""
    
    BASE_URL = "https://paperswithcode.com/api/v1"
    WEBSITE_URL = "https://paperswithcode.com"
    
    def __init__(self):
        super().__init__("papers_with_code")
        self.session.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
        })
    
    async def fetch_datasets(self) -> List[Dict[str, Any]]:
        """Fetch datasets from Papers with Code - API first, then HTML fallback."""
        # Try API first
        datasets = await self._fetch_via_api()
        
        if len(datasets) == 0:
            logger.info("API returned no results, trying HTML scraping...")
            datasets = await self._fetch_via_html()
        
        if len(datasets) == 0:
            logger.warning("PWC scraper returned 0 datasets - all methods failed")
        else:
            logger.info(f"PWC scraping complete: {len(datasets)} datasets")
        
        return datasets
    
    async def _fetch_via_api(self) -> List[Dict[str, Any]]:
        """Try to fetch datasets via the API."""
        datasets = []
        page = 1
        max_pages = 30
        
        logger.info("Trying PWC API...")
        
        while page <= max_pages:
            try:
                url = f"{self.BASE_URL}/datasets/"
                params = {'page': page, 'items_per_page': 100}
                
                response = self.session.get(url, params=params, timeout=30)
                
                content_type = response.headers.get('content-type', '')
                if 'application/json' not in content_type:
                    logger.warning(f"PWC API returned {content_type} - switching to HTML")
                    break
                
                response.raise_for_status()
                data = response.json()
                
                if not data.get('results'):
                    break
                
                for item in data['results']:
                    normalized = self.normalize_dataset(item)
                    if normalized:
                        datasets.append(normalized)
                
                logger.info(f"PWC API page {page}: {len(data['results'])} datasets")
                
                if not data.get('next'):
                    break
                
                page += 1
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"PWC API error: {e}")
                break
        
        return datasets
    
    async def _fetch_via_html(self) -> List[Dict[str, Any]]:
        """Fallback: Scrape datasets from HTML pages."""
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            logger.error("BeautifulSoup not installed - cannot use HTML fallback")
            return []
        
        datasets = []
        page = 1
        max_pages = 50  # More pages for HTML scraping
        
        logger.info("Starting PWC HTML scraping...")
        
        while page <= max_pages:
            try:
                url = f"{self.WEBSITE_URL}/datasets"
                params = {'page': page}
                
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Find dataset cards - PWC uses different structures
                dataset_cards = soup.select('.dataset-card, .paper-card, .row.infinite-container > .col-lg-3, .row > .col-lg-3.col-md-4')
                
                if not dataset_cards:
                    # Try alternative selector
                    dataset_cards = soup.select('a[href^="/dataset/"]')
                    if not dataset_cards:
                        logger.info(f"No more datasets found on page {page}")
                        break
                
                page_count = 0
                for card in dataset_cards:
                    try:
                        dataset = self._parse_html_card(card, soup)
                        if dataset:
                            datasets.append(dataset)
                            page_count += 1
                    except Exception as e:
                        logger.debug(f"Error parsing card: {e}")
                        continue
                
                if page_count == 0:
                    break
                
                logger.info(f"PWC HTML page {page}: {page_count} datasets (total: {len(datasets)})")
                
                # Check for next page
                next_link = soup.select_one('a[rel="next"], .pagination .next a, a:contains("Next")')
                if not next_link:
                    break
                
                page += 1
                time.sleep(1)  # Be respectful with HTML scraping
                
            except Exception as e:
                logger.error(f"PWC HTML scraping error on page {page}: {e}")
                break
        
        logger.info(f"PWC HTML scraping complete: {len(datasets)} datasets")
        return datasets
    
    def _parse_html_card(self, card, soup) -> Optional[Dict[str, Any]]:
        """Parse a dataset card from HTML."""
        try:
            # Get dataset link and name
            link = card.get('href') if card.name == 'a' else None
            if not link:
                link_elem = card.select_one('a[href^="/dataset/"]')
                if link_elem:
                    link = link_elem.get('href')
            
            if not link or '/dataset/' not in link:
                return None
            
            # Extract dataset slug from URL
            slug = link.replace('/dataset/', '').strip('/')
            if not slug:
                return None
            
            # Get name
            name_elem = card.select_one('h1, h2, h3, h4, .card-title, .name')
            name = name_elem.get_text(strip=True) if name_elem else slug.replace('-', ' ').title()
            
            # Get description
            desc_elem = card.select_one('.card-text, .description, p')
            description = desc_elem.get_text(strip=True) if desc_elem else ''
            
            # Get paper count if available
            paper_count = 0
            paper_elem = card.select_one('.badge, .paper-count, span:contains("paper")')
            if paper_elem:
                text = paper_elem.get_text()
                numbers = re.findall(r'\d+', text)
                if numbers:
                    paper_count = int(numbers[0])
            
            url = f"{self.WEBSITE_URL}{link}" if link.startswith('/') else link
            
            metadata = {
                'paper_count': paper_count,
                'full_name': name,
                'homepage': url,
            }
            
            domain = self._infer_domain(description, name)
            modality = self._infer_modality(description, name)
            
            return self.create_standard_dataset(
                name=name,
                description=description,
                url=url,
                platform_id=slug,
                domain=domain,
                modality=modality,
                metadata=metadata
            )
            
        except Exception as e:
            logger.debug(f"Error parsing HTML card: {e}")
            return None
    
    def normalize_dataset(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize Papers with Code dataset to standard format."""
        try:
            name = raw_data.get('name', '')
            if not name:
                return None
            
            metadata = {
                'paper_count': raw_data.get('paper_count', 0),
                'full_name': raw_data.get('full_name'),
                'homepage': raw_data.get('homepage'),
                'variants': raw_data.get('variants', [])
            }
            
            description = raw_data.get('description', '')
            domain = self._infer_domain(description, name)
            modality = self._infer_modality(description, name)
            
            return self.create_standard_dataset(
                name=name,
                description=description,
                url=raw_data.get('url', f"https://paperswithcode.com/dataset/{name}"),
                platform_id=str(raw_data.get('id', name)),
                domain=domain,
                modality=modality,
                metadata=metadata
            )
            
        except Exception as e:
            logger.error(f"Error normalizing dataset: {e}")
            return None
    
    def _infer_domain(self, description: str, name: str) -> str:
        """Infer dataset domain from description and name."""
        text = (description + " " + name).lower()
        
        if any(kw in text for kw in ['image', 'vision', 'visual', 'object detection', 'segmentation', 'face']):
            return 'Computer Vision'
        elif any(kw in text for kw in ['nlp', 'text', 'language', 'translation', 'sentiment', 'question']):
            return 'Natural Language Processing'
        elif any(kw in text for kw in ['audio', 'speech', 'sound', 'music', 'voice']):
            return 'Audio'
        elif any(kw in text for kw in ['graph', 'network', 'node', 'edge', 'knowledge']):
            return 'Graph'
        elif any(kw in text for kw in ['time series', 'temporal', 'forecasting', 'stock']):
            return 'Time Series'
        elif any(kw in text for kw in ['medical', 'health', 'clinical', 'doctor', 'patient']):
            return 'Healthcare'
        elif any(kw in text for kw in ['robot', 'control', 'reinforcement', 'game', 'atari']):
            return 'Reinforcement Learning'
        elif any(kw in text for kw in ['tabular', 'structured', 'classification', 'regression']):
            return 'Tabular'
        else:
            return 'General'
    
    def _infer_modality(self, description: str, name: str) -> str:
        """Infer dataset modality from description and name."""
        text = (description + " " + name).lower()
        
        modalities = []
        if 'image' in text or 'visual' in text or 'photo' in text:
            modalities.append('image')
        if 'text' in text or 'language' in text or 'corpus' in text:
            modalities.append('text')
        if 'audio' in text or 'speech' in text or 'sound' in text:
            modalities.append('audio')
        if 'video' in text or 'clip' in text:
            modalities.append('video')
        if 'graph' in text or 'network' in text:
            modalities.append('graph')
        if 'tabular' in text or 'table' in text or 'csv' in text:
            modalities.append('tabular')
        
        if len(modalities) == 0:
            return 'multimodal'
        elif len(modalities) == 1:
            return modalities[0]
        else:
            return 'multimodal'

