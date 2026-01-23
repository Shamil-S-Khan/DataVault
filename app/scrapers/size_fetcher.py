"""
Comprehensive Dataset Size Fetcher.
Fetches both row counts and file sizes from multiple platforms:
- HuggingFace: API + parquet endpoints
- Kaggle: Web scraping for row counts
- OpenML: API for instances and size
"""
import asyncio
import httpx
import re
import logging
from typing import Dict, Any, Optional, Tuple
from motor.motor_asyncio import AsyncIOMotorClient
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class ComprehensiveSizeFetcher:
    """Fetches dataset sizes from multiple platforms."""
    
    def __init__(self, mongo_uri: str = "mongodb://mongodb:27017"):
        self.mongo_uri = mongo_uri
        self.client = None
        self.db = None
        
    async def connect(self):
        """Connect to MongoDB."""
        self.client = AsyncIOMotorClient(self.mongo_uri)
        self.db = self.client.datavault
        
    async def close(self):
        """Close connections."""
        if self.client:
            self.client.close()
    
    async def fetch_huggingface_size(
        self, 
        platform_id: str,
        http_client: httpx.AsyncClient
    ) -> Tuple[Optional[int], Optional[int]]:
        """
        Fetch row count and file size from HuggingFace.
        Returns: (row_count, file_size_bytes)
        """
        row_count = None
        file_size = None
        
        try:
            # Method 1: Try parquet endpoint for row counts
            url = f"https://huggingface.co/api/datasets/{platform_id}/parquet"
            resp = await http_client.get(url)
            
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, dict):
                    total_rows = 0
                    total_size = 0
                    for split_info in data.values():
                        if isinstance(split_info, list):
                            for p in split_info:
                                if isinstance(p, dict):
                                    total_rows += p.get('num_rows', 0)
                                    total_size += p.get('size', 0)
                    if total_rows > 0:
                        row_count = total_rows
                    if total_size > 0:
                        file_size = total_size
            
            # Method 2: Try main API for size_categories if rows still missing
            if row_count is None:
                url = f"https://huggingface.co/api/datasets/{platform_id}"
                resp = await http_client.get(url)
                
                if resp.status_code == 200:
                    data = resp.json()
                    
                    # Get file size from 'size' field
                    if file_size is None and 'size' in data:
                        try:
                            file_size = int(data['size'])
                        except (ValueError, TypeError):
                            pass
                    
                    # Get row estimate from size_categories
                    card_data = data.get('cardData', {})
                    if 'size_categories' in card_data:
                        size_cat = card_data['size_categories']
                        if isinstance(size_cat, list) and size_cat:
                            size_cat = size_cat[0]
                        row_count = self._parse_size_category(size_cat)
                    
                    # Try splits in cardData
                    dataset_info = card_data.get('dataset_info', {})
                    if isinstance(dataset_info, dict):
                        splits = dataset_info.get('splits', [])
                        if splits:
                            total = sum(s.get('num_examples', 0) for s in splits if isinstance(s, dict))
                            if total > 0:
                                row_count = total
                    elif isinstance(dataset_info, list):
                        for info in dataset_info:
                            splits = info.get('splits', [])
                            total = sum(s.get('num_examples', 0) for s in splits if isinstance(s, dict))
                            if total > 0:
                                row_count = total
                                break
                                
        except Exception as e:
            logger.error(f"HuggingFace fetch error for {platform_id}: {e}")
        
        return row_count, file_size
    
    def _parse_size_category(self, size_cat: str) -> Optional[int]:
        """Parse HuggingFace size category to row count estimate."""
        if not size_cat or not isinstance(size_cat, str):
            return None
        
        size_cat = size_cat.upper()
        multipliers = {'K': 1000, 'M': 1000000, 'B': 1000000000}
        
        try:
            numbers = re.findall(r'(\d+)([KMB])?', size_cat)
            if numbers:
                num, suffix = numbers[0]
                multiplier = multipliers.get(suffix, 1) if suffix else 1
                return int(num) * multiplier
        except Exception:
            pass
        return None
    
    async def fetch_kaggle_size(
        self, 
        platform_id: str,
        http_client: httpx.AsyncClient
    ) -> Tuple[Optional[int], Optional[int]]:
        """
        Scrape row count and file size from Kaggle dataset page.
        platform_id format: owner/dataset-name
        Returns: (row_count, file_size_bytes)
        """
        row_count = None
        file_size = None
        
        try:
            url = f"https://www.kaggle.com/datasets/{platform_id}"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            resp = await http_client.get(url, headers=headers, follow_redirects=True)
            
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'html.parser')
                
                # Look for size info in the page
                # Kaggle typically shows: "XX files, XX.X MB" or "XX rows"
                text = soup.get_text()
                
                # Try to find row count patterns
                row_patterns = [
                    r'(\d+(?:,\d+)*)\s*rows?',
                    r'(\d+(?:\.\d+)?)\s*([KMB])\s*rows?',
                ]
                
                for pattern in row_patterns:
                    match = re.search(pattern, text, re.IGNORECASE)
                    if match:
                        if len(match.groups()) == 1:
                            row_count = int(match.group(1).replace(',', ''))
                        else:
                            num = float(match.group(1))
                            suffix = match.group(2).upper()
                            multipliers = {'K': 1000, 'M': 1000000, 'B': 1000000000}
                            row_count = int(num * multipliers.get(suffix, 1))
                        break
                
                # Try to find file size patterns
                size_patterns = [
                    r'(\d+(?:\.\d+)?)\s*(KB|MB|GB|TB)',
                    r'(\d+(?:,\d+)*)\s*bytes?',
                ]
                
                for pattern in size_patterns:
                    match = re.search(pattern, text, re.IGNORECASE)
                    if match:
                        if 'byte' in pattern.lower():
                            file_size = int(match.group(1).replace(',', ''))
                        else:
                            num = float(match.group(1))
                            unit = match.group(2).upper()
                            multipliers = {'KB': 1024, 'MB': 1024**2, 'GB': 1024**3, 'TB': 1024**4}
                            file_size = int(num * multipliers.get(unit, 1))
                        break
                        
        except Exception as e:
            logger.error(f"Kaggle fetch error for {platform_id}: {e}")
        
        return row_count, file_size
    
    async def fetch_openml_size(
        self, 
        platform_id: str,
        http_client: httpx.AsyncClient
    ) -> Tuple[Optional[int], Optional[int]]:
        """
        Fetch row count and file size from OpenML API.
        Returns: (row_count, file_size_bytes)
        """
        row_count = None
        file_size = None
        
        try:
            # Extract numeric ID
            openml_id = platform_id.split('/')[-1] if '/' in platform_id else platform_id
            
            url = f"https://www.openml.org/api/v1/json/data/{openml_id}"
            resp = await http_client.get(url)
            
            if resp.status_code == 200:
                data = resp.json()
                info = data.get('data_set_description', {})
                
                # Get row count
                if 'NumberOfInstances' in info:
                    try:
                        row_count = int(info['NumberOfInstances'])
                    except (ValueError, TypeError):
                        pass
                
                # OpenML doesn't always have file size directly
                # But we can estimate from features * instances * 8 bytes
                if 'NumberOfFeatures' in info and row_count:
                    try:
                        features = int(info['NumberOfFeatures'])
                        # Rough estimate: 8 bytes per value
                        file_size = row_count * features * 8
                    except (ValueError, TypeError):
                        pass
                        
        except Exception as e:
            logger.error(f"OpenML fetch error for {platform_id}: {e}")
        
        return row_count, file_size
    
    async def update_all_datasets(self) -> Dict[str, Any]:
        """Update all datasets with row counts and file sizes."""
        await self.connect()
        
        results = {
            'updated': [],
            'errors': [],
            'skipped': 0
        }
        
        try:
            cursor = self.db.datasets.find({})
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                async for dataset in cursor:
                    platform = dataset.get('source', {}).get('platform', '')
                    platform_id = dataset.get('source', {}).get('platform_id', '')
                    name = dataset.get('canonical_name', 'Unknown')
                    
                    current_samples = dataset.get('size', {}).get('samples')
                    current_file_size = dataset.get('size', {}).get('file_size_bytes')
                    
                    # Skip if already complete
                    if current_samples and current_file_size:
                        results['skipped'] += 1
                        continue
                    
                    if not platform_id:
                        continue
                    
                    row_count = None
                    file_size = None
                    
                    try:
                        if platform == 'huggingface':
                            row_count, file_size = await self.fetch_huggingface_size(platform_id, client)
                        elif platform == 'kaggle':
                            row_count, file_size = await self.fetch_kaggle_size(platform_id, client)
                        elif platform == 'openml':
                            row_count, file_size = await self.fetch_openml_size(platform_id, client)
                        
                        # Update if we got new data
                        update_fields = {}
                        if row_count and not current_samples:
                            update_fields['size.samples'] = row_count
                        if file_size and not current_file_size:
                            update_fields['size.file_size_bytes'] = file_size
                        
                        if update_fields:
                            await self.db.datasets.update_one(
                                {'_id': dataset['_id']},
                                {'$set': update_fields}
                            )
                            results['updated'].append({
                                'name': name,
                                'platform': platform,
                                'samples': row_count,
                                'file_size_bytes': file_size
                            })
                            logger.info(f"Updated {name}: {row_count} rows, {file_size} bytes")
                        
                        # Rate limiting
                        await asyncio.sleep(0.3)
                        
                    except Exception as e:
                        results['errors'].append({
                            'name': name,
                            'error': str(e)
                        })
                        
        finally:
            await self.close()
        
        return results


# API endpoint function
async def run_size_update():
    """Run the size update process."""
    fetcher = ComprehensiveSizeFetcher()
    results = await fetcher.update_all_datasets()
    return results


if __name__ == "__main__":
    results = asyncio.run(run_size_update())
    print(f"Updated: {len(results['updated'])}")
    print(f"Errors: {len(results['errors'])}")
    print(f"Skipped: {results['skipped']}")
