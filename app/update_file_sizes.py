"""
Script to update file sizes for HuggingFace datasets.
Run inside Docker container.
"""
import requests
import os
import time
from pymongo import MongoClient

def format_size(size_bytes):
    """Format size in human readable format."""
    if size_bytes is None:
        return None
    
    if size_bytes >= 1024 ** 3:
        return f"{size_bytes / (1024 ** 3):.2f} GB"
    elif size_bytes >= 1024 ** 2:
        return f"{size_bytes / (1024 ** 2):.2f} MB"
    elif size_bytes >= 1024:
        return f"{size_bytes / 1024:.2f} KB"
    else:
        return f"{size_bytes} B"

def update_file_sizes(limit=500):
    """Update file sizes for HuggingFace datasets."""
    # Get MongoDB URI from environment
    uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
    db_name = os.getenv('MONGODB_DB_NAME', 'datavault')
    
    print(f"Connecting to MongoDB: {uri}")
    client = MongoClient(uri)
    db = client[db_name]
    
    # Find datasets with null file size
    query = {
        'source.platform': 'huggingface',
        '$or': [
            {'size.file_size_bytes': None},
            {'size.file_size_bytes': {'$exists': False}}
        ]
    }
    
    datasets = list(db.datasets.find(query).limit(limit))
    print(f"Found {len(datasets)} datasets to update")
    
    updated = 0
    errors = 0
    
    for i, dataset in enumerate(datasets):
        platform_id = dataset.get('source', {}).get('platform_id')
        if not platform_id:
            continue
        
        try:
            url = f'https://huggingface.co/api/datasets/{platform_id}'
            resp = requests.get(url, timeout=10)
            
            if resp.ok:
                data = resp.json()
                size_bytes = data.get('size')
                
                if size_bytes:
                    db.datasets.update_one(
                        {'_id': dataset['_id']},
                        {'$set': {
                            'size.file_size_bytes': int(size_bytes),
                            'size.file_size_gb': float(size_bytes) / (1024 ** 3)
                        }}
                    )
                    print(f"[{i+1}/{len(datasets)}] {platform_id}: {format_size(size_bytes)}")
                    updated += 1
                else:
                    print(f"[{i+1}/{len(datasets)}] No size: {platform_id}")
            else:
                errors += 1
                
        except Exception as e:
            print(f"Error for {platform_id}: {e}")
            errors += 1
        
        time.sleep(0.2)
    
    print(f"\n=== Done: {updated} updated, {errors} errors ===")
    client.close()

if __name__ == '__main__':
    update_file_sizes()
