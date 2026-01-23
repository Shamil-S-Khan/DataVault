"""
Dataset API routes.
Endpoints for dataset discovery, search, and details.
"""
from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from typing import List, Optional
from datetime import datetime
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db.connection import get_database
from app.db.redis_client import redis_client
from app.db.models import Dataset
from app.llm.gemini_client import gemini_client
from app.config import settings
from app.ml.recommender import get_recommender
from app.ml.quality_scorer import quality_scorer
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/trending")
async def get_trending_datasets(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100),
    domain: Optional[str] = None,
    modality: Optional[str] = None,
    platform: Optional[str] = None,
    quality: Optional[str] = None,
    search: Optional[str] = None,
    sort_by: Optional[str] = Query("trending", regex="^(trending|downloads|likes)$"),
    sort_order: Optional[str] = Query("desc", regex="^(asc|desc)$")
):
    """
    Get trending datasets with pagination, filters, and sorting.
    
    Args:
        page: Page number (1-indexed)
        limit: Items per page (max 100)
        domain: Filter by domain
        modality: Filter by modality
        platform: Filter by source platform (huggingface, kaggle, etc)
        quality: Filter by quality level (excellent, good, fair, poor)
        search: Search query for filtering by name/description
        sort_by: Sort field (trending, downloads, likes)
        sort_order: Sort order (asc, desc)
        
    Returns:
        List of datasets sorted by specified criteria
    """
    from app.db.connection import mongodb
    
    # Build cache key
    cache_key = f"trending:{page}:{limit}:{domain}:{modality}:{platform}:{quality}:{search}:{sort_by}:{sort_order}"
    
    # Check cache
    cached = await redis_client.get(cache_key)
    if cached:
        return cached
    
    # Build query
    query = {}
    if domain:
        query['domain'] = domain
    if modality:
        query['modality'] = modality
    if platform:
        # Convert to lowercase to match database values
        query['source.platform'] = platform.lower()
    if quality:
        # Map quality labels to quality_label values
        valid_labels = {
            'excellent': 'Excellent',
            'good': 'Good', 
            'fair': 'Fair',
            'poor': 'Poor'
        }
        if quality in valid_labels:
            query['quality_label'] = valid_labels[quality]
    
    # Handle search with $or for multiple fields
    search_clause = None
    if search:
        search_clause = {
            '$or': [
                {'canonical_name': {'$regex': search, '$options': 'i'}},
                {'display_name': {'$regex': search, '$options': 'i'}},
                {'description': {'$regex': search, '$options': 'i'}}
            ]
        }
    
    # Calculate skip
    skip = (page - 1) * limit
    
    # Determine sort direction
    sort_direction = -1 if sort_order == "desc" else 1
    
    # Build sort criteria based on sort_by
    if sort_by == "downloads":
        sort_criteria = [
            ('source.source_metadata.downloads', sort_direction),
            ('created_at', -1)
        ]
    elif sort_by == "likes":
        sort_criteria = [
            ('source.source_metadata.likes', sort_direction),
            ('created_at', -1)
        ]
    else:  # trending (default)
        sort_criteria = [
            ('trend_score', sort_direction),
            ('created_at', -1)
        ]
        # When sorting by trending descending, include datasets with trend scores >= 0
        # or datasets without trend_score (they go at the end due to None sorting last)
        if sort_order == "desc" and not search_clause:
            query['$or'] = [
                {'trend_score': {'$gte': 0}},
                {'trend_score': {'$exists': False}},
                {'trend_score': None}
            ]
        elif sort_order == "desc" and search_clause:
            # Combine search and trend score filters with $and
            query['$and'] = [
                search_clause,
                {
                    '$or': [
                        {'trend_score': {'$gte': 0}},
                        {'trend_score': {'$exists': False}},
                        {'trend_score': None}
                    ]
                }
            ]
            search_clause = None  # Already added to query
    
    # Add search clause if not already added
    if search_clause:
        query.update(search_clause)
    
    # Fetch datasets with sorting
    cursor = mongodb.db.datasets.find(query).sort(sort_criteria).skip(skip).limit(limit)
    datasets = await cursor.to_list(length=limit)
    
    # Get total count
    total = await mongodb.db.datasets.count_documents(query)
    
    # Format response
    result = {
        'datasets': [
            {
                'id': str(d['_id']),
                'name': d.get('display_name') or d.get('canonical_name'),  # Prefer display_name for UI
                'canonical_name': d.get('canonical_name'),  # Keep for uniqueness
                'description': d.get('description', '')[:200] if d.get('description') else '',
                'domain': d.get('domain'),
                'modality': d.get('modality'),
                'trend_score': d.get('trend_score', 0),
                'quality_score': d.get('quality_score', 0),
                'quality_label': d.get('quality_label'),
                'created_at': d.get('created_at').isoformat() if d.get('created_at') and hasattr(d.get('created_at'), 'isoformat') else d.get('created_at'),
                'source': d.get('source', {}),
                'size': d.get('size', {}),
            }
            for d in datasets
        ],
        'pagination': {
            'page': page,
            'limit': limit,
            'total': total,
            'pages': (total + limit - 1) // limit
        }
    }
    
    # Cache for 1 hour
    await redis_client.set(cache_key, result, ttl=3600)

    
    return result


@router.get("/{dataset_id}")
async def get_dataset_details(
    dataset_id: str
):
    """
    Get detailed information about a dataset.
    
    Args:
        dataset_id: Dataset ID
        
    Returns:
        Dataset details with LLM-generated summary
    """
    from app.db.connection import mongodb
    from bson import ObjectId
    
    # Validate ObjectId
    if not ObjectId.is_valid(dataset_id):
        raise HTTPException(status_code=400, detail="Invalid dataset ID")
    
    # Check cache
    cache_key = f"dataset:{dataset_id}"
    cached = await redis_client.get(cache_key)
    if cached:
        return cached
    
    # Fetch dataset
    dataset = await mongodb.db.datasets.find_one({'_id': ObjectId(dataset_id)})
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Format response
    result = {
        'id': str(dataset['_id']),
        'name': dataset.get('display_name') or dataset.get('canonical_name'),  # Prefer display_name for UI
        'canonical_name': dataset.get('canonical_name'),  # Keep for uniqueness
        'description': dataset.get('description', ''),
        'domain': dataset.get('domain'),
        'modality': dataset.get('modality'),
        'trend_score': dataset.get('trend_score', 0),
        'quality_score': dataset.get('quality_score', 0),
        'created_at': dataset.get('created_at').isoformat() if dataset.get('created_at') and hasattr(dataset.get('created_at'), 'isoformat') else dataset.get('created_at'),
        'source': dataset.get('source', {}),
        'size': dataset.get('size', {}),
        'license': dataset.get('license'),
        'metadata': dataset.get('metadata', {}),
        'llm_summary': dataset.get('llm_summary'),
        'intelligence': dataset.get('intelligence'),  # AI-extracted metadata
        'intelligence_updated_at': dataset.get('intelligence_updated_at').isoformat() if dataset.get('intelligence_updated_at') and hasattr(dataset.get('intelligence_updated_at'), 'isoformat') else dataset.get('intelligence_updated_at')
    }
    
    # Cache for 1 hour
    await redis_client.set(cache_key, result, ttl=3600)
    
    return result


@router.get("/{dataset_id}/samples")
async def get_dataset_samples(
    dataset_id: str,
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100)
):
    """
    Get sample data from the dataset.
    
    Args:
        dataset_id: Dataset ID
        page: Page number
        limit: Items per page
        
    Returns:
        Sample data from the dataset
    """
    from app.db.connection import mongodb
    from bson import ObjectId
    import requests
    
    # Validate ObjectId
    if not ObjectId.is_valid(dataset_id):
        raise HTTPException(status_code=400, detail="Invalid dataset ID")
    
    # Fetch dataset
    dataset = await mongodb.db.datasets.find_one({'_id': ObjectId(dataset_id)})
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    source = dataset.get('source', {})
    platform = source.get('platform')
    platform_id = source.get('platform_id')
    source_url = source.get('url')
    
    # Route to platform-specific handler
    if platform == 'huggingface':
        return await _fetch_huggingface_samples(platform_id, page, limit)
    elif platform == 'openml':
        return await _fetch_openml_samples(platform_id, page, limit)
    elif platform == 'kaggle':
        return await _fetch_kaggle_samples(platform_id, page, limit)
    else:
        # Return helpful message for unsupported platforms
        raise HTTPException(
            status_code=400, 
            detail={
                'message': f"Sample preview is not available for {platform or 'this'} datasets",
                'platform': platform,
                'suggestion': f"You can view the full dataset at: {source_url}" if source_url else "Try accessing the dataset on its original platform",
                'supported_platforms': ['huggingface', 'openml', 'kaggle']
            }
        )


async def _fetch_huggingface_samples(platform_id: str, page: int, limit: int):
    """Fetch samples from HuggingFace datasets API."""
    import requests
    
    if not platform_id:
        raise HTTPException(status_code=400, detail="No platform ID found")
    
    try:
        # Fetch dataset info from HuggingFace API
        # First, try to get available configs
        api_url = "https://datasets-server.huggingface.co/splits"
        params = {'dataset': platform_id}
        
        logger.info(f"Fetching splits for dataset: {platform_id}")
        response = requests.get(api_url, params=params, timeout=10)
        
        if response.status_code != 200:
            logger.error(f"Failed to fetch splits: {response.status_code} - {response.text}")
            raise HTTPException(
                status_code=400,
                detail="Unable to fetch dataset information from HuggingFace"
            )
        
        splits_data = response.json()
        splits = splits_data.get('splits', [])
        
        if not splits:
            logger.error(f"No splits available for dataset: {platform_id}")
            raise HTTPException(
                status_code=400,
                detail="No data splits available for this dataset"
            )
        
        # Get first split configuration
        first_split = splits[0]
        config = first_split.get('config')
        split = first_split.get('split')
        
        logger.info(f"Using config: {config}, split: {split}")
        
        # Now fetch first rows
        rows_url = "https://datasets-server.huggingface.co/first-rows"
        rows_params = {
            'dataset': platform_id,
            'config': config,
            'split': split
        }
        
        logger.info(f"Fetching rows with params: {rows_params}")
        rows_response = requests.get(rows_url, params=rows_params, timeout=10)
        
        if rows_response.status_code != 200:
            logger.error(f"Failed to fetch rows: {rows_response.status_code} - {rows_response.text}")
            raise HTTPException(
                status_code=400,
                detail=f"Unable to fetch dataset samples from HuggingFace. The dataset may be private, gated, or temporarily unavailable."
            )
        
        data = rows_response.json()
        rows = data.get('rows', [])
        features = data.get('features', [])
        
        if not rows:
            logger.warning(f"No rows returned for dataset: {platform_id}")
            raise HTTPException(
                status_code=400,
                detail="No sample data available for this dataset"
            )
        
        # Calculate pagination
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_rows = rows[start_idx:end_idx]
        
        logger.info(f"Successfully fetched {len(rows)} rows, returning {len(paginated_rows)} for page {page}")
        
        return {
            'samples': paginated_rows,
            'features': features,
            'pagination': {
                'page': page,
                'limit': limit,
                'total': len(rows),
                'pages': (len(rows) + limit - 1) // limit
            },
            'config': config,
            'split': split,
            'source': 'huggingface'
        }
        
    except HTTPException:
        raise
    except requests.RequestException as e:
        logger.error(f"Request failed for dataset {platform_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to connect to HuggingFace API. Please try again later."
        )
    except Exception as e:
        logger.error(f"Unexpected error fetching samples for {platform_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="An unexpected error occurred while fetching dataset samples"
        )


async def _fetch_openml_samples(platform_id: str, page: int, limit: int):
    """Fetch samples from OpenML API."""
    import requests
    
    if not platform_id:
        raise HTTPException(status_code=400, detail="No platform ID found")
    
    try:
        # OpenML uses numeric dataset IDs
        # First, get dataset info
        data_url = f"https://www.openml.org/api/v1/json/data/{platform_id}"
        
        logger.info(f"Fetching OpenML dataset info: {platform_id}")
        response = requests.get(data_url, timeout=10)
        
        if response.status_code != 200:
            logger.error(f"Failed to fetch OpenML data info: {response.status_code}")
            raise HTTPException(
                status_code=400,
                detail="Unable to fetch dataset information from OpenML"
            )
        
        data = response.json()
        dataset_info = data.get('data_set_description', {})
        
        # Get features/attributes
        features_url = f"https://www.openml.org/api/v1/json/data/features/{platform_id}"
        features_response = requests.get(features_url, timeout=10)
        
        features = []
        if features_response.status_code == 200:
            features_data = features_response.json()
            raw_features = features_data.get('data_features', {}).get('feature', [])
            features = [
                {
                    'name': f.get('name'),
                    'type': f.get('data_type'),
                    'is_target': f.get('is_target') == 'true'
                }
                for f in raw_features
            ]
        
        # Extract metadata
        num_instances = dataset_info.get('NumberOfInstances')
        num_features = dataset_info.get('NumberOfFeatures')
        
        # Try to get actual data samples from OpenML's CSV export
        file_id = dataset_info.get('file_id')
        samples = []
        if file_id:
            try:
                csv_url = f"https://www.openml.org/data/v1/get_csv/{file_id}"
                csv_response = requests.get(csv_url, timeout=10)
                if csv_response.status_code == 200:
                    import pandas as pd
                    import io
                    # Read only enough for a sample
                    df = pd.read_csv(io.StringIO(csv_response.text), nrows=limit * 2)
                    
                    # Convert to list of dicts and handle NaN
                    raw_samples = df.to_dict('records')
                    # Replace NaN with None for JSON serialization and structure like HuggingFace
                    samples = [
                        {
                            'row_idx': idx,
                            'row': {k: (v if pd.notnull(v) else None) for k, v in row.items()},
                            'truncated_cells': []
                        }
                        for idx, row in enumerate(raw_samples)
                    ]
            except Exception as e:
                logger.warning(f"Failed to parse CSV for OpenML {platform_id}: {e}")

        # Return dataset info with features and actual samples
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_samples = samples[start_idx:end_idx]

        return {
            'samples': paginated_samples,
            'features': features,
            'dataset_info': {
                'name': dataset_info.get('name'),
                'version': dataset_info.get('version'),
                'description': dataset_info.get('description', '')[:500],
                'num_instances': num_instances,
                'num_features': num_features,
                'format': dataset_info.get('format', 'ARFF'),
                'download_url': dataset_info.get('url')
            },
            'pagination': {
                'page': page,
                'limit': limit,
                'total': len(samples),
                'pages': (len(samples) + limit - 1) // limit if samples else 0
            },
            'source': 'openml'
        }
        
    except HTTPException:
        raise
    except requests.RequestException as e:
        logger.error(f"Request failed for OpenML dataset {platform_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to connect to OpenML API. Please try again later."
        )
    except Exception as e:
        logger.error(f"Unexpected error fetching OpenML samples for {platform_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="An unexpected error occurred while fetching dataset samples"
        )


async def _fetch_kaggle_samples(platform_id: str, page: int, limit: int):
    """Fetch samples from Kaggle datasets API."""
    import requests
    from app.config import settings
    import io
    import pandas as pd
    import zipfile
    
    if not platform_id:
        raise HTTPException(status_code=400, detail="No platform ID found")
    
    # Use Kaggle credentials
    username = settings.kaggle_username
    key = settings.kaggle_key
    
    if not username or not key:
        logger.warning("Kaggle API credentials not configured")
        raise HTTPException(
            status_code=400, 
            detail="Kaggle dataset samples require API credentials. Please configure KAGGLE_USERNAME and KAGGLE_KEY."
        )
    
    try:
        # Try direct download approach - download full file but limit extraction/parsing
        # Note: We can't use Range headers because Kaggle returns zips that need full headers
        download_url = f"https://www.kaggle.com/api/v1/datasets/download/{platform_id}"
        
        logger.info(f"Attempting to fetch Kaggle dataset: {platform_id}")
        
        # Download with streaming to limit memory usage, but get full file
        # Set a reasonable max size limit (10MB) to avoid huge downloads
        max_size = 10 * 1024 * 1024  # 10MB
        resp = requests.get(download_url, auth=(username, key), timeout=30, stream=True)
        
        if resp.status_code == 401:
            raise HTTPException(status_code=401, detail="Kaggle API authentication failed. Check credentials.")
        
        if resp.status_code == 403:
            raise HTTPException(
                status_code=403, 
                detail="Access denied. This dataset may require additional permissions or acceptance of terms on Kaggle."
            )
        
        if resp.status_code == 404:
            logger.error(f"Kaggle dataset not found: {platform_id}")
            raise HTTPException(
                status_code=404, 
                detail="Dataset not found on Kaggle. It may have been removed or made private."
            )
        
        if resp.status_code not in [200, 206]:
            logger.error(f"Failed to download Kaggle dataset: {resp.status_code}")
            raise HTTPException(
                status_code=400, 
                detail=f"Unable to download dataset from Kaggle (status {resp.status_code})"
            )
        
        # Read content with size limit
        content = b''
        for chunk in resp.iter_content(chunk_size=8192):
            content += chunk
            if len(content) > max_size:
                logger.warning(f"Kaggle dataset {platform_id} exceeds {max_size/1024/1024}MB, truncating download")
                break
        
        samples = []
        features = []
        
        # Check if content is zipped
        is_zipped = content.startswith(b'PK\x03\x04')
        
        if is_zipped:
            # Try to extract data from zip
            try:
                with zipfile.ZipFile(io.BytesIO(content)) as zf:
                    file_list = zf.namelist()
                    
                    # Find first CSV file in the archive
                    csv_files = [name for name in file_list if name.lower().endswith('.csv')]
                    image_files = [name for name in file_list if name.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'))]
                    audio_files = [name for name in file_list if name.lower().endswith(('.mp3', '.wav', '.ogg', '.m4a', '.flac'))]
                    
                    # Prioritize CSV if available (tabular data with references)
                    if csv_files:
                        first_csv = csv_files[0]
                        logger.info(f"Reading CSV file from zip: {first_csv}")
                        
                        with zf.open(first_csv) as csv_file:
                            # Read limited data
                            df = pd.read_csv(csv_file, nrows=limit * 3, encoding='utf-8', encoding_errors='ignore')
                            
                            # Check if CSV has file references (image/audio paths)
                            has_file_refs = False
                            file_ref_columns = []
                            for col in df.columns:
                                col_lower = col.lower()
                                if any(kw in col_lower for kw in ['image', 'photo', 'picture', 'filename', 'file', 'path', 'audio', 'sound']):
                                    file_ref_columns.append(col)
                                    has_file_refs = True
                            
                            raw_samples = df.to_dict('records')
                            samples = [
                                {
                                    'row_idx': idx,
                                    'row': {k: (v if pd.notnull(v) else None) for k, v in row.items()},
                                    'truncated_cells': []
                                }
                                for idx, row in enumerate(raw_samples)
                            ]
                            features = [{'name': col, 'type': str(df[col].dtype)} for col in df.columns]
                            
                            if has_file_refs:
                                logger.info(f"CSV contains file references in columns: {file_ref_columns}")
                    
                    # If no CSV but has images, inform user that direct image preview not available
                    elif image_files:
                        logger.warning(f"Kaggle dataset {platform_id} contains {len(image_files)} image files but no CSV for structured preview")
                        raise HTTPException(
                            status_code=400,
                            detail=f"This dataset contains {len(image_files)} image files. Image-only datasets don't support preview. Please download the full dataset from Kaggle to view images."
                        )
                    
                    # If no CSV but has audio, inform user that direct audio preview not available  
                    elif audio_files:
                        logger.warning(f"Kaggle dataset {platform_id} contains {len(audio_files)} audio files but no CSV for structured preview")
                        raise HTTPException(
                            status_code=400,
                            detail=f"This dataset contains {len(audio_files)} audio files. Audio-only datasets don't support preview. Please download the full dataset from Kaggle to view audio files."
                        )
                    
                    else:
                        logger.warning(f"No supported files found in Kaggle zip for {platform_id}")
                        raise HTTPException(
                            status_code=400,
                            detail="No CSV, image, or audio files found in dataset archive. This dataset may not support sample preview."
                        )
                        
            except zipfile.BadZipFile:
                logger.error(f"Invalid zip file for Kaggle dataset {platform_id}")
                raise HTTPException(
                    status_code=400,
                    detail="Downloaded file is corrupted or incomplete. Try again later."
                )
            except Exception as e:
                logger.error(f"Error extracting Kaggle zip: {e}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to extract dataset samples: {str(e)}"
                )
        else:
            # Direct CSV content
            try:
                text_content = content.decode('utf-8', errors='ignore')
                # Find last complete line
                last_newline = text_content.rfind('\n')
                if last_newline > 0:
                    text_content = text_content[:last_newline]
                
                df = pd.read_csv(io.StringIO(text_content), nrows=limit * 3)
                raw_samples = df.to_dict('records')
                samples = [
                    {
                        'row_idx': idx,
                        'row': {k: (v if pd.notnull(v) else None) for k, v in row.items()},
                        'truncated_cells': []
                    }
                    for idx, row in enumerate(raw_samples)
                ]
                features = [{'name': col, 'type': str(df[col].dtype)} for col in df.columns]
                
            except Exception as e:
                logger.warning(f"Failed to parse Kaggle CSV: {e}")
                raise HTTPException(
                    status_code=400,
                    detail="Failed to parse dataset content. The format may not be supported."
                )
        
        if not samples:
            raise HTTPException(
                status_code=400,
                detail="No sample data could be extracted from this dataset."
            )
        
        # Return paginated samples
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_samples = samples[start_idx:end_idx]
        
        logger.info(f"Successfully extracted {len(samples)} samples from Kaggle dataset {platform_id}")
        
        return {
            'samples': paginated_samples,
            'features': features,
            'pagination': {
                'page': page,
                'limit': limit,
                'total': len(samples),
                'pages': (len(samples) + limit - 1) // limit if samples else 0
            },
            'source': 'kaggle',
            'is_partial': True
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching Kaggle samples: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="An unexpected error occurred while fetching Kaggle samples"
        )





@router.post("/search")
async def search_datasets(
    query: str = Query(..., min_length=1),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100),
    semantic: bool = Query(False),
    db: AsyncIOMotorDatabase = Depends(get_database)
):
    """
    Search datasets using full-text or semantic search.
    
    Args:
        query: Search query
        page: Page number
        limit: Items per page
        semantic: Use semantic search (embeddings)
        
    Returns:
        Search results
    """
    # Build cache key
    cache_key = f"search:{query}:{page}:{limit}:{semantic}"
    
    # Check cache
    cached = await redis_client.get(cache_key)
    if cached:
        return cached
    
    skip = (page - 1) * limit
    
    if semantic:
        # Use semantic search
        from app.ml.semantic_search import get_semantic_search
        
        try:
            search = await get_semantic_search(db)
            datasets = await search.search_datasets(query, k=limit, threshold=0.3)
            
            # Format response
            result = {
                'query': query,
                'method': 'semantic',
                'datasets': [
                    {
                        'id': str(d['_id']),
                        'name': d.get('canonical_name'),
                        'description': d.get('description', '')[:200],
                        'domain': d.get('domain'),
                        'modality': d.get('modality'),
                        'trend_score': d.get('trend_score'),
                        'similarity_score': d.get('similarity_score')
                    }
                    for d in datasets
                ],
                'pagination': {
                    'page': page,
                    'limit': limit,
                    'total': len(datasets)
                }
            }
        except Exception as e:
            logger.error(f"Semantic search failed: {e}")
            # Fall back to text search
            semantic = False
    
    if not semantic:
        # Full-text search
        cursor = db.datasets.find(
            {'$text': {'$search': query}}
        ).sort('trend_score', -1).skip(skip).limit(limit)
        
        datasets = await cursor.to_list(length=limit)
        
        # Format response
        result = {
            'query': query,
            'method': 'full_text',
            'datasets': [
                {
                    'id': str(d['_id']),
                    'name': d.get('canonical_name'),
                    'description': d.get('description', '')[:200],
                    'domain': d.get('domain'),
                    'modality': d.get('modality'),
                    'trend_score': d.get('trend_score'),
                }
                for d in datasets
            ],
            'pagination': {
                'page': page,
                'limit': limit,
                'total': len(datasets)
            }
        }
    
    # Cache for 1 hour
    await redis_client.set(cache_key, result, ttl=settings.search_cache_ttl_seconds)
    
    return result


@router.get("/filters/options")
async def get_filter_options(
    db: AsyncIOMotorDatabase = Depends(get_database)
):
    """
    Get available filter options (domains, modalities).
    
    Returns:
        Available filter values
    """
    # Check cache
    cache_key = "filters:options"
    cached = await redis_client.get(cache_key)
    if cached:
        return cached
    
    # Get distinct values
    domains = await db.datasets.distinct('domain')
    modalities = await db.datasets.distinct('modality')
    
    result = {
        'domains': [d for d in domains if d],
        'modalities': [m for m in modalities if m]
    }
    
    # Cache for 24 hours
    await redis_client.set(cache_key, result, ttl=24 * 3600)
    
    return result


@router.post("/{dataset_id}/analyze")
async def trigger_dataset_analysis(
    dataset_id: str,
    force_refresh: bool = Query(False)
):
    """
    Trigger intelligence analysis for a dataset.
    
    Args:
        dataset_id: Dataset ID
        force_refresh: Force re-analysis even if cached
        
    Returns:
        Task status and ID
    """
    from app.tasks.llm_tasks import analyze_dataset_intelligence
    from bson import ObjectId
    
    # Validate ObjectId
    if not ObjectId.is_valid(dataset_id):
        raise HTTPException(status_code=400, detail="Invalid dataset ID")
    
    # Check if dataset exists
    from app.db.connection import mongodb
    dataset = await mongodb.db.datasets.find_one({'_id': ObjectId(dataset_id)})
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Check if already analyzed (unless force_refresh)
    if not force_refresh and dataset.get('intelligence'):
        return {
            'status': 'already_analyzed',
            'dataset_id': dataset_id,
            'intelligence': dataset.get('intelligence'),
            'analyzed_at': dataset.get('intelligence_updated_at').isoformat() if dataset.get('intelligence_updated_at') else None
        }
    
    # Trigger analysis task
    task = analyze_dataset_intelligence.delay(dataset_id)
    
    logger.info(f"Triggered intelligence analysis for {dataset.get('canonical_name')} (task: {task.id})")
    
    return {
        'status': 'queued',
        'dataset_id': dataset_id,
        'task_id': task.id,
        'message': 'Analysis task queued. Check back in a few moments.'
    }


@router.get("/{dataset_id}/intelligence")
async def get_dataset_intelligence(
    dataset_id: str
):
    """
    Get intelligence data for a dataset.
    
    Args:
        dataset_id: Dataset ID
        
    Returns:
        Intelligence metadata
    """
    from bson import ObjectId
    
    # Validate ObjectId
    if not ObjectId.is_valid(dataset_id):
        raise HTTPException(status_code=400, detail="Invalid dataset ID")
    
    # Fetch dataset
    from app.db.connection import mongodb
    dataset = await mongodb.db.datasets.find_one({'_id': ObjectId(dataset_id)})
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    intelligence = dataset.get('intelligence')
    
    if not intelligence:
        return {
            'status': 'not_analyzed',
            'dataset_id': dataset_id,
            'message': 'Dataset has not been analyzed yet. Use POST /datasets/{id}/analyze to trigger analysis.'
        }
    
    return {
        'status': 'success',
        'dataset_id': dataset_id,
        'intelligence': intelligence,
        'analyzed_at': dataset.get('intelligence_updated_at').isoformat() if dataset.get('intelligence_updated_at') else None,
        'version': dataset.get('intelligence_version', '1.0')
    }


@router.get("/{dataset_id}/fitness")
async def get_dataset_fitness_score(dataset_id: str):
    """
    Get fitness score for a dataset.
    
    Returns a 0-10 composite score with breakdown across:
    - Metadata Completeness
    - Size Appropriateness
    - Documentation Quality
    - License Clarity
    - Freshness
    - Community Signals
    """
    try:
        ObjectId(dataset_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid dataset ID")
    
    from app.db.connection import mongodb
    from app.analytics.fitness_calculator import fitness_calculator
    
    dataset = await mongodb.db.datasets.find_one({'_id': ObjectId(dataset_id)})
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Calculate fitness score
    fitness_result = fitness_calculator.calculate_fitness_score(dataset)
    
    return {
        'status': 'success',
        'dataset_id': dataset_id,
        'dataset_name': dataset.get('canonical_name'),
        'fitness': fitness_result
    }


@router.get("/{dataset_id}/license")
async def get_dataset_license_analysis(dataset_id: str):
    """
    Get license analysis for a dataset.
    
    Returns:
    - License classification (commercial-safe, non-commercial, etc.)
    - Commercial use eligibility
    - Required conditions (attribution, share-alike)
    - Safety color code for UI
    """
    try:
        ObjectId(dataset_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid dataset ID")
    
    from app.db.connection import mongodb
    from app.analytics.license_analyzer import license_analyzer
    
    dataset = await mongodb.db.datasets.find_one({'_id': ObjectId(dataset_id)})
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Analyze license
    license_result = license_analyzer.analyze_license(dataset)
    safety_badge = license_analyzer.get_safety_badge(dataset)
    commercial_check = license_analyzer.can_use_commercially(dataset)
    
    return {
        'status': 'success',
        'dataset_id': dataset_id,
        'dataset_name': dataset.get('canonical_name'),
        'license': license_result,
        'safety_badge': safety_badge,
        'commercial_use': commercial_check
    }


@router.get("/{dataset_id}/full-analysis")
async def get_dataset_full_analysis(dataset_id: str):
    """
    Get complete analysis for a dataset including:
    - Fitness score with breakdown
    - License analysis with safety classification
    - AI intelligence (if available)
    
    This is the comprehensive view for the dataset detail page.
    """
    try:
        ObjectId(dataset_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid dataset ID")
    
    from app.db.connection import mongodb
    from app.analytics.fitness_calculator import fitness_calculator
    from app.analytics.license_analyzer import license_analyzer
    
    dataset = await mongodb.db.datasets.find_one({'_id': ObjectId(dataset_id)})
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Calculate all analyses
    fitness_result = fitness_calculator.calculate_fitness_score(dataset)
    license_result = license_analyzer.analyze_license(dataset)
    safety_badge = license_analyzer.get_safety_badge(dataset)
    commercial_check = license_analyzer.can_use_commercially(dataset)
    
    return {
        'status': 'success',
        'dataset_id': dataset_id,
        'dataset_name': dataset.get('canonical_name'),
        'fitness': fitness_result,
        'license': {
            **license_result,
            'safety_badge': safety_badge,
            'commercial_use': commercial_check
        },
        'intelligence': dataset.get('intelligence'),
        'has_intelligence': dataset.get('intelligence') is not None,
        'trend_score': dataset.get('trend_score'),
        'domain': dataset.get('domain'),
        'modality': dataset.get('modality')
    }


@router.get("/{dataset_id}/similar")
async def get_similar_datasets(
    dataset_id: str,
    limit: int = Query(10, ge=1, le=20),
    min_similarity: float = Query(0.1, ge=0, le=1)
):
    """
    Find datasets similar to the given dataset.
    
    Uses multi-factor similarity based on:
    - Domain matching
    - Modality matching  
    - Description/content similarity
    - Tag overlap
    - Task similarity
    
    Args:
        dataset_id: ID of the source dataset
        limit: Maximum number of similar datasets (1-20)
        min_similarity: Minimum similarity threshold (0-1)
        
    Returns:
        List of similar datasets with similarity scores and match reasons
    """
    try:
        ObjectId(dataset_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid dataset ID")
    
    from app.db.connection import mongodb
    from app.analytics.similarity_engine import similarity_engine
    
    # Get source dataset
    source = await mongodb.db.datasets.find_one({'_id': ObjectId(dataset_id)})
    if not source:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Find similar datasets
    similar = await similarity_engine.find_similar(
        dataset_id=dataset_id,
        db=mongodb.db,
        limit=limit,
        min_similarity=min_similarity
    )
    
    # Format response
    formatted = [similarity_engine.format_similar_dataset(item) for item in similar]
    
    return {
        'status': 'success',
        'source_dataset': {
            'id': dataset_id,
            'name': source.get('canonical_name'),
            'domain': source.get('domain'),
            'modality': source.get('modality')
        },
        'similar_datasets': formatted,
        'count': len(formatted)
    }


@router.get("/{dataset_id}/models")
async def get_model_recommendations(
    dataset_id: str,
    limit: int = Query(5, ge=1, le=10)
):
    """
    Get recommended model architectures for a dataset.
    
    Based on the dataset's:
    - Task (classification, detection, QA, etc.)
    - Modality (image, text, audio, tabular)
    - Size (for complexity recommendations)
    
    Returns:
        List of recommended models with explanations
    """
    try:
        ObjectId(dataset_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid dataset ID")
    
    from app.db.connection import mongodb
    from app.analytics.model_matcher import model_matcher
    
    dataset = await mongodb.db.datasets.find_one({'_id': ObjectId(dataset_id)})
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Get recommendations
    result = model_matcher.get_recommendations(dataset, limit=limit)
    
    return {
        'status': 'success',
        'dataset_id': dataset_id,
        'dataset_name': dataset.get('canonical_name'),
        **result
    }


@router.get("/search/advanced")
async def advanced_search(
    q: str = Query(..., description="DSL query string"),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100)
):
    """
    Advanced search using DataVault Query Language.
    
    Syntax examples:
    - task:image-classification
    - domain:nlp modality:text
    - license:mit downloads>10000
    - size>5000 platform:huggingface
    - "machine learning" task:classification
    
    Supported fields:
    - task, domain, modality, license, platform
    - downloads, likes, size (numeric with >, <, >=, <=)
    - name, author, tag
    """
    from app.db.connection import mongodb
    from app.analytics.query_parser import query_parser
    
    # Parse the query
    parsed = query_parser.parse(q)
    
    if not parsed['valid']:
        return {
            'status': 'error',
            'errors': parsed['errors'],
            'datasets': [],
            'count': 0
        }
    
    # Execute search
    skip = (page - 1) * limit
    query = parsed['mongodb_query']
    
    cursor = mongodb.db.datasets.find(query).sort([
        ('trend_score', -1),
        ('created_at', -1)
    ]).skip(skip).limit(limit)
    
    datasets = await cursor.to_list(length=limit)
    total = await mongodb.db.datasets.count_documents(query)
    
    # Format results
    results = []
    for d in datasets:
        metadata = d.get('source', {}).get('source_metadata', {})
        results.append({
            'id': str(d['_id']),
            'name': d.get('canonical_name'),
            'description': (d.get('description', '') or '')[:200],
            'domain': d.get('domain'),
            'modality': d.get('modality'),
            'platform': d.get('source', {}).get('platform'),
            'downloads': metadata.get('downloads'),
            'likes': metadata.get('likes'),
            'trend_score': d.get('trend_score')
        })
    
    return {
        'status': 'success',
        'query': q,
        'parsed': {
            'tokens': parsed['tokens'],
            'text_search': parsed['text_search']
        },
        'datasets': results,
        'pagination': {
            'page': page,
            'limit': limit,
            'total': total,
            'pages': (total + limit - 1) // limit
        }
    }


@router.get("/search/suggestions")
async def get_search_suggestions(
    q: str = Query("", description="Partial query for suggestions")
):
    """Get autocomplete suggestions for DSL query."""
    from app.analytics.query_parser import query_parser
    
    suggestions = query_parser.get_suggestions(q)
    
    return {
        'status': 'success',
        'query': q,
        'suggestions': suggestions
    }


@router.get("/search/validate")
async def validate_search_query(
    q: str = Query(..., description="Query to validate")
):
    """Validate a DSL query and return helpful errors."""
    from app.analytics.query_parser import query_parser
    
    validation = query_parser.validate_query(q)
    parsed = query_parser.parse(q)
    
    return {
        'status': 'success',
        'query': q,
        'valid': validation['valid'],
        'errors': validation['errors'],
        'warnings': validation['warnings'],
        'suggestions': validation['suggestions'],
        'parsed_tokens': parsed['tokens']
    }


@router.get("/{dataset_id}/bias")
async def get_bias_analysis(dataset_id: str):
    """
    Get bias and fairness analysis for a dataset.
    
    Returns:
    - Risk score (0-100)
    - Risk level (low, medium, high)
    - Specific warnings with severity
    - Recommendations for mitigation
    """
    try:
        ObjectId(dataset_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid dataset ID")
    
    from app.db.connection import mongodb
    from app.analytics.bias_analyzer import bias_analyzer
    
    dataset = await mongodb.db.datasets.find_one({'_id': ObjectId(dataset_id)})
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Analyze for bias
    result = bias_analyzer.analyze(dataset)
    
    return {
        'status': 'success',
        'dataset_id': dataset_id,
        'dataset_name': dataset.get('canonical_name'),
        **result
    }


@router.get("/{dataset_id}/versions")
async def get_version_history(dataset_id: str):
    """
    Get version history for a dataset.
    
    Returns timeline of snapshots showing dataset evolution.
    """
    try:
        ObjectId(dataset_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid dataset ID")
    
    from app.db.connection import mongodb
    from app.analytics.version_tracker import version_tracker
    
    dataset = await mongodb.db.datasets.find_one({'_id': ObjectId(dataset_id)})
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    versions = version_tracker.get_versions(dataset)
    
    return {
        'status': 'success',
        'dataset_id': dataset_id,
        'dataset_name': dataset.get('canonical_name'),
        'versions': versions,
        'version_count': len(versions)
    }


@router.get("/{dataset_id}/drift")
async def get_drift_analysis(dataset_id: str):
    """
    Get drift analysis for a dataset.
    
    Detects significant changes and potential issues with the dataset.
    """
    try:
        ObjectId(dataset_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid dataset ID")
    
    from app.db.connection import mongodb
    from app.analytics.version_tracker import version_tracker
    
    dataset = await mongodb.db.datasets.find_one({'_id': ObjectId(dataset_id)})
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    drift = version_tracker.detect_drift(dataset)
    
    return {
        'status': 'success',
        'dataset_id': dataset_id,
        'dataset_name': dataset.get('canonical_name'),
        **drift
    }


@router.get("/{dataset_id}/synthetic")
async def get_synthetic_analysis(dataset_id: str):
    """
    Get synthetic data suitability analysis for a dataset.
    
    Returns augmentation recommendations and suitability scoring.
    """
    try:
        ObjectId(dataset_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid dataset ID")
    
    from app.db.connection import mongodb
    from app.analytics.synthetic_analyzer import synthetic_analyzer
    
    dataset = await mongodb.db.datasets.find_one({'_id': ObjectId(dataset_id)})
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    result = synthetic_analyzer.analyze(dataset)
    
    return {
        'status': 'success',
        'dataset_id': dataset_id,
        'dataset_name': dataset.get('canonical_name'),
        **result
    }


@router.get("/{dataset_id}/card")
async def get_dataset_card(dataset_id: str):
    """
    Generate a comprehensive dataset card.
    
    Returns structured documentation sections and full markdown.
    """
    try:
        ObjectId(dataset_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid dataset ID")
    
    from app.db.connection import mongodb
    from app.analytics.card_generator import card_generator
    
    dataset = await mongodb.db.datasets.find_one({'_id': ObjectId(dataset_id)})
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    result = card_generator.generate_card(dataset)
    
    return {
        'status': 'success',
        'dataset_id': dataset_id,
        **result
    }


@router.get("/{dataset_id}/card/download")
async def download_dataset_card(dataset_id: str):
    """
    Download dataset card as Markdown file.
    """
    from fastapi.responses import Response
    
    try:
        ObjectId(dataset_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid dataset ID")
    
    from app.db.connection import mongodb
    from app.analytics.card_generator import card_generator
    
    dataset = await mongodb.db.datasets.find_one({'_id': ObjectId(dataset_id)})
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    result = card_generator.generate_card(dataset)
    markdown = result['markdown']
    filename = f"{result['name'].replace('/', '_')}_card.md"
    
    return Response(
        content=markdown,
        media_type="text/markdown",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@router.post("/admin/update-sizes")
async def update_dataset_sizes():
    """
    Admin endpoint to update dataset sizes from HuggingFace API.
    """
    import httpx
    from app.db.connection import mongodb
    
    # Known sizes from HuggingFace API and common datasets
    KNOWN_SIZES = {
        # Computer Vision
        'ylecun/mnist': 70000,
        'uoft-cs/cifar100': 60000,
        'uoft-cs/cifar10': 60000,
        'rishitdagli/cppe-5': 1029,
        'zh-plus/tiny-imagenet': 120000,
        'frgfm/imagenette': 13394,
        'fashion_mnist': 70000,
        'svhn': 630420,
        'beans': 1295,
        'cats_vs_dogs': 25000,
        'food101': 101000,
        'oxford_flowers102': 8189,
        'stanford_cars': 16185,
        'dtd': 5640,
        'fgvc_aircraft': 10000,
        'caltech101': 9146,
        # NLP
        'stanfordnlp/imdb': 100000,
        'rajpurkar/squad': 98169,
        'tatsu-lab/alpaca': 52002,
        'openai/gsm8k': 8792,
        'glue/sst2': 70042,
        'glue/mnli': 432702,
        'glue/qqp': 795524,
        'wikitext': 36718,
        'ag_news': 127600,
        'yelp_review_full': 700000,
        'amazon_polarity': 4000000,
        'dbpedia_14': 630000,
        'yahoo_answers': 1460000,
        'conll2003': 22137,
        # Audio
        'common_voice': 1600000,
        'librispeech_asr': 960,
        'speech_commands': 105829,
    }
    
    updated = []
    
    # Update from known sizes first
    for platform_id, size in KNOWN_SIZES.items():
        result = await mongodb.db.datasets.update_one(
            {'source.platform_id': platform_id},
            {'$set': {'size.samples': size}}
        )
        if result.modified_count > 0:
            updated.append({'platform_id': platform_id, 'samples': size})
    
    # Fetch from HuggingFace API for unknown datasets
    cursor = mongodb.db.datasets.find({
        'source.platform': 'huggingface',
        '$or': [
            {'size.samples': {'$exists': False}},
            {'size.samples': None},
            {'size.samples': 0}
        ]
    })
    
    async with httpx.AsyncClient(timeout=30) as client:
        async for dataset in cursor:
            platform_id = dataset.get('source', {}).get('platform_id', '')
            if not platform_id:
                continue
            
            try:
                url = f'https://huggingface.co/api/datasets/{platform_id}/parquet'
                resp = await client.get(url)
                
                if resp.status_code == 200:
                    data = resp.json()
                    total = 0
                    if isinstance(data, dict):
                        for split_info in data.values():
                            if isinstance(split_info, list):
                                for p in split_info:
                                    if isinstance(p, dict):
                                        total += p.get('num_rows', 0)
                    
                    if total > 0:
                        await mongodb.db.datasets.update_one(
                            {'_id': dataset['_id']},
                            {'$set': {'size.samples': total}}
                        )
                        updated.append({'platform_id': platform_id, 'samples': total})
            except Exception:
                pass
    
    return {
        'status': 'success',
        'updated_count': len(updated),
        'updated_datasets': updated
    }


@router.get("/admin/list-datasets")
async def list_all_datasets():
    """
    Admin endpoint to list all datasets with their size info.
    """
    from app.db.connection import mongodb
    
    cursor = mongodb.db.datasets.find(
        {},
        {
            'canonical_name': 1,
            'source.platform': 1,
            'source.platform_id': 1,
            'size.samples': 1,
            'modality': 1
        }
    )
    
    datasets = []
    missing_samples = []
    
    async for d in cursor:
        info = {
            'id': str(d['_id']),
            'name': d.get('canonical_name', 'Unknown'),
            'platform': d.get('source', {}).get('platform', 'Unknown'),
            'platform_id': d.get('source', {}).get('platform_id', ''),
            'samples': d.get('size', {}).get('samples'),
            'modality': d.get('modality', '')
        }
        datasets.append(info)
        
        if not info['samples']:
            missing_samples.append(info['name'])
    
    return {
        'status': 'success',
        'total_datasets': len(datasets),
        'missing_samples_count': len(missing_samples),
        'missing_samples': missing_samples,
        'datasets': datasets
    }


@router.post("/admin/fetch-all-sizes")
async def fetch_all_sizes():
    """
    Fetch and update sizes for ALL datasets from their respective platforms.
    """
    import httpx
    from app.db.connection import mongodb
    
    updated = []
    errors = []
    
    # Get all datasets
    cursor = mongodb.db.datasets.find({})
    
    async with httpx.AsyncClient(timeout=30) as client:
        async for dataset in cursor:
            platform = dataset.get('source', {}).get('platform', '')
            platform_id = dataset.get('source', {}).get('platform_id', '')
            name = dataset.get('canonical_name', 'Unknown')
            current_samples = dataset.get('size', {}).get('samples')
            
            if current_samples and current_samples > 0:
                continue  # Already has samples
            
            if not platform_id:
                continue
            
            try:
                size = None
                
                # HuggingFace
                if platform == 'huggingface':
                    url = f'https://huggingface.co/api/datasets/{platform_id}/parquet'
                    resp = await client.get(url)
                    
                    if resp.status_code == 200:
                        data = resp.json()
                        total = 0
                        if isinstance(data, dict):
                            for split_info in data.values():
                                if isinstance(split_info, list):
                                    for p in split_info:
                                        if isinstance(p, dict):
                                            total += p.get('num_rows', 0)
                        if total > 0:
                            size = total
                
                # OpenML
                elif platform == 'openml':
                    # Extract numeric ID
                    openml_id = platform_id.split('/')[-1] if '/' in platform_id else platform_id
                    url = f'https://www.openml.org/api/v1/json/data/{openml_id}'
                    resp = await client.get(url)
                    
                    if resp.status_code == 200:
                        data = resp.json()
                        num_instances = data.get('data_set_description', {}).get('NumberOfInstances')
                        if num_instances:
                            size = int(num_instances)
                
                # Update if we got a size
                if size and size > 0:
                    await mongodb.db.datasets.update_one(
                        {'_id': dataset['_id']},
                        {'$set': {'size.samples': size}}
                    )
                    updated.append({'name': name, 'platform': platform, 'samples': size})
                else:
                    errors.append({'name': name, 'platform': platform, 'reason': 'No size available from API'})
                    
            except Exception as e:
                errors.append({'name': name, 'platform': platform, 'reason': str(e)})
    
    return {
        'status': 'success',
        'updated_count': len(updated),
        'error_count': len(errors),
        'updated_datasets': updated,
        'errors': errors
    }


@router.post("/admin/fetch-comprehensive-sizes")
async def fetch_comprehensive_sizes(background_tasks: BackgroundTasks):
    """
    Comprehensive size fetcher that gets both row counts and file sizes.
    Supports: HuggingFace, Kaggle (scraping), OpenML
    Runs in background due to long execution time.
    """
    from app.scrapers.size_fetcher import ComprehensiveSizeFetcher
    
    async def run_fetch():
        fetcher = ComprehensiveSizeFetcher()
        return await fetcher.update_all_datasets()
    
    # Start in background
    background_tasks.add_task(run_fetch)
    
    return {
        'status': 'started',
        'message': 'Size fetching started in background. This may take several minutes.'
    }


@router.post("/admin/fetch-sizes-sync")
async def fetch_sizes_sync(limit: int = Query(20, ge=1, le=100)):
    """
    Synchronous size fetcher - fetches sizes for limited number of datasets.
    Uses HuggingFace main API with size_categories and size fields.
    """
    import httpx
    import re
    from app.db.connection import mongodb
    
    def parse_size_category(size_cat: str) -> int:
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
    
    updated = []
    errors = []
    
    cursor = mongodb.db.datasets.find({
        'source.platform': 'huggingface',
        '$or': [
            {'size.samples': {'$exists': False}},
            {'size.samples': None},
            {'size.samples': 0}
        ]
    }).limit(limit)
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        async for dataset in cursor:
            platform_id = dataset.get('source', {}).get('platform_id', '')
            name = dataset.get('canonical_name', platform_id)
            
            if not platform_id:
                continue
            
            try:
                # Use main API endpoint
                url = f"https://huggingface.co/api/datasets/{platform_id}"
                resp = await client.get(url)
                
                row_count = None
                file_size = None
                
                if resp.status_code == 200:
                    data = resp.json()
                    
                    # Get file size from 'size' field (bytes)
                    if 'size' in data and data['size']:
                        try:
                            file_size = int(data['size'])
                        except (ValueError, TypeError):
                            pass
                    
                    # Get row count from cardData
                    card_data = data.get('cardData', {})
                    
                    # Try size_categories
                    if 'size_categories' in card_data:
                        size_cat = card_data['size_categories']
                        if isinstance(size_cat, list) and size_cat:
                            size_cat = size_cat[0]
                        row_count = parse_size_category(size_cat)
                    
                    # Try dataset_info splits if no size_categories
                    if row_count is None:
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
                
                # Update database
                if row_count or file_size:
                    update_fields = {}
                    if row_count:
                        update_fields['size.samples'] = row_count
                    if file_size:
                        update_fields['size.file_size_bytes'] = file_size
                    
                    await mongodb.db.datasets.update_one(
                        {'_id': dataset['_id']},
                        {'$set': update_fields}
                    )
                    updated.append({
                        'name': name,
                        'samples': row_count,
                        'file_size_bytes': file_size
                    })
                else:
                    errors.append({'name': name, 'reason': 'No size data in API'})
                
            except Exception as e:
                errors.append({'name': name, 'reason': str(e)})
    
    return {
        'status': 'success',
        'updated_count': len(updated),
        'error_count': len(errors),
        'updated': updated,
        'errors': errors[:10]
    }
@router.get("/admin/size-stats")
async def get_size_stats():
    """Get statistics about dataset size data coverage."""
    from app.db.connection import mongodb
    
    total = await mongodb.db.datasets.count_documents({})
    
    has_samples = await mongodb.db.datasets.count_documents({
        'size.samples': {'$exists': True, '$ne': None, '$gt': 0}
    })
    
    has_file_size = await mongodb.db.datasets.count_documents({
        'size.file_size_bytes': {'$exists': True, '$ne': None, '$gt': 0}
    })
    
    # Get breakdown by platform
    pipeline = [
        {
            '$group': {
                '_id': '$source.platform',
                'total': {'$sum': 1},
                'with_samples': {
                    '$sum': {'$cond': [{'$gt': ['$size.samples', 0]}, 1, 0]}
                },
                'with_file_size': {
                    '$sum': {'$cond': [{'$gt': ['$size.file_size_bytes', 0]}, 1, 0]}
                }
            }
        }
    ]
    
    platform_stats = []
    async for doc in mongodb.db.datasets.aggregate(pipeline):
        platform_stats.append({
            'platform': doc['_id'],
            'total': doc['total'],
            'with_samples': doc['with_samples'],
            'with_file_size': doc['with_file_size'],
            'sample_coverage': f"{(doc['with_samples']/doc['total']*100):.1f}%" if doc['total'] > 0 else "0%"
        })
    
    return {
        'status': 'success',
        'total_datasets': total,
        'with_samples': has_samples,
        'with_file_size': has_file_size,
        'sample_coverage': f"{(has_samples/total*100):.1f}%" if total > 0 else "0%",
        'file_size_coverage': f"{(has_file_size/total*100):.1f}%" if total > 0 else "0%",
        'by_platform': platform_stats
    }


@router.get("/{dataset_id}/versions")
async def get_dataset_versions(dataset_id: str):
    """
    Get version history for a dataset.
    
    Returns all tracked version snapshots ordered by timestamp descending.
    """
    from app.db.connection import mongodb
    
    try:
        obj_id = ObjectId(dataset_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid dataset ID")
    
    # Fetch versions from database
    cursor = mongodb.db.dataset_versions.find(
        {'dataset_id': obj_id}
    ).sort('timestamp', -1)
    
    versions = await cursor.to_list(length=100)
    
    # Format response
    formatted_versions = []
    for v in versions:
        ts = v.get('timestamp')
        if ts:
            ts_str = ts.isoformat() if hasattr(ts, 'isoformat') else str(ts)
        else:
            ts_str = None
        
        formatted_versions.append({
            'version': v.get('version', 'unknown'),
            'timestamp': ts_str,
            'samples': v.get('samples'),
            'downloads': v.get('downloads'),
            'likes': v.get('likes'),
            'file_size': v.get('file_size'),
            'is_current': v.get('is_current', False)
        })

    
    return {
        'status': 'success',
        'versions': formatted_versions
    }


@router.get("/{dataset_id}/drift")
async def get_dataset_drift(dataset_id: str):
    """
    Calculate drift metrics for a dataset by comparing versions.
    
    Returns drift score, level, and specific alerts for significant changes.
    """
    from app.db.connection import mongodb
    
    try:
        obj_id = ObjectId(dataset_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid dataset ID")
    
    # Fetch the two most recent versions
    cursor = mongodb.db.dataset_versions.find(
        {'dataset_id': obj_id}
    ).sort('timestamp', -1).limit(2)
    
    versions = await cursor.to_list(length=2)
    
    # If less than 2 versions, no drift to calculate
    if len(versions) < 2:
        return {
            'status': 'success',
            'drift_score': 0,
            'drift_level': 'stable',
            'alerts': []
        }
    
    current = versions[0]
    previous = versions[1]
    
    alerts = []
    drift_score = 0
    
    # Compare key metrics
    def calc_change(field_name: str, display_name: str):
        nonlocal drift_score
        curr_val = current.get(field_name)
        prev_val = previous.get(field_name)
        
        if curr_val is None or prev_val is None or prev_val == 0:
            return
        
        change_percent = ((curr_val - prev_val) / prev_val) * 100
        
        if abs(change_percent) > 50:
            severity = 'high'
            drift_score += 30
        elif abs(change_percent) > 20:
            severity = 'medium'
            drift_score += 15
        elif abs(change_percent) > 5:
            severity = 'low'
            drift_score += 5
        else:
            return  # No significant change
        
        direction = "increased" if change_percent > 0 else "decreased"
        alerts.append({
            'field': field_name,
            'previous_value': prev_val,
            'current_value': curr_val,
            'change_percent': round(change_percent, 1),
            'severity': severity,
            'description': f"{display_name} {direction} by {abs(round(change_percent, 1))}%"
        })
    
    calc_change('samples', 'Sample count')
    calc_change('downloads', 'Downloads')
    calc_change('likes', 'Likes')
    calc_change('file_size', 'File size')
    
    # Determine drift level
    if drift_score >= 60:
        drift_level = 'high'
    elif drift_score >= 30:
        drift_level = 'medium'
    elif drift_score > 0:
        drift_level = 'low'
    else:
        drift_level = 'stable'
    
    return {
        'status': 'success',
        'drift_score': min(drift_score, 100),
        'drift_level': drift_level,
        'alerts': alerts
    }


@router.get("/{dataset_id}/similar")
async def get_similar_datasets(
    dataset_id: str,
    limit: int = Query(10, ge=1, le=50),
    same_platform: bool = False,
    same_modality: bool = False,
    db: AsyncIOMotorDatabase = Depends(get_database)
):
    """
    Get similar datasets using ML-based recommendations.
    Uses semantic embeddings to find contextually similar datasets.
    
    Args:
        dataset_id: Target dataset ID
        limit: Number of recommendations
        same_platform: Filter to same platform
        same_modality: Filter to same modality
    
    Returns:
        List of similar datasets with similarity scores
    """
    try:
        # Get recommender instance
        recommender = await get_recommender(db)
        
        # Build filters
        filters = {}
        if same_platform:
            filters['same_platform'] = True
        if same_modality:
            filters['same_modality'] = True
        
        # Get recommendations
        recommendations = await recommender.get_similar_datasets(
            dataset_id=dataset_id,
            limit=limit,
            filters=filters
        )
        
        return {
            'status': 'success',
            'target_dataset_id': dataset_id,
            'count': len(recommendations),
            'recommendations': recommendations
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error getting similar datasets: {e}")
        raise HTTPException(status_code=500, detail="Failed to get recommendations")


@router.get("/{dataset_id}/quality")
async def get_dataset_quality(
    dataset_id: str,
    detailed: bool = False,
    db: AsyncIOMotorDatabase = Depends(get_database)
):
    """
    Get quality score and breakdown for a dataset.
    
    Args:
        dataset_id: Dataset ID
        detailed: Include detailed breakdown
    
    Returns:
        Quality score and optional breakdown
    """
    try:
        # Fetch dataset
        dataset = await db.datasets.find_one({'_id': ObjectId(dataset_id)})
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        # Calculate quality score
        if detailed:
            result = quality_scorer.get_quality_breakdown(dataset)
            result['label'] = quality_scorer.get_quality_label(result['overall'])
        else:
            score = quality_scorer.calculate_quality_score(dataset)
            result = {
                'overall': score,
                'label': quality_scorer.get_quality_label(score)
            }
        
        return {
            'status': 'success',
            'dataset_id': dataset_id,
            'quality': result
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error calculating quality score: {e}")
        raise HTTPException(status_code=500, detail="Failed to calculate quality")


@router.post("/search/semantic")
async def semantic_search(
    query: str,
    limit: int = Query(10, ge=1, le=50),
    domain: Optional[str] = None,
    modality: Optional[str] = None,
    platform: Optional[str] = None,
    db: AsyncIOMotorDatabase = Depends(get_database)
):
    """
    Search datasets using semantic similarity.
    Finds datasets matching natural language queries.
    
    Args:
        query: Natural language search query
        limit: Number of results
        domain: Optional domain filter
        modality: Optional modality filter
        platform: Optional platform filter
    
    Returns:
        Semantically similar datasets
    """
    try:
        recommender = await get_recommender(db)
        
        # Build filters
        filters = {}
        if domain:
            filters['domain'] = domain
        if modality:
            filters['modality'] = modality
        if platform:
            filters['source.platform'] = platform.lower()
        
        # Get semantic recommendations
        results = await recommender.get_recommendations_by_query(
            query_text=query,
            limit=limit,
            filters=filters
        )
        
        return {
            'status': 'success',
            'query': query,
            'count': len(results),
            'results': results
        }
        
    except Exception as e:
        logger.error(f"Error in semantic search: {e}")
        raise HTTPException(status_code=500, detail="Semantic search failed")
