"""
Dataset API routes.
Endpoints for dataset discovery, search, and details.
"""
from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks, Request
from typing import List, Optional, Dict, Any
import hashlib
import json
from datetime import datetime
from bson import ObjectId
from pydantic import BaseModel, Field
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db.connection import get_database
from app.db.redis_client import redis_client
from app.db.models import Dataset
from app.llm.gemini_client import gemini_client
from app.llm.xai_client import xai_client
from app.llm.groq_client import groq_client
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
    
    # Trigger analysis task with fallback if Celery/Redis is down
    try:
        task = analyze_dataset_intelligence.delay(dataset_id)
        logger.info(f"Triggered intelligence analysis for {dataset.get('canonical_name')} (Celery task: {task.id})")
        return {
            'status': 'queued',
            'dataset_id': dataset_id,
            'task_id': task.id,
            'message': 'Analysis task queued via Celery.'
        }
    except Exception as e:
        logger.warning(f"Celery dispatch failed, falling back to sync analysis for {dataset_id}: {e}")
        # Run intelligence analysis synchronously (in a separate thread to not block)
        from app.llm.dataset_intelligence import dataset_intelligence_analyzer
        import asyncio
        
        # We'll use BackgroundTasks to run it so we can return immediately
        from fastapi import BackgroundTasks
        
        async def run_sync_analysis():
            try:
                # Fetch schema and samples
                from app.db.connection import mongodb
                db = mongodb.db
                schema = dataset.get('metadata', {}).get('schema')
                
                # Get sample data
                from bson import ObjectId
                samples_cursor = db.dataset_samples.find({'dataset_id': ObjectId(dataset_id)}).limit(5)
                sample_data = []
                async for sample in samples_cursor:
                    if 'data' in sample:
                        sample_data.append(sample['data'])
                
                intelligence = await dataset_intelligence_analyzer.analyze_dataset(
                    dataset_id=dataset_id,
                    dataset_name=dataset.get('canonical_name', ''),
                    description=dataset.get('description', ''),
                    schema=schema,
                    samples=sample_data if sample_data else None,
                    metadata=dataset.get('metadata')
                )
                
                if intelligence:
                    # Update database
                    intelligence_dict = intelligence.model_dump()
                    if 'analyzed_at' in intelligence_dict:
                        intelligence_dict['analyzed_at'] = intelligence_dict['analyzed_at'].isoformat()
                    
                    await db.datasets.update_one(
                        {'_id': ObjectId(dataset_id)},
                        {
                            '$set': {
                                'intelligence': intelligence_dict,
                                'intelligence_updated_at': datetime.utcnow(),
                                'updated_at': datetime.utcnow()
                            }
                        }
                    )
            except Exception as inner_e:
                logger.error(f"Sync analysis fallback failed for {dataset_id}: {inner_e}")

        # Note: In a production environment, you'd use BackgroundTasks from the FastAPI dependency
        # But here we'll just fire and forget since we are already inside the route
        asyncio.create_task(run_sync_analysis())
        
        return {
            'status': 'processing',
            'dataset_id': dataset_id,
            'message': 'Analysis started in-process (Celery unavailable).'
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


@router.get("/{dataset_id}/gqi")
async def get_dataset_gqi(dataset_id: str):
    """
    Get Global Quality Index (GQI) for a dataset.

    Returns a 0-1 composite score with breakdown across:
    - Structural Clarity (20%)
    - Representational Entropy (20%)
    - Academic Authority (20%)
    - Operational Fitness (40%)
    Plus label reliability multiplier and synthetic resilience context.
    """
    try:
        ObjectId(dataset_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid dataset ID")

    from app.db.connection import mongodb
    from app.ml.composite_scorer import composite_scorer

    dataset = await mongodb.db.datasets.find_one({'_id': ObjectId(dataset_id)})

    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    # Calculate GQI
    gqi_result = composite_scorer.calculate_gqi(dataset)

    return {
        'status': 'success',
        'dataset_id': dataset_id,
        'dataset_name': dataset.get('display_name') or dataset.get('canonical_name'),
        'gqi': gqi_result
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
        obj_id = ObjectId(dataset_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid dataset ID")
    
    from app.db.connection import mongodb
    from app.analytics.fitness_calculator import fitness_calculator
    from app.analytics.license_analyzer import license_analyzer
    
    dataset = await mongodb.db.datasets.find_one({'_id': obj_id})
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


class ChatMessage(BaseModel):
    role: str = Field(..., pattern="^(user|assistant)$")
    content: str

class ChatRequest(BaseModel):
    message: str
    history: List[ChatMessage] = Field(default_factory=list)

class SmartSearchFilters(BaseModel):
    domain: Optional[str] = None
    modality: Optional[str] = None
    row_count_gte: Optional[int] = None
    size_bytes_lte: Optional[int] = None
    platform: Optional[str] = None
    min_gqi: Optional[float] = None

class SmartSearchRequest(BaseModel):
    query: str
    filters: Optional[SmartSearchFilters] = None
    limit: int = 100
    offset: int = 0

@router.post("/{dataset_id}/chat")
async def chat_with_dataset(
    dataset_id: str,
    request: ChatRequest,
    fastapi_request: Request,
    db: AsyncIOMotorDatabase = Depends(get_database)
):
    """
    Conversational Q&A about a specific dataset.
    """
    try:
        obj_id = ObjectId(dataset_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid dataset ID")

    # 1. Rate Limiting (Bypass if disabled)
    if not settings.disable_tier_gating:
        client_ip = fastapi_request.client.host
        today = datetime.utcnow().strftime("%Y-%m-%d")
        rate_key = f"chat_limit:unauth:{dataset_id}:{client_ip}:{today}"
        
        count = await redis_client.increment(rate_key)
        if count == 1:
            await redis_client.expire(rate_key, 86400) # 24h
            
        if count > 2:
            raise HTTPException(
                status_code=429,
                detail={
                    "error": "UPGRADE_REQUIRED",
                    "message": "You've reached the free chat limit for today. Upgrade to Pro for unlimited dataset chat."
                }
            )

    # 2. Fetch Dataset and Samples
    dataset = await db.datasets.find_one({"_id": obj_id})
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
        
    samples = await db.dataset_samples.find({"dataset_id": obj_id}).limit(5).to_list(5)

    # 3. Assemble Context
    context_parts = []
    
    # Basic Info
    name = dataset.get("canonical_name", "Unknown")
    desc = dataset.get("description", "No description provided.")
    context_parts.append(f"Dataset Name: {name}\nDescription: {desc}")
    
    # Metadata
    source_info = dataset.get("source", {})
    platform = source_info.get("platform", "Unknown")
    source_url = source_info.get("url", "N/A")
    domain = dataset.get("domain", "N/A")
    modality = dataset.get("modality", "N/A")
    context_parts.append(f"Platform: {platform}\nSource URL: {source_url}\nDomain: {domain}\nModality: {modality}")
    
    # Size
    size_info = dataset.get("size", {})
    samples_count = size_info.get("samples", "Unknown")
    file_size = size_info.get("file_size_gb", "Unknown")
    context_parts.append(f"Scale: {samples_count} rows, approx {file_size} GB")
    
    # Quality (GQI)
    gqi_score = dataset.get("gqi_score")
    if gqi_score is not None:
        context_parts.append(f"Global Quality Index (GQI): {gqi_score:.2f}")
        
    quality_breakdown = dataset.get("quality_breakdown", {})
    if quality_breakdown:
        breakdown_items = []
        for k, v in quality_breakdown.items():
            if isinstance(v, dict):
                score = v.get("score")
                if score is not None:
                    breakdown_items.append(f"- {k.title()}: {score:.2f}")
                else:
                    breakdown_items.append(f"- {k.title()}: {v}")
            elif isinstance(v, (int, float)):
                breakdown_items.append(f"- {k.title()}: {v:.2f}")
            else:
                breakdown_items.append(f"- {k.title()}: {v}")
        
        qb_str = "\n".join(breakdown_items)
        context_parts.append(f"Quality Breakdown:\n{qb_str}")
        
    # Task Fitness
    fitness = dataset.get("fitness", {})
    task_fitness = fitness.get("task_fitness")
    if task_fitness:
        tf_str = "\n".join([f"- {k}: {v['match_rate']}% (Reasoning: {', '.join(v['reasoning'])})" for k, v in task_fitness.items()])
        context_parts.append(f"Task Fitness Scores:\n{tf_str}")
        
    # Intelligence / AI Analysis
    intelligence = dataset.get("intelligence", {})
    if intelligence:
        summary = intelligence.get("summary")
        if summary:
            context_parts.append(f"AI Analysis Summary: {summary}")
        
        tasks = intelligence.get("tasks", [])
        if tasks:
            context_parts.append(f"Suggested Tasks: {', '.join(tasks)}")

    # Sample Data Table
    if samples:
        header = "| " + " | ".join(samples[0]["row"].keys()) + " |"
        separator = "| " + " | ".join(["---"] * len(samples[0]["row"])) + " |"
        rows = []
        for s in samples:
            row_vals = [str(v)[:50] + ("..." if len(str(v)) > 50 else "") for v in s["row"].values()]
            rows.append("| " + " | ".join(row_vals) + " |")
        
        sample_table = f"Sample Data (First 5 Rows):\n{header}\n{separator}\n" + "\n".join(rows)
        context_parts.append(sample_table)

    assembled_context = "\n\n---\n\n".join(context_parts)

    # 4. Prompt Engineering
    system_prompt = f"""You are DataVault Assistant, an expert AI helping users understand and evaluate datasets for machine learning projects.

You have been given structured information about a specific dataset:
---
{assembled_context}
---

Answer the user's questions about this dataset accurately and concisely. Base your answers ONLY on the information provided above — do not invent statistics, scores, or facts not present in the context. If the context does not contain enough information to answer a question, say so clearly and suggest what additional information would help. Keep answers under 150 words unless a detailed breakdown is explicitly requested. Use plain language, not jargon."""

    messages = [{"role": "system", "content": system_prompt}]
    
    # Add history (last 10 turns max)
    history = request.history[-10:] if request.history else []
    for msg in history:
        messages.append({"role": msg.role, "content": msg.content})
        
    # Add new user message
    messages.append({"role": "user", "content": request.message})

    # 5. Call LLM
    try:
        if settings.llm_provider == "gemini":
            from app.llm.gemini_client import gemini_client
            reply = await gemini_client.chat(messages)
        elif settings.llm_provider == "grok":
            from app.llm.xai_client import xai_client
            reply = await xai_client.chat(messages)
        elif settings.llm_provider == "groq":
            from app.llm.groq_client import groq_client
            reply = await groq_client.chat(messages)
        else:
            from app.llm.huggingface_client import huggingface_client
            reply = await huggingface_client.chat(messages)
        
        if not reply:
            raise Exception("LLM returned empty response")
            
        return {
            "reply": reply,
            "dataset_id": dataset_id
        }
    except Exception as e:
        logger.error(f"Chat failed for dataset {dataset_id}: {e}")
        raise HTTPException(
            status_code=503,
            detail="The chat service is temporarily unavailable. Please try again in a moment."
        )




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
    
    cursor = mongodb.db.datasets.aggregate(pipeline, allowDiskUse=True)
    platform_stats = []
    async for doc in cursor:
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


@router.post("/smart-search")
async def smart_search(
    request: SmartSearchRequest,
    fastapi_request: Request,
    db: AsyncIOMotorDatabase = Depends(get_database)
):
    """
    AI-powered natural language search.
    - Extracts intent using LLM
    - Performs semantic search
    - Applies MongoDB filters
    """
    # 1. Intent Extraction (cached)
    query_hash = hashlib.sha256(request.query.encode()).hexdigest()
    cache_key = f"search_intent:{query_hash}"
    
    intent = await redis_client.get(cache_key)
    if not intent:
        try:
            provider = getattr(settings, 'llm_provider', 'gemini').lower()
            
            system_prompt = """Extract search intent from the user's natural language query.
Return ONLY a JSON object with these fields (use null if not mentioned):
{
  "semantic_query": "rephrased query for vector search",
  "domain": "Computer Vision|NLP|Audio|Tabular|Healthcare|etc",
  "modality": "image|text|audio|video|tabular",
  "min_samples": integer,
  "max_size_gb": float,
  "platform": "huggingface|kaggle|openml",
  "min_gqi": float (0-1)
}"""
            
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": request.query}
            ]
            
            llm_response = None
            if provider == 'gemini':
                llm_response = await gemini_client.chat(messages, max_tokens=150, temperature=0.1)
            elif provider == 'grok':
                from app.llm.xai_client import xai_client
                llm_response = await xai_client.chat(messages, max_tokens=150, temperature=0.1)
            elif provider == 'groq':
                from app.llm.groq_client import groq_client
                llm_response = await groq_client.chat(messages)
            else:
                from app.llm.huggingface_client import huggingface_client
                llm_response = await huggingface_client.chat(messages, max_tokens=150, temperature=0.1)
                
            if llm_response:
                # Clean JSON
                start = llm_response.find('{')
                end = llm_response.rfind('}')
                if start != -1 and end != -1:
                    intent_json = llm_response[start:end+1]
                    intent = json.loads(intent_json)
                    await redis_client.set(cache_key, intent, ttl=3600) # 1h
        except Exception as e:
            logger.error(f"Intent extraction failed: {e}")
            intent = None

    # Fallback to defaults
    if not intent:
        intent = {"semantic_query": request.query}

    semantic_query = intent.get("semantic_query") or request.query

    # 2. Parallel Search
    # 2a. Semantic Search Candidates
    try:
        from app.ml.semantic_search import get_semantic_search
        semantic_engine = await get_semantic_search(db)
        semantic_results = await semantic_engine.search(semantic_query, top_k=100)
    except Exception as e:
        logger.error(f"Semantic search failed: {e}")
        semantic_results = []

    # 2b. Keyword Search (MongoDB Text Search)
    keyword_query = {"$text": {"$search": request.query}} if request.query else {}
    # Apply context filters if extracted by LLM
    if intent.get("domain"): keyword_query["domain"] = intent.get("domain")
    if intent.get("modality"): keyword_query["modality"] = intent.get("modality").lower()

    keyword_results = []
    try:
        keyword_cursor = db.datasets.find(
            keyword_query, 
            {"_id": 1, "canonical_name": 1}
        ).sort([("_text_score", {"$meta": "textScore"})]).limit(100)
        keyword_results = await keyword_cursor.to_list(length=100)
    except Exception as e:
        logger.error(f"Keyword search failed: {e}")

    # 3. RRF Fusion
    from app.ml.fusion import reciprocal_rank_fusion
    ranked_ids = reciprocal_rank_fusion(keyword_results, semantic_results)

    # 4. Fetch Full Documents & Apply Hard Filters
    # Fetch a candidate pool based on fused ranks
    candidate_pool_ids = [ObjectId(rid) for rid in ranked_ids[:150]]
    
    filter_query = {"_id": {"$in": candidate_pool_ids}}
    req_f = request.filters or SmartSearchFilters()
    
    if req_f.domain: filter_query["domain"] = req_f.domain
    if req_f.modality: filter_query["modality"] = req_f.modality.lower()
    
    if req_f.row_count_gte or intent.get("min_samples"):
        filter_query["size.samples"] = {"$gte": req_f.row_count_gte or intent.get("min_samples")}
    
    if req_f.size_bytes_lte or (intent.get("max_size_gb") * 1024 * 1024 * 1024 if intent.get("max_size_gb") else None):
        filter_query["size.file_size_bytes"] = {"$lte": req_f.size_bytes_lte or (intent.get("max_size_gb") * 1024 * 1024 * 1024 if intent.get("max_size_gb") else None)}

    if req_f.platform or intent.get("platform"):
        filter_query["source.platform"] = (req_f.platform or intent.get("platform")).lower()

    min_gqi = req_f.min_gqi or intent.get("min_gqi")
    if min_gqi:
        filter_query["gqi_score"] = {"$gte": min_gqi}

    # Fetch final datasets
    final_cursor = db.datasets.find(filter_query)
    datasets_dict = {str(d["_id"]): d for d in await final_cursor.to_list(length=150)}

    # Re-sort into fused order
    filtered_datasets = []
    for rid in ranked_ids:
        if rid in datasets_dict:
            filtered_datasets.append(datasets_dict[rid])

    # 5. Paginate & Format
    total = len(filtered_datasets)
    paged_datasets = filtered_datasets[request.offset : request.offset + request.limit]

    results = []
    # Map semantic scores for the UI display
    semantic_score_map = {str(r["id"]): r["score"] for r in semantic_results}
    
    for d in paged_datasets:
        results.append({
            "id": str(d["_id"]),
            "name": d.get("canonical_name") or d.get("display_name"),
            "description": d.get("description", "")[:250],
            "domain": d.get("domain"),
            "modality": d.get("modality"),
            "platform": d.get("source", {}).get("platform"),
            "gqi_score": d.get("gqi_score"),
            "quality_score": d.get("quality_score") if d.get("quality_score") is not None else d.get("gqi_score", 0),
            "trend_score": d.get("trend_score") if d.get("trend_score") is not None else (d.get("gqi_score", 0) * 0.8), # Mock trend if missing
            "quality_label": d.get("quality_label") or ("Excellent" if d.get("gqi_score", 0) > 0.8 else "Good" if d.get("gqi_score", 0) > 0.5 else "Fair"),
            "size": d.get("size", {}),
            "source": d.get("source", {}),
            "created_at": d.get("created_at").isoformat() if d.get("created_at") and hasattr(d.get("created_at"), 'isoformat') else d.get("created_at"),
            "similarity_score": semantic_score_map.get(str(d["_id"]))
        })


    return {
        "results": results,
        "total": total,
        "intent": intent,
        "query": request.query
    }


@router.get("/{dataset_id}/snippet")
async def get_dataset_snippet(
    dataset_id: str,
    lang: str = Query("python", regex="^(python|javascript|r)$"),
    db: AsyncIOMotorDatabase = Depends(get_database)
):
    """
    Generate code snippets to load the dataset.
    Branches logic based on the dataset source platform.
    """
    # Check cache
    cache_key = f"snippet:{dataset_id}:{lang}"
    cached = await redis_client.get(cache_key)
    if cached:
        return cached

    # Fetch dataset
    try:
        dataset = await db.datasets.find_one({"_id": ObjectId(dataset_id)})
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid dataset ID")

    source = dataset.get("source", {})
    platform = source.get("platform", "").lower()
    platform_id = source.get("platform_id", "")
    url = source.get("url", "")
    
    snippet = ""
    install_note = ""
    
    if lang == "python":
        if platform == "huggingface":
            snippet = f"from datasets import load_dataset\n\n# Load the dataset from HuggingFace\ndataset = load_dataset('{platform_id}')\n\n# Quick preview\nprint(dataset)"
            install_note = "Requires: pip install datasets"
        
        elif platform == "kaggle":
            # Kaggle snippets often require the kaggle CLI or specific username/id
            snippet = f"import kaggle\n\n# Authenticate and download\nkaggle.api.authenticate()\nkaggle.api.dataset_download_files('{platform_id}', path='./data', unzip=True)\n\nimport pandas as pd\nimport os\n# Assuming first CSV found is the main data\ncsv_files = [f for f in os.listdir('./data') if f.endswith('.csv')]\nif csv_files:\n    df = pd.read_csv(os.path.join('./data', csv_files[0]))\n    print(df.head())"
            install_note = "Requires: pip install kaggle pandas (and ~/.kaggle/kaggle.json setup)"
            
        elif platform == "openml":
            openml_id = platform_id.split('/')[-1] if '/' in platform_id else platform_id
            snippet = f"import openml\nimport pandas as pd\n\n# Load dataset from OpenML\ndataset = openml.datasets.get_dataset({openml_id})\nX, y, _, _ = dataset.get_data(target=dataset.default_target_attribute)\n\nprint(f'Loaded: {dataset.name}')\nprint(X.head())"
            install_note = "Requires: pip install openml pandas"
            
        elif url and (url.endswith('.csv') or url.endswith('.parquet')):
            if url.endswith('.csv'):
                snippet = f"import pandas as pd\n\n# Load directly from URL\nurl = '{url}'\ndf = pd.read_csv(url)\n\nprint(df.head())"
            else:
                snippet = f"import pandas as pd\n\n# Load directly from URL\nurl = '{url}'\ndf = pd.read_parquet(url)\n\nprint(df.head())"
            install_note = "Requires: pip install pandas"
            
        else:
            snippet = f"# Manual download required\n# URL: {url or 'No URL available'}\n\nprint('Snippet not available for this platform/format.')"
    
    else:
        snippet = f"// Snippet for {lang} is coming soon!\nconsole.log('Loading dataset {dataset_id}');"

    response_data = {
        "dataset_id": dataset_id,
        "language": lang,
        "platform": platform,
        "snippet": snippet,
        "install_note": install_note
    }

    # Cache for 24h
    await redis_client.set(cache_key, response_data, ttl=86400)
    
    return response_data


@router.get("/{dataset_id}/fitness")
async def get_dataset_fitness(
    dataset_id: str,
    db: AsyncIOMotorDatabase = Depends(get_database)
):
    """
    Calculate fitness scores for standard ML tasks based on dataset characteristics.
    """
    try:
        dataset = await db.datasets.find_one({"_id": ObjectId(dataset_id)})
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid dataset ID")

    modality = (dataset.get("modality") or "").lower()
    domain = (dataset.get("domain") or "").lower()
    intelligence = dataset.get("intelligence", {})
    it_tasks = [t.lower() for t in intelligence.get("tasks", [])]
    
    # Define common tasks and their requirements
    tasks_to_eval = [
        {
            "name": "Image Classification",
            "req_modality": "image",
            "keywords": ["classification", "labels", "recognition"]
        },
        {
            "name": "Sentiment Analysis",
            "req_modality": "text",
            "keywords": ["sentiment", "classification", "nlp"]
        },
        {
            "name": "Object Detection",
            "req_modality": "image",
            "keywords": ["detection", "bounding boxes", "localization"]
        },
        {
            "name": "Tabular Classification",
            "req_modality": "tabular",
            "keywords": ["classification", "tabular"]
        }
    ]
    
    fitness_results = {}
    overall_sum = 0
    
    for task in tasks_to_eval:
        score = 0
        reasoning = []
        
        # 1. Modality match (0-4 points)
        if modality == task["req_modality"]:
            score += 4
            reasoning.append(f"Modality matches {task['req_modality']}")
        elif modality == "multimodal":
            score += 2
            reasoning.append("Dataset is multimodal, likely includes required modality")
        else:
            reasoning.append(f"Modality mismatch (Expected {task['req_modality']}, found {modality})")
            
        # 2. Intelligence Task match (0-4 points)
        task_found = False
        for t in it_tasks:
            if task["name"].lower() in t or t in task["name"].lower():
                task_found = True
                break
        
        if task_found:
            score += 4
            reasoning.append(f"AI Analysis explicitly identified {task['name']} as a valid task")
        else:
            # Sub-keyword search
            matches = [k for k in task["keywords"] if any(k in t for t in it_tasks)]
            if matches:
                score += 2
                reasoning.append(f"Related signals found: {', '.join(matches)}")
            else:
                reasoning.append("No explicit task match found in AI analysis")
                
        # 3. Data Freshness/Quality (0-2 points)
        if dataset.get("quality_score", 0) > 70:
            score += 2
            reasoning.append("High overall data quality")
        elif dataset.get("quality_score", 0) > 40:
            score += 1
            reasoning.append("Moderate data quality")
            
        fitness_results[task["name"]] = {
            "score": score,
            "max_score": 10,
            "match_rate": (score / 10) * 100,
            "reasoning": reasoning
        }
        overall_sum += score

    # Calculate overall stats for the existing component
    avg_score = round(overall_sum / len(tasks_to_eval), 1)
    grade = "D"
    if avg_score >= 8: grade = "A"
    elif avg_score >= 6: grade = "B"
    elif avg_score >= 4: grade = "C"
    
    # Map breakdown to what the component expects
    breakdown = {
        "metadata_completeness": min(10, dataset.get("quality_score", 50) // 10 + 2),
        "size_appropriateness": 8 if dataset.get("size", {}).get("samples", 0) > 1000 else 5,
        "documentation_quality": 9 if dataset.get("description") and len(dataset["description"]) > 500 else 6,
        "license_clarity": 10 if dataset.get("license") else 4,
        "freshness": 7,
        "community_signals": min(10, (dataset.get("source", {}).get("source_metadata", {}).get("likes", 0) // 100) + 5)
    }

    return {
        "status": "success",
        "dataset_id": dataset_id,
        "fitness": {
            "overall_score": avg_score,
            "grade": grade,
            "breakdown": breakdown,
            "task_fitness": fitness_results,
            "explanation": f"Based on its {modality} modality and {domain} domain, this dataset is most suitable for {max(fitness_results.items(), key=lambda x: x[1]['score'])[0]}.",
            "calculated_at": datetime.now().isoformat()
        }
    }


@router.post("/admin/enrich")
async def trigger_pwc_enrichment(
    dataset_id: Optional[str] = Query(None),
    db: AsyncIOMotorDatabase = Depends(get_database)
):
    """
    Trigger Papers With Code enrichment for datasets.
    If dataset_id is provided, only that one is enriched.
    Otherwise, all datasets with 'Papers With Code' in source/metadata are queued.
    """
    from app.tasks.scraping_tasks import enrich_dataset_metadata
    
    if dataset_id:
        enrich_dataset_metadata.delay(dataset_id)
        return {"status": "queued", "count": 1}
        
    # Find candidates
    query = {
        "$or": [
            {"source.platform": "papers_with_code"},
            {"metadata.pwc_id": {"$exists": True}}
        ]
    }
    
    cursor = db.datasets.find(query, {"_id": 1})
    candidates = await cursor.to_list(length=1000)
    
    for doc in candidates:
        enrich_dataset_metadata.delay(str(doc["_id"]))
        
    return {
        "status": "queued",
        "count": len(candidates)
    }


@router.post("/{dataset_id}/star")
async def toggle_star_dataset(
    dataset_id: str,
    db: AsyncIOMotorDatabase = Depends(get_database)
):
    """
    Toggle star status for a dataset.
    In a real app, this would be per-user. 
    Here we just toggle a 'is_starred' flag for demo purposes.
    """
    try:
        obj_id = ObjectId(dataset_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid dataset ID")
        
    dataset = await db.datasets.find_one({"_id": obj_id})
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
        
    new_status = not dataset.get("is_starred", False)
    await db.datasets.update_one(
        {"_id": obj_id},
        {"$set": {"is_starred": new_status, "last_starred_at": datetime.utcnow()}}
    )
    
    return {
        "status": "success",
        "is_starred": new_status,
        "message": "Dataset followed for drift alerts" if new_status else "Dataset unfollowed"
    }


@router.get("/{dataset_id}/drift-events")
async def get_drift_events(
    dataset_id: str,
    db: AsyncIOMotorDatabase = Depends(get_database)
):
    """Get historical drift events for a dataset."""
    try:
        obj_id = ObjectId(dataset_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid dataset ID")
        
    cursor = db.drift_events.find({"dataset_id": obj_id}).sort("timestamp", -1)
    events = await cursor.to_list(length=50)
    
    # Format for frontend
    for e in events:
        e["_id"] = str(e["_id"])
        e["dataset_id"] = str(e["dataset_id"])
        e["timestamp"] = e["timestamp"].isoformat()
        
    return {
        "status": "success",
        "events": events
    }
