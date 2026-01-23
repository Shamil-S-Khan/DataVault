"""
Script to refresh metadata for existing datasets by re-running scrapers.
Run this inside the Docker container: docker-compose exec backend python -m app.refresh_datasets
"""
import sys
import logging
from app.tasks.scraping_tasks import scrape_source, SOURCE_CATEGORIES

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def refresh_all_sources():
    """Refresh metadata from all sources."""
    all_sources = []
    for category, sources in SOURCE_CATEGORIES.items():
        all_sources.extend(sources)
    
    logger.info(f"Refreshing metadata from {len(all_sources)} sources: {', '.join(all_sources)}")
    
    results = []
    
    for source in all_sources:
        logger.info(f"\n{'='*60}")
        logger.info(f"Refreshing {source}...")
        logger.info(f"{'='*60}")
        
        try:
            result = scrape_source(source, auto_save=True)
            
            if result['status'] == 'success':
                logger.info(f"✓ {source}: Scraped {result['scraped_count']} datasets, saved {result['saved_count']}")
                results.append({'source': source, 'status': 'success', 'count': result['scraped_count']})
            else:
                logger.error(f"✗ {source}: {result.get('message', 'Unknown error')}")
                results.append({'source': source, 'status': 'error', 'message': result.get('message', 'Unknown error')})
                
        except Exception as e:
            logger.error(f"✗ {source}: Failed with error: {e}")
            results.append({'source': source, 'status': 'error', 'message': str(e)})
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("SUMMARY:")
    logger.info(f"{'='*60}")
    for r in results:
        if r['status'] == 'success':
            logger.info(f"  ✓ {r['source']}: {r['count']} datasets")
        else:
            logger.error(f"  ✗ {r['source']}: {r['message']}")
    
    logger.info(f"\n{'='*60}")
    logger.info("Metadata refresh complete!")
    logger.info(f"{'='*60}")


def refresh_specific_sources(source_names: list):
    """Refresh metadata for specific sources."""
    logger.info(f"Refreshing metadata from: {', '.join(source_names)}")
    
    results = []
    
    for source in source_names:
        logger.info(f"\n{'='*60}")
        logger.info(f"Refreshing {source}...")
        logger.info(f"{'='*60}")
        
        try:
            result = scrape_source(source, auto_save=True)
            
            if result['status'] == 'success':
                logger.info(f"✓ {source}: Scraped {result['scraped_count']} datasets, saved {result['saved_count']}")
                results.append({'source': source, 'status': 'success', 'count': result['scraped_count']})
            else:
                logger.error(f"✗ {source}: {result.get('message', 'Unknown error')}")
                results.append({'source': source, 'status': 'error', 'message': result.get('message', 'Unknown error')})
                
        except Exception as e:
            logger.error(f"✗ {source}: Failed with error: {e}")
            results.append({'source': source, 'status': 'error', 'message': str(e)})
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("SUMMARY:")
    logger.info(f"{'='*60}")
    for r in results:
        if r['status'] == 'success':
            logger.info(f"  ✓ {r['source']}: {r['count']} datasets")
        else:
            logger.error(f"  ✗ {r['source']}: {r['message']}")
    
    logger.info(f"\n{'='*60}")
    logger.info("Metadata refresh complete!")
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════════════════════╗
║         DataVault Metadata Refresh Utility               ║
╚══════════════════════════════════════════════════════════╝

This script will re-scrape datasets and update their metadata
including descriptions, row counts, and other information.

Available sources:
  - ML Platforms: huggingface, kaggle, openml
  - Academic: zenodo, harvard_dataverse
  - Government: datagov, aws_opendata
  - Curated: curated

Usage:
  python -m app.refresh_datasets                    # Refresh all sources
  python -m app.refresh_datasets zenodo kaggle      # Refresh specific sources
    """)
    
    if len(sys.argv) > 1:
        # Refresh specific sources
        sources = sys.argv[1:]
        refresh_specific_sources(sources)
    else:
        # Refresh all sources
        refresh_all_sources()
