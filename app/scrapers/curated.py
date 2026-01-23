"""
Curated ML/AI dataset sources.
Static entries for well-known benchmark datasets that are crucial for ML research.
"""
from typing import Dict, List, Any
from datetime import datetime
import logging
from app.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)


class CuratedDatasetsScraper(BaseScraper):
    """
    Curated list of well-known ML/AI benchmark datasets.
    These are essential datasets that should always be included.
    """
    
    def __init__(self):
        super().__init__("curated")
    
    async def fetch_datasets(self) -> List[Dict[str, Any]]:
        """Return curated list of essential ML datasets."""
        datasets = []
        
        for entry in self.CURATED_DATASETS:
            normalized = self.normalize_dataset(entry)
            if normalized:
                datasets.append(normalized)
        
        logger.info(f"Loaded {len(datasets)} curated ML benchmark datasets")
        return datasets
    
    def normalize_dataset(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize curated entry to standard format."""
        return self.create_standard_dataset(
            name=raw_data['name'],
            description=raw_data['description'],
            url=raw_data['url'],
            platform_id=raw_data['id'],
            domain=raw_data['domain'],
            modality=raw_data['modality'],
            metadata={
                'category': raw_data.get('category'),
                'tasks': raw_data.get('tasks', []),
                'paper_url': raw_data.get('paper_url'),
                'is_benchmark': raw_data.get('is_benchmark', True),
                'year': raw_data.get('year'),
            },
            samples=raw_data.get('samples'),
            file_size_gb=raw_data.get('size_gb'),
            license=raw_data.get('license')
        )
    
    # ===== CURATED DATASETS =====
    CURATED_DATASETS = [
        # ===== Computer Vision =====
        {
            'id': 'imagenet',
            'name': 'ImageNet',
            'description': 'Large-scale hierarchical image database with 14M+ images organized according to WordNet hierarchy. The de facto standard for image classification benchmarks.',
            'url': 'https://www.image-net.org/',
            'domain': 'Computer Vision',
            'modality': 'image',
            'category': 'Vision Benchmark',
            'tasks': ['image classification', 'object detection'],
            'samples': 14000000,
            'size_gb': 150,
            'license': 'Custom (research only)',
            'year': 2009,
        },
        {
            'id': 'coco',
            'name': 'COCO (Common Objects in Context)',
            'description': 'Large-scale object detection, segmentation, and captioning dataset with 330K images and 1.5M object instances.',
            'url': 'https://cocodataset.org/',
            'domain': 'Computer Vision',
            'modality': 'image',
            'category': 'Vision Benchmark',
            'tasks': ['object detection', 'segmentation', 'image captioning'],
            'samples': 330000,
            'size_gb': 25,
            'license': 'CC BY 4.0',
            'year': 2014,
        },
        {
            'id': 'open-images',
            'name': 'Open Images Dataset',
            'description': 'Google\'s dataset of ~9M images annotated with labels, bounding boxes, segmentations, and relationships.',
            'url': 'https://storage.googleapis.com/openimages/web/index.html',
            'domain': 'Computer Vision',
            'modality': 'image',
            'category': 'Vision Benchmark',
            'tasks': ['image classification', 'object detection', 'segmentation'],
            'samples': 9000000,
            'size_gb': 570,
            'license': 'CC BY 4.0',
            'year': 2016,
        },
        {
            'id': 'laion-5b',
            'name': 'LAION-5B',
            'description': 'Largest openly available image-text dataset with 5.85 billion CLIP-filtered image-text pairs.',
            'url': 'https://laion.ai/blog/laion-5b/',
            'domain': 'Computer Vision',
            'modality': 'multimodal',
            'category': 'Vision-Language',
            'tasks': ['image-text matching', 'text-to-image generation'],
            'samples': 5850000000,
            'size_gb': 240000,
            'license': 'CC BY 4.0 (metadata)',
            'year': 2022,
        },
        {
            'id': 'mnist',
            'name': 'MNIST',
            'description': 'Classic handwritten digit recognition dataset with 70K 28x28 grayscale images.',
            'url': 'http://yann.lecun.com/exdb/mnist/',
            'domain': 'Computer Vision',
            'modality': 'image',
            'category': 'Vision Benchmark',
            'tasks': ['digit recognition', 'image classification'],
            'samples': 70000,
            'size_gb': 0.01,
            'license': 'CC BY-SA 3.0',
            'year': 1998,
        },
        {
            'id': 'cifar-10',
            'name': 'CIFAR-10',
            'description': 'Dataset of 60K 32x32 color images in 10 classes. Popular for image classification research.',
            'url': 'https://www.cs.toronto.edu/~kriz/cifar.html',
            'domain': 'Computer Vision',
            'modality': 'image',
            'category': 'Vision Benchmark',
            'tasks': ['image classification'],
            'samples': 60000,
            'size_gb': 0.16,
            'license': 'MIT',
            'year': 2009,
        },
        
        # ===== Natural Language Processing =====
        {
            'id': 'common-crawl',
            'name': 'Common Crawl',
            'description': 'Open repository of web crawl data with petabytes of data collected over 10+ years.',
            'url': 'https://commoncrawl.org/',
            'domain': 'Natural Language Processing',
            'modality': 'text',
            'category': 'NLP Corpus',
            'tasks': ['pretraining', 'language modeling'],
            'samples': 3200000000,  # 3.2B web pages per crawl
            'size_gb': 300000,  # petabytes
            'license': 'CC0',
            'year': 2011,
        },
        {
            'id': 'the-pile',
            'name': 'The Pile',
            'description': '825GB diverse text dataset for language modeling containing 22 curated high-quality subsets.',
            'url': 'https://pile.eleuther.ai/',
            'domain': 'Natural Language Processing',
            'modality': 'text',
            'category': 'NLP Corpus',
            'tasks': ['language modeling', 'pretraining'],
            'samples': 260000000000,  # 260B tokens
            'size_gb': 825,
            'license': 'Various',
            'year': 2020,
        },
        {
            'id': 'glue',
            'name': 'GLUE Benchmark',
            'description': 'General Language Understanding Evaluation benchmark with 9 NLU tasks.',
            'url': 'https://gluebenchmark.com/',
            'domain': 'Natural Language Processing',
            'modality': 'text',
            'category': 'NLP Benchmark',
            'tasks': ['sentiment analysis', 'text classification', 'NLI'],
            'samples': 300000,
            'size_gb': 0.1,
            'license': 'Various',
            'year': 2018,
        },
        {
            'id': 'super-glue',
            'name': 'SuperGLUE Benchmark',
            'description': 'Harder successor to GLUE with more challenging language understanding tasks.',
            'url': 'https://super.gluebenchmark.com/',
            'domain': 'Natural Language Processing',
            'modality': 'text',
            'category': 'NLP Benchmark',
            'tasks': ['reading comprehension', 'coreference', 'reasoning'],
            'samples': 100000,
            'size_gb': 0.05,
            'license': 'Various',
            'year': 2019,
        },
        {
            'id': 'squad',
            'name': 'SQuAD',
            'description': 'Stanford Question Answering Dataset with 100K+ question-answer pairs.',
            'url': 'https://rajpurkar.github.io/SQuAD-explorer/',
            'domain': 'Natural Language Processing',
            'modality': 'text',
            'category': 'NLP Benchmark',
            'tasks': ['question answering', 'reading comprehension'],
            'samples': 150000,
            'size_gb': 0.03,
            'license': 'CC BY-SA 4.0',
            'year': 2016,
        },
        
        # ===== Speech & Audio =====
        {
            'id': 'librispeech',
            'name': 'LibriSpeech',
            'description': 'Large-scale corpus of read English speech derived from audiobooks. 1000 hours of speech.',
            'url': 'https://www.openslr.org/12',
            'domain': 'Speech and Audio',
            'modality': 'audio',
            'category': 'Speech Benchmark',
            'tasks': ['speech recognition', 'ASR'],
            'samples': 1000,  # hours
            'size_gb': 60,
            'license': 'CC BY 4.0',
            'year': 2015,
        },
        {
            'id': 'common-voice',
            'name': 'Mozilla Common Voice',
            'description': 'Crowdsourced multilingual speech dataset in 100+ languages.',
            'url': 'https://commonvoice.mozilla.org/',
            'domain': 'Speech and Audio',
            'modality': 'audio',
            'category': 'Speech Dataset',
            'tasks': ['speech recognition', 'ASR', 'multilingual'],
            'samples': 20000,  # hours across all languages
            'size_gb': 100,
            'license': 'CC0',
            'year': 2017,
        },
        
        # ===== Multimodal =====
        {
            'id': 'vqa',
            'name': 'Visual Question Answering (VQA)',
            'description': 'Dataset for visual question answering with 250K+ images and 760K+ questions.',
            'url': 'https://visualqa.org/',
            'domain': 'Multimodal',
            'modality': 'multimodal',
            'category': 'Multimodal Benchmark',
            'tasks': ['visual question answering', 'vision-language'],
            'samples': 250000,
            'size_gb': 25,
            'license': 'CC BY 4.0',
            'year': 2015,
        },
        
        # ===== Tabular / Benchmarks =====
        {
            'id': 'uci-ml-repo',
            'name': 'UCI Machine Learning Repository',
            'description': 'Collection of 600+ ML datasets maintained by UC Irvine. Classic benchmark source.',
            'url': 'https://archive.ics.uci.edu/ml/index.php',
            'domain': 'Tabular',
            'modality': 'tabular',
            'category': 'ML Benchmark',
            'tasks': ['classification', 'regression', 'clustering'],
            'samples': 600,  # Number of datasets
            'size_gb': 10,
            'license': 'Various',
            'year': 1987,
        },
        {
            'id': 'mlcommons-mlperf',
            'name': 'MLCommons MLPerf',
            'description': 'Industry-standard ML benchmark suite for training and inference performance.',
            'url': 'https://mlcommons.org/en/',
            'domain': 'General',
            'modality': 'multimodal',
            'category': 'ML Benchmark',
            'tasks': ['training benchmark', 'inference benchmark'],
            'samples': 0,
            'size_gb': 0,
            'license': 'Apache 2.0',
            'year': 2018,
        },
    ]
