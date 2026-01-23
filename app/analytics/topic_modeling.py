"""
Topic modeling using Gensim LDA.
Discovers latent topics in dataset descriptions.
"""
from typing import List, Dict, Any, Optional, Tuple
import logging
from gensim import corpora, models
from gensim.models import CoherenceModel
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import re
from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId
from datetime import datetime

logger = logging.getLogger(__name__)

# Download NLTK data if not already present
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')
    nltk.download('stopwords')


class TopicModeler:
    """LDA topic modeling for dataset descriptions."""
    
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.stop_words = set(stopwords.words('english'))
        # Add domain-specific stop words
        self.stop_words.update([
            'dataset', 'data', 'datasets', 'collection', 'contains',
            'used', 'using', 'use', 'based', 'provide', 'provides'
        ])
    
    def preprocess_text(self, text: str) -> List[str]:
        """
        Preprocess text for topic modeling.
        
        Args:
            text: Input text
            
        Returns:
            List of preprocessed tokens
        """
        # Convert to lowercase
        text = text.lower()
        
        # Remove URLs
        text = re.sub(r'http\S+|www\S+', '', text)
        
        # Remove special characters and digits
        text = re.sub(r'[^a-zA-Z\s]', '', text)
        
        # Tokenize
        tokens = word_tokenize(text)
        
        # Remove stopwords and short tokens
        tokens = [
            token for token in tokens
            if token not in self.stop_words and len(token) > 3
        ]
        
        return tokens
    
    async def prepare_corpus(
        self,
        limit: Optional[int] = None
    ) -> Tuple[List[List[str]], corpora.Dictionary]:
        """
        Prepare corpus from dataset descriptions.
        
        Args:
            limit: Optional limit on number of datasets
            
        Returns:
            Tuple of (documents, dictionary)
        """
        logger.info("Preparing corpus for topic modeling")
        
        # Fetch datasets
        cursor = self.db.datasets.find({})
        if limit:
            cursor = cursor.limit(limit)
        
        datasets = await cursor.to_list(length=limit or 10000)
        
        # Preprocess documents
        documents = []
        for dataset in datasets:
            text = f"{dataset.get('canonical_name', '')} {dataset.get('description', '')}"
            tokens = self.preprocess_text(text)
            if tokens:  # Only add non-empty documents
                documents.append(tokens)
        
        logger.info(f"Prepared {len(documents)} documents")
        
        # Create dictionary
        dictionary = corpora.Dictionary(documents)
        
        # Filter extremes
        dictionary.filter_extremes(no_below=2, no_above=0.5)
        
        return documents, dictionary
    
    def train_lda_model(
        self,
        documents: List[List[str]],
        dictionary: corpora.Dictionary,
        num_topics: int = 10,
        passes: int = 15,
        iterations: int = 400
    ) -> models.LdaModel:
        """
        Train LDA model.
        
        Args:
            documents: Preprocessed documents
            dictionary: Gensim dictionary
            num_topics: Number of topics to discover
            passes: Number of passes through corpus
            iterations: Number of iterations
            
        Returns:
            Trained LDA model
        """
        logger.info(f"Training LDA model with {num_topics} topics")
        
        # Create corpus
        corpus = [dictionary.doc2bow(doc) for doc in documents]
        
        # Train LDA model
        lda_model = models.LdaModel(
            corpus=corpus,
            id2word=dictionary,
            num_topics=num_topics,
            random_state=42,
            update_every=1,
            chunksize=100,
            passes=passes,
            iterations=iterations,
            alpha='auto',
            per_word_topics=True
        )
        
        logger.info("LDA model training complete")
        
        return lda_model
    
    def calculate_coherence(
        self,
        model: models.LdaModel,
        documents: List[List[str]],
        dictionary: corpora.Dictionary
    ) -> float:
        """
        Calculate coherence score for model.
        
        Args:
            model: Trained LDA model
            documents: Preprocessed documents
            dictionary: Gensim dictionary
            
        Returns:
            Coherence score
        """
        coherence_model = CoherenceModel(
            model=model,
            texts=documents,
            dictionary=dictionary,
            coherence='c_v'
        )
        
        coherence_score = coherence_model.get_coherence()
        logger.info(f"Coherence score: {coherence_score:.4f}")
        
        return coherence_score
    
    def find_optimal_topics(
        self,
        documents: List[List[str]],
        dictionary: corpora.Dictionary,
        min_topics: int = 5,
        max_topics: int = 20,
        step: int = 5
    ) -> Tuple[int, float]:
        """
        Find optimal number of topics using coherence score.
        
        Args:
            documents: Preprocessed documents
            dictionary: Gensim dictionary
            min_topics: Minimum number of topics
            max_topics: Maximum number of topics
            step: Step size
            
        Returns:
            Tuple of (optimal_topics, best_coherence)
        """
        logger.info(f"Finding optimal number of topics ({min_topics}-{max_topics})")
        
        best_coherence = 0
        optimal_topics = min_topics
        
        for num_topics in range(min_topics, max_topics + 1, step):
            model = self.train_lda_model(documents, dictionary, num_topics, passes=10)
            coherence = self.calculate_coherence(model, documents, dictionary)
            
            logger.info(f"Topics: {num_topics}, Coherence: {coherence:.4f}")
            
            if coherence > best_coherence:
                best_coherence = coherence
                optimal_topics = num_topics
        
        logger.info(f"Optimal topics: {optimal_topics} (coherence: {best_coherence:.4f})")
        
        return optimal_topics, best_coherence
    
    async def save_topics_to_db(
        self,
        model: models.LdaModel,
        num_keywords: int = 10
    ):
        """
        Save discovered topics to database.
        
        Args:
            model: Trained LDA model
            num_keywords: Number of keywords per topic
        """
        logger.info("Saving topics to database")
        
        # Clear existing topics
        await self.db.topics.delete_many({})
        
        # Extract and save topics
        for topic_id in range(model.num_topics):
            # Get top keywords
            keywords = model.show_topic(topic_id, topn=num_keywords)
            keyword_list = [word for word, _ in keywords]
            
            # Generate topic name (use top 3 keywords)
            topic_name = " + ".join(keyword_list[:3])
            
            # Create topic document
            topic_doc = {
                'name': topic_name,
                'keywords': keyword_list,
                'topic_id': topic_id,
                'created_at': datetime.utcnow()
            }
            
            result = await self.db.topics.insert_one(topic_doc)
            logger.info(f"Saved topic {topic_id}: {topic_name}")
    
    async def assign_topics_to_datasets(
        self,
        model: models.LdaModel,
        dictionary: corpora.Dictionary,
        threshold: float = 0.1
    ):
        """
        Assign topics to datasets.
        
        Args:
            model: Trained LDA model
            dictionary: Gensim dictionary
            threshold: Minimum probability threshold
        """
        logger.info("Assigning topics to datasets")
        
        # Clear existing assignments
        await self.db.dataset_topics.delete_many({})
        
        # Get all datasets
        datasets = await self.db.datasets.find({}).to_list(length=10000)
        
        # Get topics from database
        topics = await self.db.topics.find({}).to_list(length=100)
        topic_map = {t['topic_id']: t['_id'] for t in topics}
        
        assigned_count = 0
        
        for dataset in datasets:
            # Preprocess text
            text = f"{dataset.get('canonical_name', '')} {dataset.get('description', '')}"
            tokens = self.preprocess_text(text)
            
            if not tokens:
                continue
            
            # Get topic distribution
            bow = dictionary.doc2bow(tokens)
            topic_dist = model.get_document_topics(bow)
            
            # Assign topics above threshold
            for topic_id, score in topic_dist:
                if score >= threshold and topic_id in topic_map:
                    await self.db.dataset_topics.insert_one({
                        'dataset_id': dataset['_id'],
                        'topic_id': topic_map[topic_id],
                        'score': float(score),
                        'created_at': datetime.utcnow()
                    })
                    assigned_count += 1
        
        logger.info(f"Assigned {assigned_count} topic associations")
    
    async def run_topic_modeling(
        self,
        num_topics: Optional[int] = None,
        auto_optimize: bool = True
    ):
        """
        Run complete topic modeling pipeline.
        
        Args:
            num_topics: Number of topics (auto-detected if None)
            auto_optimize: Whether to find optimal number of topics
        """
        logger.info("Starting topic modeling pipeline")
        
        # Prepare corpus
        documents, dictionary = await self.prepare_corpus()
        
        # Find optimal topics if requested
        if auto_optimize and num_topics is None:
            num_topics, _ = self.find_optimal_topics(documents, dictionary)
        elif num_topics is None:
            num_topics = 10
        
        # Train model
        model = self.train_lda_model(documents, dictionary, num_topics)
        
        # Calculate coherence
        coherence = self.calculate_coherence(model, documents, dictionary)
        logger.info(f"Final coherence score: {coherence:.4f}")
        
        # Save topics
        await self.save_topics_to_db(model)
        
        # Assign topics to datasets
        await self.assign_topics_to_datasets(model, dictionary)
        
        logger.info("Topic modeling pipeline complete")
        
        return model, coherence
