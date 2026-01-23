"""
Smart Query Language Parser.
Parses DSL queries like: task:image-classification license:MIT size>5000 modality:image
"""
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
import re
import logging

logger = logging.getLogger(__name__)


@dataclass
class QueryToken:
    """A parsed query token."""
    field: str
    operator: str  # ':', '=', '>', '<', '>=',' <='
    value: str
    raw: str


class QueryParser:
    """
    Parse DataVault Smart Query Language (DSL).
    
    Supported syntax:
    - field:value         (exact match)
    - field=value         (exact match)
    - field>value         (greater than, for numeric)
    - field<value         (less than, for numeric)
    - field>=value        (greater or equal)
    - field<=value        (less or equal)
    - "quoted phrase"     (text search)
    - word                (text search)
    
    Supported fields:
    - task: image-classification, object-detection, text-classification, etc.
    - domain: computer-vision, nlp, audio, tabular, etc.
    - modality: image, text, audio, video, tabular
    - license: mit, apache-2.0, cc-by-4.0, etc.
    - platform: huggingface, kaggle, openml, etc.
    - downloads: numeric (use >, <, >=, <=)
    - likes: numeric
    - size: numeric (sample count)
    """
    
    # Field mappings to MongoDB paths
    FIELD_MAPPINGS = {
        'task': 'intelligence.tasks',
        'domain': 'domain',
        'modality': 'modality',
        'license': 'license',
        'platform': 'source.platform',
        'downloads': 'source.source_metadata.downloads',
        'likes': 'source.source_metadata.likes',
        'size': 'size.samples',
        'name': 'canonical_name',
        'author': 'source.source_metadata.author',
        'tag': 'source.source_metadata.tags',
    }
    
    # Numeric fields
    NUMERIC_FIELDS = {'downloads', 'likes', 'size'}
    
    # Array fields (use $in operator)
    ARRAY_FIELDS = {'task', 'tag'}
    
    # Valid operators
    OPERATORS = ['>=', '<=', '>', '<', ':', '=']
    
    def parse(self, query_string: str) -> Dict[str, Any]:
        """
        Parse a query string into MongoDB query.
        
        Args:
            query_string: DSL query like "task:image modality:image size>1000"
            
        Returns:
            Dictionary with:
            - mongodb_query: The MongoDB query dict
            - tokens: Parsed tokens
            - text_search: Free text to search
            - errors: Any parsing errors
        """
        tokens = []
        text_parts = []
        errors = []
        
        # Tokenize the query
        remaining = query_string.strip()
        
        while remaining:
            remaining = remaining.strip()
            if not remaining:
                break
            
            # Try to match quoted phrase
            quoted_match = re.match(r'^"([^"]+)"', remaining)
            if quoted_match:
                text_parts.append(quoted_match.group(1))
                remaining = remaining[quoted_match.end():]
                continue
            
            # Try to match field:operator:value pattern
            field_match = None
            for op in self.OPERATORS:
                pattern = rf'^(\w+){re.escape(op)}(\S+)'
                field_match = re.match(pattern, remaining)
                if field_match:
                    field = field_match.group(1).lower()
                    value = field_match.group(2)
                    
                    if field in self.FIELD_MAPPINGS:
                        tokens.append(QueryToken(
                            field=field,
                            operator=op,
                            value=value,
                            raw=field_match.group(0)
                        ))
                    else:
                        errors.append(f"Unknown field: {field}")
                    
                    remaining = remaining[field_match.end():]
                    break
            
            if field_match:
                continue
            
            # Match a word (free text search)
            word_match = re.match(r'^(\S+)', remaining)
            if word_match:
                text_parts.append(word_match.group(1))
                remaining = remaining[word_match.end():]
        
        # Build MongoDB query
        mongodb_query = self._build_mongodb_query(tokens, text_parts)
        
        return {
            'mongodb_query': mongodb_query,
            'tokens': [{'field': t.field, 'operator': t.operator, 'value': t.value} for t in tokens],
            'text_search': ' '.join(text_parts) if text_parts else None,
            'errors': errors,
            'valid': len(errors) == 0
        }
    
    def _build_mongodb_query(
        self, 
        tokens: List[QueryToken], 
        text_parts: List[str]
    ) -> Dict[str, Any]:
        """Build MongoDB query from tokens."""
        query = {}
        
        for token in tokens:
            mongo_field = self.FIELD_MAPPINGS[token.field]
            
            if token.field in self.NUMERIC_FIELDS:
                # Numeric comparison
                try:
                    value = int(token.value)
                except ValueError:
                    try:
                        value = float(token.value)
                    except ValueError:
                        continue
                
                if token.operator == '>':
                    query[mongo_field] = {'$gt': value}
                elif token.operator == '<':
                    query[mongo_field] = {'$lt': value}
                elif token.operator == '>=':
                    query[mongo_field] = {'$gte': value}
                elif token.operator == '<=':
                    query[mongo_field] = {'$lte': value}
                else:
                    query[mongo_field] = value
                    
            elif token.field in self.ARRAY_FIELDS:
                # Array contains (case-insensitive)
                query[mongo_field] = {'$regex': token.value, '$options': 'i'}
                
            else:
                # String match (case-insensitive)
                if token.operator in [':', '=']:
                    query[mongo_field] = {'$regex': f'^{re.escape(token.value)}$', '$options': 'i'}
        
        # Add text search if present
        if text_parts:
            text_query = ' '.join(text_parts)
            query['$or'] = [
                {'canonical_name': {'$regex': text_query, '$options': 'i'}},
                {'description': {'$regex': text_query, '$options': 'i'}}
            ]
        
        return query
    
    def get_suggestions(self, partial: str) -> List[Dict[str, str]]:
        """
        Get autocomplete suggestions for partial query.
        
        Args:
            partial: Partial query string
            
        Returns:
            List of suggestions with field, value, and description
        """
        suggestions = []
        partial_lower = partial.lower().strip()
        
        # If empty or just started typing
        if not partial_lower or len(partial_lower) < 2:
            # Show field suggestions
            suggestions = [
                {'text': 'task:', 'description': 'Filter by task (e.g., task:image-classification)'},
                {'text': 'domain:', 'description': 'Filter by domain (e.g., domain:nlp)'},
                {'text': 'modality:', 'description': 'Filter by modality (e.g., modality:image)'},
                {'text': 'license:', 'description': 'Filter by license (e.g., license:mit)'},
                {'text': 'platform:', 'description': 'Filter by source (e.g., platform:huggingface)'},
                {'text': 'downloads>', 'description': 'Filter by downloads (e.g., downloads>10000)'},
                {'text': 'size>', 'description': 'Filter by sample count (e.g., size>5000)'},
            ]
        
        # Field-specific value suggestions
        elif partial_lower.startswith('task:'):
            suggestions = [
                {'text': 'task:image-classification', 'description': 'Image classification tasks'},
                {'text': 'task:object-detection', 'description': 'Object detection tasks'},
                {'text': 'task:text-classification', 'description': 'Text classification tasks'},
                {'text': 'task:question-answering', 'description': 'QA tasks'},
                {'text': 'task:text-generation', 'description': 'Text generation tasks'},
            ]
        elif partial_lower.startswith('domain:'):
            suggestions = [
                {'text': 'domain:computer-vision', 'description': 'Computer vision datasets'},
                {'text': 'domain:nlp', 'description': 'Natural language processing'},
                {'text': 'domain:audio', 'description': 'Audio/speech datasets'},
                {'text': 'domain:medical', 'description': 'Medical/healthcare datasets'},
            ]
        elif partial_lower.startswith('modality:'):
            suggestions = [
                {'text': 'modality:image', 'description': 'Image datasets'},
                {'text': 'modality:text', 'description': 'Text datasets'},
                {'text': 'modality:audio', 'description': 'Audio datasets'},
                {'text': 'modality:tabular', 'description': 'Tabular/CSV datasets'},
            ]
        elif partial_lower.startswith('license:'):
            suggestions = [
                {'text': 'license:mit', 'description': 'MIT License (permissive)'},
                {'text': 'license:apache-2.0', 'description': 'Apache 2.0 License'},
                {'text': 'license:cc-by-4.0', 'description': 'Creative Commons Attribution'},
                {'text': 'license:cc0', 'description': 'Public Domain'},
            ]
        elif partial_lower.startswith('platform:'):
            suggestions = [
                {'text': 'platform:huggingface', 'description': 'HuggingFace datasets'},
                {'text': 'platform:kaggle', 'description': 'Kaggle datasets'},
                {'text': 'platform:openml', 'description': 'OpenML datasets'},
            ]
        
        return suggestions
    
    def validate_query(self, query_string: str) -> Dict[str, Any]:
        """
        Validate a query and return helpful error messages.
        
        Returns:
            Dictionary with valid flag and any errors
        """
        result = self.parse(query_string)
        
        validation = {
            'valid': result['valid'],
            'errors': result['errors'],
            'warnings': [],
            'suggestions': []
        }
        
        # Check for common issues
        if 'size:' in query_string:
            validation['warnings'].append("Use 'size>' or 'size<' for size comparisons")
        
        if 'download:' in query_string:
            validation['suggestions'].append("Did you mean 'downloads>'?")
        
        return validation


# Singleton instance
query_parser = QueryParser()
