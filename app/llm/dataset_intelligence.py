"""
Dataset Intelligence Layer using configurable LLM API.
Extracts semantic, structural, and usability information from datasets.
Supports Gemini and HuggingFace Inference API.
"""
from typing import Optional, List, Dict, Any
import logging
import json
from datetime import datetime
from pydantic import BaseModel, Field
from tenacity import retry, stop_after_attempt, wait_exponential

from app.config import settings
from app.db.redis_client import redis_client

logger = logging.getLogger(__name__)


# ============================================================================
# Pydantic Models for Structured Output
# ============================================================================

class FieldSemantics(BaseModel):
    """Semantic information about a dataset field."""
    name: str
    semantic_meaning: str
    data_type: str  # text, numeric, categorical, boolean, image, audio, etc.
    role: str  # input, label, metadata, identifier


class LabelAnalysis(BaseModel):
    """Analysis of dataset labels and annotations."""
    type: str  # single_label, multi_label, regression, generative, etc.
    categories: Optional[List[str]] = None
    annotation_method: Optional[str] = None  # human, synthetic, generated, crowdsourced


class DatasetIntelligence(BaseModel):
    """Complete intelligence analysis of a dataset."""
    summary: str = Field(description="Plain-English description of the dataset")
    tasks: List[str] = Field(description="Primary ML tasks (e.g., classification, QA)")
    use_cases: List[str] = Field(description="Intended use cases")
    modalities: List[str] = Field(description="Data modalities (text, image, audio, etc.)")
    domain: str = Field(description="Primary domain (NLP, Vision, etc.)")
    subdomains: List[str] = Field(description="Relevant sub-domains")
    fields: List[FieldSemantics] = Field(description="Field-level semantic analysis")
    labels: LabelAnalysis = Field(description="Label and annotation analysis")
    difficulty: str = Field(description="Difficulty level: easy, medium, hard")
    quality_notes: List[str] = Field(description="Quality signals and observations")
    ethical_flags: List[str] = Field(description="Ethical or sensitivity concerns")
    tags: List[str] = Field(description="Auto-generated searchable tags")
    
    # Metadata
    analyzed_at: datetime = Field(default_factory=datetime.utcnow)
    version: str = "1.0"


# ============================================================================
# Dataset Intelligence Analyzer
# ============================================================================

class DatasetIntelligenceAnalyzer:
    """Analyzes datasets using configurable LLM (Gemini or HuggingFace) to extract semantic intelligence."""
    
    def __init__(self):
        # Select LLM client based on configuration
        self.llm_provider = getattr(settings, 'llm_provider', 'huggingface')
        
        if self.llm_provider == 'gemini':
            from app.llm.gemini_client import gemini_client
            self.client = gemini_client
            logger.info("Using Gemini API for dataset intelligence")
        else:
            from app.llm.huggingface_client import huggingface_client
            self.client = huggingface_client
            logger.info("Using HuggingFace Inference API for dataset intelligence")
        
        self.cache_ttl = 30 * 24 * 60 * 60  # 30 days
    
    async def analyze_dataset(
        self,
        dataset_id: str,
        dataset_name: str,
        description: str,
        schema: Optional[Dict[str, Any]] = None,
        samples: Optional[List[Dict[str, Any]]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        force_refresh: bool = False
    ) -> Optional[DatasetIntelligence]:
        """
        Analyze a dataset and extract comprehensive intelligence.
        
        Args:
            dataset_id: Unique dataset identifier
            dataset_name: Dataset name
            description: Dataset description/README
            schema: Dataset schema (field names and types)
            samples: Sample rows (limited, representative)
            metadata: Additional metadata
            force_refresh: Force re-analysis even if cached
            
        Returns:
            DatasetIntelligence object or None on failure
        """
        # Check cache first
        if not force_refresh:
            cached = await self._get_cached_intelligence(dataset_id)
            if cached:
                logger.info(f"Using cached intelligence for {dataset_name}")
                return cached
        
        if not self.client.enabled:
            logger.warning("LLM API not enabled, skipping intelligence analysis")
            return None
        
        try:
            # Build comprehensive prompt
            prompt = self._build_analysis_prompt(
                dataset_name, description, schema, samples, metadata
            )
            
            # Call LLM API
            logger.info(f"Analyzing dataset: {dataset_name} using {self.llm_provider}")
            response = await self.client.generate_text(
                prompt,
                max_tokens=2000,
                temperature=0.3  # Lower temperature for more consistent output
            )
            
            if not response:
                logger.error(f"No response from LLM for {dataset_name}")
                return None
            
            # Parse and validate response
            intelligence = self._parse_llm_response(response)
            
            if intelligence:
                # Cache the result
                await self._cache_intelligence(dataset_id, intelligence)
                logger.info(f"Successfully analyzed {dataset_name}")
                return intelligence
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to analyze dataset {dataset_name}: {e}")
            return None
    
    def _build_analysis_prompt(
        self,
        name: str,
        description: str,
        schema: Optional[Dict[str, Any]],
        samples: Optional[List[Dict[str, Any]]],
        metadata: Optional[Dict[str, Any]]
    ) -> str:
        """Build comprehensive analysis prompt for Gemini."""
        
        # Determine if this is a description-only analysis
        has_samples = samples and len(samples) > 0
        has_schema = schema and len(schema) > 0
        
        # Detect modality hints from name/description
        name_lower = name.lower()
        desc_lower = description.lower() if description else ""
        combined_text = f"{name_lower} {desc_lower}"
        
        modality_hint = ""
        if any(kw in combined_text for kw in ['image', 'photo', 'picture', 'visual', 'vision', 'imagenet', 'coco', 'cifar']):
            modality_hint = "This appears to be an IMAGE/VISION dataset."
        elif any(kw in combined_text for kw in ['audio', 'speech', 'sound', 'music', 'voice', 'acoustic']):
            modality_hint = "This appears to be an AUDIO/SPEECH dataset."
        elif any(kw in combined_text for kw in ['video', 'clip', 'movie', 'frames']):
            modality_hint = "This appears to be a VIDEO dataset."
        elif any(kw in combined_text for kw in ['text', 'nlp', 'language', 'corpus', 'sentiment', 'translation']):
            modality_hint = "This appears to be a TEXT/NLP dataset."
        elif any(kw in combined_text for kw in ['tabular', 'csv', 'table', 'structured', 'features']):
            modality_hint = "This appears to be a TABULAR dataset."
        
        prompt = f"""You are an expert ML dataset analyst. Analyze the following dataset and extract comprehensive semantic intelligence.

Dataset Name: {name}

Description:
{description if description else "No description provided."}

"""
        
        # Add modality hint if detected
        if modality_hint:
            prompt += f"**Modality Hint**: {modality_hint}\n\n"
        
        # Add schema information
        if has_schema:
            prompt += f"Schema/Fields:\n"
            if isinstance(schema, dict):
                for field_name, field_info in schema.items():
                    prompt += f"- {field_name}: {field_info}\n"
            else:
                prompt += f"{schema}\n"
            prompt += "\n"
        
        # Add sample data if available
        if has_samples:
            prompt += f"Sample Data (first {min(len(samples), 3)} rows):\n"
            for i, sample in enumerate(samples[:3], 1):
                prompt += f"\nSample {i}:\n{json.dumps(sample, indent=2, default=str)}\n"
            prompt += "\n"
        else:
            # Give explicit guidance for no-sample analysis
            prompt += """**Note**: No sample data is available for this dataset. 
Analyze based on the dataset name, description, and any available metadata.
For image/audio/video datasets, this is expected - focus on the semantic understanding from the description.\n\n"""
        
        # Add metadata if available
        if metadata:
            # Filter out very long or nested metadata
            filtered_metadata = {}
            for k, v in metadata.items():
                if k not in ['schema', 'samples', 'readme'] and not isinstance(v, (list, dict)):
                    filtered_metadata[k] = v
                elif isinstance(v, list) and len(v) <= 10:
                    filtered_metadata[k] = v
            
            if filtered_metadata:
                prompt += f"Additional Metadata:\n{json.dumps(filtered_metadata, indent=2, default=str)}\n\n"
        
        # Analysis instructions - enhanced for description-only analysis
        prompt += """
TASK: Extract the following information and return ONLY a valid JSON object with this exact structure:

{
  "summary": "Plain-English description (2-3 sentences) of what the dataset contains and its main purpose",
  "tasks": ["primary ML tasks - e.g., classification, object_detection, question_answering, text_generation, speech_recognition, image_captioning, etc."],
  "use_cases": ["intended use cases - e.g., training, evaluation, benchmarking, fine_tuning, research"],
  "modalities": ["data modalities - e.g., text, image, audio, video, tabular, multimodal"],
  "domain": "Primary domain - e.g., Computer Vision, NLP, Audio/Speech, Healthcare, Finance, etc.",
  "subdomains": ["relevant sub-domains"],
  "fields": [
    {
      "name": "field_name",
      "semantic_meaning": "what this field represents",
      "data_type": "text|numeric|categorical|boolean|image|audio|video|structured",
      "role": "input|label|metadata|identifier"
    }
  ],
  "labels": {
    "type": "single_label|multi_label|regression|generative|ranking|detection|segmentation|transcription|etc.",
    "categories": ["known class values if applicable"],
    "annotation_method": "human|synthetic|generated|crowdsourced|automatic|unknown"
  },
  "difficulty": "easy|medium|hard",
  "quality_notes": ["observations about data quality, consistency, completeness"],
  "ethical_flags": ["any privacy, bias, or ethical concerns - leave empty if none detected"],
  "tags": ["short searchable keywords - include task, modality, domain, and dataset-specific terms"]
}

IMPORTANT GUIDELINES:
1. Return ONLY the JSON object, no additional text before or after
2. Be specific and accurate based on the provided information
3. For fields you're uncertain about, make reasonable inferences from the name and description
4. Keep summary concise (2-3 sentences max)
5. Generate 5-10 relevant tags
6. Focus on semantic understanding, not statistics

SPECIAL HANDLING:
- For IMAGE datasets: infer fields like "image" (input), "label/class" (output), bounding boxes, etc.
- For AUDIO datasets: infer fields like "audio_file" (input), "transcription" or "label" (output)
- For VIDEO datasets: infer temporal aspects and frame-level annotations
- If no schema/samples available: make informed inferences from the description
- Always provide a complete analysis even with limited information

JSON Response:"""
        
        return prompt
    
    def _parse_llm_response(self, response: str) -> Optional[DatasetIntelligence]:
        """Parse and validate LLM response into DatasetIntelligence object."""
        try:
            # Extract JSON from response (handle cases where LLM adds extra text)
            response = response.strip()
            
            # Find JSON object boundaries
            start_idx = response.find('{')
            end_idx = response.rfind('}')
            
            if start_idx == -1 or end_idx == -1:
                logger.error("No JSON object found in response")
                logger.debug(f"Response preview: {response[:200]}")
                return None
            
            json_str = response[start_idx:end_idx + 1]
            
            # Clean up common JSON formatting issues
            # Fix trailing commas before closing braces/brackets
            import re
            json_str = re.sub(r',(\s*[}\]])', r'\1', json_str)
            # Fix single quotes to double quotes (common LLM mistake)
            json_str = json_str.replace("'", '"')
            # Remove any control characters
            json_str = ''.join(char for char in json_str if ord(char) >= 32 or char in '\n\r\t')
            
            # Parse JSON
            try:
                data = json.loads(json_str)
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error: {e}")
                logger.debug(f"Problematic JSON (first 500 chars): {json_str[:500]}")
                
                # Try to fix common issues and parse again
                # Remove trailing commas more aggressively
                json_str = re.sub(r',\s*}', '}', json_str)
                json_str = re.sub(r',\s*]', ']', json_str)
                
                try:
                    data = json.loads(json_str)
                    logger.info("Successfully parsed JSON after cleanup")
                except json.JSONDecodeError as e2:
                    logger.error(f"JSON still invalid after cleanup: {e2}")
                    return None
            
            # Validate and create DatasetIntelligence object
            intelligence = DatasetIntelligence(**data)
            
            return intelligence
            
        except Exception as e:
            logger.error(f"Failed to validate intelligence data: {e}")
            logger.debug(f"Response that caused error: {response[:500] if response else 'None'}")
            return None
    
    async def _get_cached_intelligence(self, dataset_id: str) -> Optional[DatasetIntelligence]:
        """Retrieve cached intelligence from Redis."""
        cache_key = f"intelligence:{dataset_id}"
        
        try:
            cached_data = await redis_client.get(cache_key)
            if cached_data:
                # Parse cached JSON
                if isinstance(cached_data, str):
                    data = json.loads(cached_data)
                else:
                    data = cached_data
                
                return DatasetIntelligence(**data)
        except Exception as e:
            logger.error(f"Error retrieving cached intelligence: {e}")
        
        return None
    
    async def _cache_intelligence(self, dataset_id: str, intelligence: DatasetIntelligence):
        """Cache intelligence data in Redis."""
        cache_key = f"intelligence:{dataset_id}"
        
        try:
            # Convert to dict and then to JSON
            data = intelligence.model_dump()
            
            # Convert datetime to ISO string for JSON serialization
            if 'analyzed_at' in data and isinstance(data['analyzed_at'], datetime):
                data['analyzed_at'] = data['analyzed_at'].isoformat()
            
            await redis_client.set(cache_key, json.dumps(data), ttl=self.cache_ttl)
            logger.debug(f"Cached intelligence for {dataset_id}")
        except Exception as e:
            logger.error(f"Error caching intelligence: {e}")


# Global analyzer instance
dataset_intelligence_analyzer = DatasetIntelligenceAnalyzer()
