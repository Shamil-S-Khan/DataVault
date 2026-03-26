"""
HuggingFace Inference API client for dataset analysis.
Alternative to Gemini API with free tier limits.
Uses HuggingFace Hub's serverless inference API.
"""
from typing import List, Optional, Dict, Any
import logging
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential
from app.config import settings
from app.db.redis_client import redis_client

logger = logging.getLogger(__name__)


class HuggingFaceClient:
    """Client for HuggingFace Inference API with caching."""
    
    # Models for text generation (free tier compatible)
    MODELS = {
        'mistral': 'mistralai/Mistral-7B-Instruct-v0.3',
        'llama': 'meta-llama/Llama-3.2-1B-Instruct',
        'qwen': 'Qwen/Qwen2.5-7B-Instruct'
    }
    
    def __init__(self):
        """Initialize HuggingFace client."""
        self.api_key = settings.huggingface_api_key or getattr(settings, 'hf_token', None)
        self.base_url = "https://router.huggingface.co/v1/chat/completions"
        
        # Default to Llama 3.2 1B as it's almost certainly free and fast
        self.model = self.MODELS.get('llama')
        
        if self.api_key:
            self.enabled = True
            logger.info(f"HuggingFace Router API initialized with {self.model}")
        else:
            self.enabled = False
            logger.warning("HuggingFace API key not found, LLM features disabled")
    
    async def chat(
        self,
        messages: List[Dict[str, str]],
        max_tokens: int = 500,
        temperature: float = 0.7,
        model: str = None
    ) -> Optional[str]:
        """
        Generate a chat response using HuggingFace Router.
        
        Args:
            messages: List of chat messages [{"role": "user", "content": "..."}]
            max_tokens: Maximum tokens to generate
            temperature: Sampling temperature
            model: Optional specific model to use
            
        Returns:
            Generated response or None on failure
        """
        if not self.enabled:
            return None
        
        use_model = model or self.model
        url = self.base_url
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": use_model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "stream": False
        }
        
        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                response = await client.post(url, headers=headers, json=payload)
                
                if response.status_code == 200:
                    result = response.json()
                    if "choices" in result and len(result["choices"]) > 0:
                        return result["choices"][0]["message"]["content"].strip()
                    return None
                else:
                    logger.warning(f"HF Router chat error: {response.status_code} - {response.text[:200]}")
                    return None
        except Exception as e:
            logger.error(f"HF Router chat exception: {e}")
            return None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=60)
    )
    async def generate_text(
        self,
        prompt: str,
        max_tokens: int = 500,
        temperature: float = 0.7,
        model: str = None
    ) -> Optional[str]:
        """
        Generate text using HuggingFace Router (OpenAI-compatible).
        
        Args:
            prompt: Input prompt
            max_tokens: Maximum tokens to generate
            temperature: Sampling temperature
            model: Optional specific model to use
            
        Returns:
            Generated text or None on failure
        """
        if not self.enabled:
            return None
        
        use_model = model or self.model
        url = self.base_url
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        logger.info(f"HF Router Request Model: {use_model}")
        
        payload = {
            "model": use_model,
            "messages": [
                {"role": "user", "content": prompt}
            ],
            "max_tokens": max_tokens,
            "temperature": temperature,
            "stream": False
        }
        
        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                response = await client.post(url, headers=headers, json=payload)
                
                if response.status_code == 200:
                    result = response.json()
                    logger.info("HF Router Success Response received")
                    
                    # OpenAI format: choices[0].message.content
                    if "choices" in result and len(result["choices"]) > 0:
                        content = result["choices"][0]["message"]["content"]
                        return content.strip()
                    return None
                
                elif response.status_code == 503:
                    logger.warning(f"Provider for {use_model} is loading/unavailable, will retry...")
                    raise Exception("Service Unavailable, retrying...")
                
                else:
                    error_text = response.text
                    logger.warning(f"HuggingFace Router error: {response.status_code} - URL: {url} - Model: {use_model} - Body: {error_text[:300]}...")
                    return None
                    
        except Exception as e:
            logger.warning(f"HuggingFace Router exception: {e}")
            raise
    
    async def generate_summary(
        self,
        dataset_name: str,
        description: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """Generate dataset summary."""
        cache_key = f"hf:summary:{dataset_name}"
        cached = await redis_client.get(cache_key)
        if cached:
            return cached
        
        prompt = f"""Summarize this dataset in 150-250 words:

Dataset Name: {dataset_name}
Description: {description}
{f'Metadata: {metadata}' if metadata else ''}

Focus on what the dataset contains, its use cases, and why it's valuable for ML/AI research. Be informative and concise."""
        
        try:
            summary = await self.generate_text(prompt, max_tokens=400, temperature=0.7)
            
            if summary:
                await redis_client.set(cache_key, summary, ttl=7 * 24 * 60 * 60)
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to generate summary for {dataset_name}: {e}")
            return None


# Global client instance
huggingface_client = HuggingFaceClient()
