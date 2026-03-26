"""
xAI (Grok) API client for dataset analysis.
OpenAI-compatible implementation for xAI's API.
"""
import httpx
import json
import logging
from typing import Optional, List, Dict, Any
from tenacity import retry, stop_after_attempt, wait_exponential
from app.config import settings
from app.db.redis_client import redis_client

logger = logging.getLogger(__name__)

class XAIClient:
    """Client for xAI (Grok) API with OpenAI compatibility."""
    
    def __init__(self):
        """Initialize xAI client."""
        self.api_key = settings.grok_api_key
        self.base_url = "https://api.x.ai/v1"
        self.model = "grok-3"
        
        if self.api_key:
            self.enabled = True
            logger.info(f"xAI API client initialized with {self.model}")
        else:
            self.enabled = False
            logger.warning("GROK_API_KEY not found, xAI features disabled")
            
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=60)
    )
    async def generate_text(
        self,
        prompt: str,
        max_tokens: int = 500,
        temperature: float = 0.7
    ) -> Optional[str]:
        """Generate text using xAI API."""
        if not self.enabled:
            return None
            
        messages = [{"role": "user", "content": prompt}]
        return await self.chat(messages, max_tokens=max_tokens, temperature=temperature)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=60)
    )
    async def chat(
        self,
        messages: List[Dict[str, str]],
        max_tokens: int = 500,
        temperature: float = 0.7
    ) -> Optional[str]:
        """Generate a chat response using xAI API."""
        if not self.enabled:
            return None
            
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(
                    f"{self.base_url}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json"
                    },
                    json={
                        "model": self.model,
                        "messages": messages,
                        "max_tokens": max_tokens,
                        "temperature": temperature
                    }
                )
                
                if response.status_code != 200:
                    logger.error(f"xAI API error: {response.status_code} - {response.text}")
                    response.raise_for_status()
                
                data = response.json()
                return data["choices"][0]["message"]["content"]
                
        except Exception as e:
            logger.error(f"xAI Chat API error: {e}")
            raise

    async def generate_summary(
        self,
        dataset_name: str,
        description: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """Generate dataset summary using xAI."""
        cache_key = f"llm:summary:xai:{dataset_name}"
        cached = await redis_client.get(cache_key)
        if cached:
            return cached
            
        prompt = f"Summarize the following dataset in 150-250 words:\nName: {dataset_name}\nDescription: {description}"
        if metadata:
            prompt += f"\nMetadata: {json.dumps(metadata)}"
            
        try:
            summary = await self.generate_text(prompt, max_tokens=500)
            if summary:
                await redis_client.set(cache_key, summary, ttl=7 * 24 * 3600)
            return summary
        except Exception as e:
            logger.error(f"xAI Summary failed for {dataset_name}: {e}")
            return description[:300] + "..." if description else "No summary available."

    async def extract_insights(
        self,
        dataset_name: str,
        description: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> List[str]:
        """Extract key insights using xAI."""
        prompt = f"Extract 3-5 key insights from this dataset:\nName: {dataset_name}\nDescription: {description}"
        try:
            response = await self.generate_text(prompt, max_tokens=300)
            if response:
                return [line.strip("- ").strip() for line in response.split("\n") if line.strip()]
            return []
        except Exception as e:
            logger.error(f"xAI Insights failed for {dataset_name}: {e}")
            return []

# Global xAI client instance
xai_client = XAIClient()
