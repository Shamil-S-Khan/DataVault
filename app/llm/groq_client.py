"""
Groq LLM client implementation.
Uses the OpenAI-compatible Groq API for text generation and chat.
"""
import httpx
import logging
from typing import List, Dict, Any, Optional
from tenacity import retry, stop_after_attempt, wait_exponential

from app.config import settings

logger = logging.getLogger(__name__)

class GroqClient:
    """Client for interacting with Groq LPU API."""
    
    def __init__(self):
        """Initialize Groq client."""
        self.api_key = settings.groq_api_key
        self.base_url = "https://api.groq.com/openai/v1"
        self.model = "llama-3.3-70b-versatile" # Premium versatile model
        
        if self.api_key:
            self.enabled = True
            logger.info(f"Groq Client initialized with model: {self.model}")
        else:
            self.enabled = False
            logger.warning("Groq API key not found. Groq features will be disabled.")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True
    )
    async def generate_text(self, prompt: str, system_prompt: Optional[str] = None) -> str:
        """Generate text using Groq API."""
        if not self.enabled:
            return "Groq API is not configured."

        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{self.base_url}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json"
                    },
                    json={
                        "model": self.model,
                        "messages": messages,
                        "temperature": 0.5,
                        "max_tokens": 1024
                    }
                )
                
                if response.status_code == 429:
                    logger.error("Groq Rate Limit exceeded (429).")
                    raise Exception("Groq API rate limit exceeded. Please wait a moment and try again.")
                
                if response.status_code != 200:
                    logger.error(f"Groq API error: {response.status_code} - {response.text}")
                    response.raise_for_status()
                
                result = response.json()
                return result["choices"][0]["message"]["content"]
        except Exception as e:
            logger.error(f"Groq generation failed: {e}")
            raise

    async def chat(self, messages: List[Dict[str, str]]) -> str:
        """Continue a conversation using Groq API."""
        if not self.enabled:
            return "Groq API is not configured."

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{self.base_url}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json"
                    },
                    json={
                        "model": self.model,
                        "messages": messages,
                        "temperature": 0.7,
                        "max_tokens": 2048
                    }
                )
                
                if response.status_code == 429:
                    logger.error("Groq Rate Limit exceeded (429).")
                    raise Exception("Groq API rate limit exceeded. Please wait a moment and try again.")
                
                if response.status_code != 200:
                    logger.error(f"Groq API error: {response.status_code} - {response.text}")
                    response.raise_for_status()
                
                result = response.json()
                return result["choices"][0]["message"]["content"]
        except Exception as e:
            logger.error(f"Groq chat failed: {e}")
            raise

    async def generate_summary(self, text: str) -> str:
        """Generate a concise summary of the provided text."""
        prompt = f"Please provide a concise, professional summary of the following text:\n\n{text}"
        system_prompt = "You are an expert technical writer summarizing dataset information."
        return await self.generate_text(prompt, system_prompt)

    async def extract_insights(self, metadata: Dict[str, Any]) -> str:
        """Extract key insights from dataset metadata."""
        prompt = f"Analyze this dataset metadata and provide 3-5 key technical insights for a data scientist:\n\n{metadata}"
        system_prompt = "You are a senior data scientist and machine learning expert."
        return await self.generate_text(prompt, system_prompt)

# Global instance
groq_client = GroqClient()
