"""
Google Gemini API client for dataset analysis.
Generates summaries, insights, and semantic understanding.
"""
from typing import Optional, List, Dict, Any
import logging
from tenacity import retry, stop_after_attempt, wait_exponential
import google.generativeai as genai
from app.config import settings
from app.db.redis_client import redis_client

logger = logging.getLogger(__name__)


class GeminiClient:
    """Client for Google Gemini API with caching and rate limiting."""
    
    def __init__(self):
        """Initialize Gemini client."""
        if settings.gemini_api_key:
            genai.configure(api_key=settings.gemini_api_key)
            # Using flash model for faster responses and potentially separate quota
            self.model = genai.GenerativeModel('gemini-flash-latest')
            self.enabled = True
            logger.info("Gemini API client initialized with gemini-flash-latest")
        else:
            self.enabled = False
            logger.warning("Gemini API key not found, LLM features disabled")
    
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
        """
        Generate text using Gemini API.
        
        Args:
            prompt: Input prompt
            max_tokens: Maximum tokens to generate
            temperature: Sampling temperature
            
        Returns:
            Generated text or None on failure
        """
        if not self.enabled:
            return None
        
        try:
            response = self.model.generate_content(
                prompt,
                generation_config=genai.types.GenerationConfig(
                    max_output_tokens=max_tokens,
                    temperature=temperature,
                )
            )
            
            return response.text
            
        except Exception as e:
            logger.error(f"Gemini API error: {e}")
            raise
            
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
        """
        Generate a chat response using Gemini API.
        
        Args:
            messages: List of chat messages [{"role": "user", "content": "..."}]
            max_tokens: Maximum tokens to generate
            temperature: Sampling temperature
            
        Returns:
            Generated response or None on failure
        """
        if not self.enabled:
            return None
        
        try:
            # Convert messages to Gemini format or just a single prompt for simplicity
            # For now, let's just use the last user message + system context if we want to stay simple
            # but better to handle the full history
            
            # Separate system prompt if present
            system_instruction = ""
            chat_history = []
            
            for msg in messages:
                if msg["role"] == "system":
                    system_instruction = msg["content"]
                elif msg["role"] == "user":
                    chat_history.append({"role": "user", "parts": [msg["content"]]})
                elif msg["role"] == "assistant":
                    chat_history.append({"role": "model", "parts": [msg["content"]]})
            
            # Re-initialize with system instruction if we can
            if system_instruction:
                model = genai.GenerativeModel('gemini-1.5-flash-latest', system_instruction=system_instruction)
            else:
                model = self.model
                
            # Use the last message as the prompt, others as history
            if not chat_history:
                return None
                
            last_msg = chat_history.pop()
            chat = model.start_chat(history=chat_history)
            
            response = chat.send_message(
                last_msg["parts"][0],
                generation_config=genai.types.GenerationConfig(
                    max_output_tokens=max_tokens,
                    temperature=temperature,
                )
            )
            
            return response.text
            
        except Exception as e:
            logger.error(f"Gemini Chat API error: {e}")
            raise    
    async def generate_summary(
        self,
        dataset_name: str,
        description: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """
        Generate dataset summary (150-250 words).
        
        Args:
            dataset_name: Dataset name
            description: Dataset description
            metadata: Optional metadata
            
        Returns:
            Generated summary or None
        """
        # Check cache first
        cache_key = f"llm:summary:{dataset_name}"
        cached = await redis_client.get(cache_key)
        if cached:
            logger.debug(f"Using cached summary for {dataset_name}")
            return cached
        
        # Build prompt
        prompt = self._build_summary_prompt(dataset_name, description, metadata)
        
        try:
            summary = await self.generate_text(prompt, max_tokens=300, temperature=0.7)
            
            if summary:
                # Cache for 7 days
                await redis_client.set(cache_key, summary, ttl=7 * 24 * 60 * 60)
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to generate summary for {dataset_name}: {e}")
            return self._fallback_summary(dataset_name, description)
    
    async def extract_insights(
        self,
        dataset_name: str,
        description: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> List[str]:
        """
        Extract 3-5 key insights about the dataset.
        
        Args:
            dataset_name: Dataset name
            description: Dataset description
            metadata: Optional metadata
            
        Returns:
            List of insights
        """
        # Check cache first
        cache_key = f"llm:insights:{dataset_name}"
        cached = await redis_client.get(cache_key)
        if cached:
            logger.debug(f"Using cached insights for {dataset_name}")
            return cached
        
        # Build prompt
        prompt = self._build_insights_prompt(dataset_name, description, metadata)
        
        try:
            response = await self.generate_text(prompt, max_tokens=200, temperature=0.8)
            
            if response:
                # Parse insights (assume bullet points or numbered list)
                insights = self._parse_insights(response)
                
                # Cache for 7 days
                await redis_client.set(cache_key, insights, ttl=7 * 24 * 60 * 60)
                
                return insights
            
            return []
            
        except Exception as e:
            logger.error(f"Failed to extract insights for {dataset_name}: {e}")
            return []
    
    async def enhance_search_query(self, query: str) -> List[str]:
        """
        Enhance search query with semantic expansions.
        
        Args:
            query: Original search query
            
        Returns:
            List of expanded queries
        """
        if not self.enabled:
            return [query]
        
        prompt = f"""Given the search query: "{query}"
        
Generate 3 semantically similar queries that would help find related datasets.
Return only the queries, one per line, without numbering or bullets."""
        
        try:
            response = await self.generate_text(prompt, max_tokens=100, temperature=0.9)
            
            if response:
                queries = [q.strip() for q in response.split('\n') if q.strip()]
                return [query] + queries[:3]
            
            return [query]
            
        except Exception as e:
            logger.error(f"Failed to enhance query: {e}")
            return [query]
    
    def _build_summary_prompt(
        self,
        name: str,
        description: str,
        metadata: Optional[Dict[str, Any]]
    ) -> str:
        """Build prompt for summary generation."""
        prompt = f"""Summarize the following dataset in 150-250 words. Focus on:
- What the dataset contains
- Primary use cases and applications
- Key characteristics and scale
- Why it's valuable for ML/AI research

Dataset Name: {name}
Description: {description}
"""
        
        if metadata:
            prompt += f"\nAdditional Info: {metadata}\n"
        
        prompt += "\nProvide a comprehensive, informative summary:"
        
        return prompt
    
    def _build_insights_prompt(
        self,
        name: str,
        description: str,
        metadata: Optional[Dict[str, Any]]
    ) -> str:
        """Build prompt for insights extraction."""
        prompt = f"""Analyze the following dataset and extract 3-5 key insights.
Focus on unique characteristics, strengths, potential applications, and notable features.

Dataset Name: {name}
Description: {description}
"""
        
        if metadata:
            prompt += f"\nAdditional Info: {metadata}\n"
        
        prompt += "\nProvide 3-5 concise insights (one per line, start each with a dash):"
        
        return prompt
    
    def _parse_insights(self, response: str) -> List[str]:
        """Parse insights from LLM response."""
        insights = []
        
        for line in response.split('\n'):
            line = line.strip()
            # Remove common prefixes
            for prefix in ['-', '•', '*', '1.', '2.', '3.', '4.', '5.']:
                if line.startswith(prefix):
                    line = line[len(prefix):].strip()
                    break
            
            if line and len(line) > 10:  # Filter out very short lines
                insights.append(line)
        
        return insights[:5]  # Limit to 5 insights
    
    def _fallback_summary(self, name: str, description: str) -> str:
        """Generate fallback summary when LLM fails."""
        # Simple template-based summary
        summary = f"{name} is a dataset "
        
        if description:
            # Take first 200 characters of description
            summary += description[:200]
            if len(description) > 200:
                summary += "..."
        else:
            summary += "for machine learning and AI research."
        
        return summary


# Global Gemini client instance
gemini_client = GeminiClient()
