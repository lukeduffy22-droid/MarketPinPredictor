"""
OpenAI AI Provider implementation.
Uses Replit's AI Integrations for OpenAI access.
"""

import os
import json
import logging
from typing import Dict, Any, Optional

from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

from .base_provider import BaseAIProvider, PredictionCritique, MarketAnalysis

log = logging.getLogger(__name__)

AI_INTEGRATIONS_OPENAI_API_KEY = os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY")
AI_INTEGRATIONS_OPENAI_BASE_URL = os.environ.get("AI_INTEGRATIONS_OPENAI_BASE_URL")


def is_rate_limit_error(exception: BaseException) -> bool:
    """Check if the exception is a rate limit error."""
    error_msg = str(exception)
    return (
        "429" in error_msg
        or "RATELIMIT_EXCEEDED" in error_msg
        or "quota" in error_msg.lower()
        or "rate limit" in error_msg.lower()
        or (hasattr(exception, "status_code") and exception.status_code == 429)
    )


class OpenAIProvider(BaseAIProvider):
    """
    OpenAI provider using Replit AI Integrations.
    This uses Replit's managed OpenAI access - no API key needed, billed to credits.
    """
    
    def __init__(self):
        self._client = None
        if self.is_available():
            # the newest OpenAI model is "gpt-5" which was released August 7, 2025.
            # do not change this unless explicitly requested by the user
            self._client = OpenAI(
                api_key=AI_INTEGRATIONS_OPENAI_API_KEY,
                base_url=AI_INTEGRATIONS_OPENAI_BASE_URL
            )
            self._model = "gpt-4.1-mini"  # Fast model for real-time use
            self._reasoning_model = "gpt-4.1"  # Smarter model for complex analysis
    
    @property
    def name(self) -> str:
        return "openai"
    
    def is_available(self) -> bool:
        """Check if OpenAI integration is configured."""
        return bool(AI_INTEGRATIONS_OPENAI_API_KEY and AI_INTEGRATIONS_OPENAI_BASE_URL)
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception(is_rate_limit_error),
        reraise=True
    )
    def _call_openai(self, messages: list, use_reasoning: bool = False) -> str:
        """Make an OpenAI API call with retries."""
        if not self._client:
            raise RuntimeError("OpenAI client not initialized")
        
        model = self._reasoning_model if use_reasoning else self._model
        
        response = self._client.chat.completions.create(
            model=model,
            messages=messages,
            response_format={"type": "json_object"},
            max_completion_tokens=2048
        )
        
        return response.choices[0].message.content or "{}"
    
    async def analyze_prediction(
        self,
        symbol: str,
        current_price: float,
        predicted_eod: float,
        gamma_data: Dict[str, Any],
        vwap_deviation: float,
        microtrend: float,
        minutes_to_close: int,
        historical_accuracy: Optional[float] = None
    ) -> PredictionCritique:
        """Analyze and critique an EOD prediction using OpenAI."""
        
        # Build context for the AI
        gamma_summary = self._format_gamma_for_prompt(gamma_data)
        predicted_move = predicted_eod - current_price
        predicted_pct = (predicted_move / current_price) * 100
        
        prompt = f"""You are an expert quantitative analyst reviewing an end-of-day stock index prediction.

CURRENT MARKET DATA for {symbol}:
- Current Price: {current_price:.2f}
- Model's EOD Prediction: {predicted_eod:.2f} (move of {predicted_move:+.2f} pts, {predicted_pct:+.3f}%)
- Minutes to Close: {minutes_to_close}
- VWAP Deviation: {vwap_deviation:+.2f} pts
- Microtrend (5-min slope): {microtrend:+.4f}
{f'- Historical Model Accuracy: {historical_accuracy:.1f}%' if historical_accuracy else ''}

GAMMA EXPOSURE ANALYSIS:
{gamma_summary}

TASK: Analyze this prediction and provide:
1. Whether the prediction seems reasonable given the gamma positioning
2. Any adjustments you would make (if any)
3. Key risk factors
4. A confidence score (0.0 to 1.0)

Respond in JSON format:
{{
    "adjusted_prediction": <float - your adjusted EOD prediction, or same as original if no change>,
    "confidence": <float 0.0-1.0>,
    "adjustment_reason": "<brief explanation of any adjustment or 'No adjustment needed'>",
    "market_conditions": "<1-2 sentence market condition summary>",
    "risk_factors": ["<risk 1>", "<risk 2>", ...],
    "recommendation": "<actionable insight for traders>"
}}"""

        messages = [
            {"role": "system", "content": "You are a quantitative trading analyst. Provide analysis in JSON format only."},
            {"role": "user", "content": prompt}
        ]
        
        try:
            response_text = self._call_openai(messages, use_reasoning=False)
            result = json.loads(response_text)
            
            return PredictionCritique(
                original_prediction=predicted_eod,
                adjusted_prediction=float(result.get("adjusted_prediction", predicted_eod)),
                confidence=float(result.get("confidence", 0.5)),
                adjustment_reason=result.get("adjustment_reason", "Analysis unavailable"),
                market_conditions=result.get("market_conditions", ""),
                risk_factors=result.get("risk_factors", []),
                recommendation=result.get("recommendation", ""),
                provider=self.name
            )
        except Exception as e:
            log.error(f"OpenAI prediction analysis failed: {e}")
            # Return a fallback with no adjustment
            return PredictionCritique(
                original_prediction=predicted_eod,
                adjusted_prediction=predicted_eod,
                confidence=0.0,
                adjustment_reason=f"AI analysis unavailable: {str(e)}",
                market_conditions="Unable to analyze",
                risk_factors=["AI service error"],
                recommendation="Use model prediction without AI adjustment",
                provider=self.name
            )
    
    async def get_market_briefing(
        self,
        symbol: str,
        current_price: float,
        gamma_data: Dict[str, Any],
        eod_prediction: float,
        multi_expiry_data: Optional[Dict[str, Any]] = None
    ) -> MarketAnalysis:
        """Generate a comprehensive market briefing."""
        
        gamma_summary = self._format_gamma_for_prompt(gamma_data)
        multi_expiry_summary = self._format_multi_expiry_for_prompt(multi_expiry_data) if multi_expiry_data else ""
        
        predicted_move = eod_prediction - current_price
        
        prompt = f"""Generate a concise market briefing for {symbol}.

CURRENT DATA:
- Current Price: {current_price:.2f}
- EOD Prediction: {eod_prediction:.2f} ({predicted_move:+.2f} pts expected move)

GAMMA POSITIONING:
{gamma_summary}

{f'MULTI-EXPIRATION GAMMA (0-7 DTE):{chr(10)}{multi_expiry_summary}' if multi_expiry_summary else ''}

Provide a trading briefing in JSON format:
{{
    "summary": "<2-3 sentence executive summary of market conditions and expectations>",
    "gamma_interpretation": "<what the gamma positioning means for price action>",
    "trend_assessment": "<bullish/bearish/neutral with reasoning>",
    "key_levels": {{
        "resistance": <nearest resistance level>,
        "support": <nearest support level>,
        "magnet": <gamma pin/magnet level>
    }},
    "sentiment": "<bullish|bearish|neutral>",
    "confidence": <0.0-1.0>
}}"""

        messages = [
            {"role": "system", "content": "You are a market analyst providing trading briefings. Respond only in JSON format."},
            {"role": "user", "content": prompt}
        ]
        
        try:
            response_text = self._call_openai(messages, use_reasoning=False)
            result = json.loads(response_text)
            
            return MarketAnalysis(
                summary=result.get("summary", "Analysis unavailable"),
                gamma_interpretation=result.get("gamma_interpretation", ""),
                trend_assessment=result.get("trend_assessment", ""),
                key_levels=result.get("key_levels", {}),
                sentiment=result.get("sentiment", "neutral"),
                confidence=float(result.get("confidence", 0.5)),
                provider=self.name
            )
        except Exception as e:
            log.error(f"OpenAI market briefing failed: {e}")
            return MarketAnalysis(
                summary=f"AI briefing unavailable: {str(e)}",
                gamma_interpretation="",
                trend_assessment="",
                key_levels={},
                sentiment="neutral",
                confidence=0.0,
                provider=self.name
            )
    
    def _format_gamma_for_prompt(self, gamma_data: Dict[str, Any]) -> str:
        """Format gamma data for AI prompt."""
        if not gamma_data:
            return "No gamma data available"
        
        lines = []
        
        if "pin_strike" in gamma_data:
            lines.append(f"- Pin Strike: {gamma_data['pin_strike']:.2f}")
        if "total_gex" in gamma_data:
            gex_billions = gamma_data['total_gex'] / 1e9
            lines.append(f"- Total GEX: ${gex_billions:.2f}B")
        if "net_gex" in gamma_data:
            net_gex_billions = gamma_data['net_gex'] / 1e9
            lines.append(f"- Net GEX: ${net_gex_billions:+.2f}B")
        if "direction" in gamma_data:
            lines.append(f"- Price vs Pin: {gamma_data['direction']}")
        if "pull_strength" in gamma_data:
            lines.append(f"- Pull Strength: {gamma_data['pull_strength']:.2f}%")
        if "zero_gamma" in gamma_data:
            lines.append(f"- Zero Gamma Level: {gamma_data['zero_gamma']:.2f}")
        
        return "\n".join(lines) if lines else "Gamma data format not recognized"
    
    def _format_multi_expiry_for_prompt(self, data: Dict[str, Any]) -> str:
        """Format multi-expiry gamma data for AI prompt."""
        if not data:
            return ""
        
        lines = []
        
        if "aggregate_pin" in data:
            lines.append(f"- Aggregate Pin (weighted): {data['aggregate_pin']:.2f}")
        
        if "gamma_by_expiry" in data:
            for dte, expiry_data in data["gamma_by_expiry"].items():
                weight = expiry_data.get("weight", 0)
                pin = expiry_data.get("pin_strike", 0)
                gex = expiry_data.get("total_gex", 0) / 1e9
                lines.append(f"- {dte} DTE: Pin={pin:.0f}, GEX=${gex:.2f}B, Weight={weight:.0%}")
        
        if "unified_walls" in data and data["unified_walls"]:
            top_wall = data["unified_walls"][0] if isinstance(data["unified_walls"], list) else None
            if top_wall:
                lines.append(f"- Strongest Unified Wall: {top_wall.get('strike', 'N/A')}")
        
        return "\n".join(lines)
