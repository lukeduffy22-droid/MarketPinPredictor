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
        self._model = "gpt-4.1-mini"
        self._reasoning_model = "gpt-4.1"
    
    def _get_client(self) -> Optional[OpenAI]:
        """Lazily initialize OpenAI client, checking env vars at call time."""
        if self._client is not None:
            return self._client
        
        api_key = os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY")
        base_url = os.environ.get("AI_INTEGRATIONS_OPENAI_BASE_URL")
        
        if api_key and base_url:
            self._client = OpenAI(api_key=api_key, base_url=base_url)
            return self._client
        return None
    
    @property
    def name(self) -> str:
        return "openai"
    
    def is_available(self) -> bool:
        """Check if OpenAI integration is configured (lazy check at call time)."""
        api_key = os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY")
        base_url = os.environ.get("AI_INTEGRATIONS_OPENAI_BASE_URL")
        return bool(api_key and base_url)
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception(is_rate_limit_error),
        reraise=True
    )
    def _call_openai(self, messages: list, use_reasoning: bool = False) -> str:
        """Make an OpenAI API call with retries."""
        client = self._get_client()
        if not client:
            raise RuntimeError("OpenAI client not initialized - env vars may not be set")
        
        model = self._reasoning_model if use_reasoning else self._model
        
        response = client.chat.completions.create(
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
        historical_accuracy: Optional[float] = None,
        historical_accuracy_data: Optional[Dict[str, Any]] = None,
        orb_data: Optional[Dict[str, Any]] = None,
        market_events: Optional[str] = None
    ) -> PredictionCritique:
        """Analyze and critique an EOD prediction using OpenAI.
        
        Now includes ORB (Opening Range Breakout) data and market events context.
        """
        
        # Build context for the AI
        gamma_summary = self._format_gamma_for_prompt(gamma_data)
        predicted_move = predicted_eod - current_price
        predicted_pct = (predicted_move / current_price) * 100
        
        # Build historical accuracy context
        accuracy_context = ""
        if historical_accuracy_data and historical_accuracy_data.get('sample_size', 0) > 0:
            avg_acc = historical_accuracy_data.get('avg_accuracy')
            avg_err = historical_accuracy_data.get('avg_error_pct', 0)
            bias = historical_accuracy_data.get('bias', 'unknown')
            consistency = historical_accuracy_data.get('consistency', 'unknown')
            
            accuracy_context = f"""
HISTORICAL MODEL PERFORMANCE ({historical_accuracy_data['sample_size']} past predictions):
- Average Accuracy: {avg_acc:.1f}% (if available)
- Prediction Bias: {bias.replace('_', ' ').title()}
- Consistency: {consistency.replace('_', ' ').title()}
- Average Error: {avg_err:+.2f}%
""" if avg_acc else f"""
HISTORICAL MODEL PERFORMANCE ({historical_accuracy_data['sample_size']} past predictions):
- Prediction Bias: {bias.replace('_', ' ').title()}
- Consistency: {consistency.replace('_', ' ').title()}
- Average Error: {avg_err:+.2f}%
"""
            recent_preds = historical_accuracy_data.get('recent_predictions', [])
            if recent_preds:
                valid_recent = [p for p in recent_preds if p.get('accuracy') is not None]
                if valid_recent:
                    accuracy_context += f"\nLast {len(valid_recent)} Prediction{'s' if len(valid_recent) > 1 else ''}:\n"
                    for p in valid_recent[:3]:
                        accuracy_context += f"  - Predicted: {p['predicted']:.2f}, Actual: {p['actual']:.2f}, Accuracy: {p['accuracy']:.1f}%\n"
        elif historical_accuracy is not None:
            accuracy_context = f"\n- Historical Model Accuracy: {historical_accuracy:.1f}%\n"
        
        # Build ORB context (new)
        orb_context = ""
        if orb_data:
            orb_complete = orb_data.get('orb_complete', False)
            if orb_complete:
                orb_high = orb_data.get('orb_high', 0)
                orb_low = orb_data.get('orb_low', 0)
                range_width_pct = orb_data.get('range_width_pct', 0)
                breakout = orb_data.get('breakout_direction', 'unknown')
                position = orb_data.get('position_in_range', 0.5)
                
                orb_context = f"""
1-HOUR OPENING RANGE BREAKOUT (ORB) ANALYSIS:
- ORB High: {orb_high:.2f}
- ORB Low: {orb_low:.2f}
- Range Width: {range_width_pct:.2f}% of opening price
- Current Breakout Status: {breakout.upper()}
- Position in Range: {position:.2f} (0=at low, 0.5=midpoint, 1=at high, >1=above high, <0=below low)
Note: ORB theory suggests breakouts tend to continue toward EOD. A bullish breakout favors higher closes, bearish breakout favors lower closes.
"""
            else:
                orb_context = "\n1-HOUR ORB: Still forming (before 10:30 AM ET)\n"
        
        # Build market events context (new)
        events_context = ""
        if market_events:
            events_context = f"\n{market_events}\n"
        
        prompt = f"""You are an expert quantitative analyst reviewing an end-of-day stock index prediction.

CURRENT MARKET DATA for {symbol}:
- Current Price: {current_price:.2f}
- Model's EOD Prediction: {predicted_eod:.2f} (move of {predicted_move:+.2f} pts, {predicted_pct:+.3f}%)
- Minutes to Close: {minutes_to_close}
- VWAP Deviation: {vwap_deviation:+.2f} pts
- Microtrend (5-min slope): {microtrend:+.4f}
{accuracy_context}
GAMMA EXPOSURE ANALYSIS:
{gamma_summary}
{orb_context}{events_context}
TASK: Analyze this prediction and provide:
1. Whether the prediction seems reasonable given gamma positioning AND ORB breakout status
2. Any adjustments you would make - consider historical bias, ORB breakout direction, and market events
3. Key risk factors including any significant macro events
4. A confidence score (0.0 to 1.0) - adjust based on historical accuracy and event risk

Respond in JSON format:
{{
    "adjusted_prediction": <float - your adjusted EOD prediction, or same as original if no change>,
    "confidence": <float 0.0-1.0>,
    "adjustment_reason": "<brief explanation of any adjustment or 'No adjustment needed'>",
    "market_conditions": "<1-2 sentence market condition summary including ORB status and key events>",
    "risk_factors": ["<risk 1>", "<risk 2>", ...],
    "recommendation": "<actionable insight for traders considering ORB and events>"
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
