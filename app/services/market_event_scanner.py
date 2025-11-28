"""
AI Market Event Scanner

Scans for macro and micro market events that could impact stock index prices.
Uses AI to analyze news and economic data for prediction enhancement.

Event categories:
- Fed announcements (FOMC, rate decisions, speeches)
- Economic data releases (CPI, jobs, GDP, PMI)
- Earnings reports (major companies)
- Geopolitical events
- Market structure events (options expiration, etc.)
"""

import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
import json
import os

log = logging.getLogger("market_event_scanner")

# Key economic events that typically move markets
MAJOR_EVENT_KEYWORDS = {
    "fed": ["fomc", "fed", "federal reserve", "rate decision", "powell", "interest rate", "quantitative"],
    "economic": ["cpi", "inflation", "jobs report", "nfp", "payroll", "gdp", "pmi", "retail sales", "unemployment"],
    "earnings": ["earnings", "quarterly report", "revenue", "eps", "guidance"],
    "geopolitical": ["tariff", "trade war", "sanction", "conflict", "election"],
    "market_structure": ["opex", "options expiration", "triple witching", "quad witching", "rebalance"]
}

# Major companies whose earnings move indices
INDEX_MOVING_COMPANIES = [
    "Apple", "Microsoft", "Amazon", "Google", "Meta", "Nvidia", "Tesla",
    "JPMorgan", "Bank of America", "Berkshire", "Johnson & Johnson", "UnitedHealth",
    "Visa", "Mastercard", "Walmart", "Exxon", "Chevron"
]


@dataclass
class MarketEvent:
    """A market-moving event"""
    event_type: str  # fed, economic, earnings, geopolitical, market_structure
    title: str
    description: str
    impact_level: str  # high, medium, low
    expected_direction: Optional[str] = None  # bullish, bearish, neutral
    event_date: Optional[date] = None
    source: Optional[str] = None
    
    def to_dict(self) -> Dict:
        return {
            "event_type": self.event_type,
            "title": self.title,
            "description": self.description,
            "impact_level": self.impact_level,
            "expected_direction": self.expected_direction,
            "event_date": self.event_date.isoformat() if self.event_date else None,
            "source": self.source
        }


@dataclass
class MarketEventScan:
    """Results of a market event scan"""
    scan_time: datetime
    events: List[MarketEvent] = field(default_factory=list)
    summary: str = ""
    overall_sentiment: str = "neutral"  # bullish, bearish, neutral
    risk_level: str = "normal"  # low, normal, elevated, high
    
    def to_dict(self) -> Dict:
        return {
            "scan_time": self.scan_time.isoformat(),
            "events": [e.to_dict() for e in self.events],
            "summary": self.summary,
            "overall_sentiment": self.overall_sentiment,
            "risk_level": self.risk_level,
            "event_count": len(self.events)
        }


class MarketEventScanner:
    """
    Scans for market-moving events using AI.
    Caches results to avoid excessive API calls.
    """
    
    def __init__(self):
        self._cache: Optional[MarketEventScan] = None
        self._cache_expiry: Optional[datetime] = None
        self._cache_duration_minutes = 15  # Refresh every 15 minutes
    
    def _get_openai_client(self):
        """Get OpenAI client for AI analysis"""
        try:
            from openai import OpenAI
            
            # Use Replit AI Integration if available
            base_url = os.environ.get("AI_INTEGRATIONS_OPENAI_BASE_URL")
            api_key = os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY") or os.environ.get("OPENAI_API_KEY")
            
            if api_key:
                if base_url:
                    return OpenAI(api_key=api_key, base_url=base_url)
                else:
                    return OpenAI(api_key=api_key)
        except Exception as e:
            log.warning(f"OpenAI client not available: {e}")
        return None
    
    def _generate_event_query(self) -> str:
        """Generate a search query for today's market events"""
        today = datetime.now().strftime("%B %d, %Y")
        return f"stock market news events today {today} Fed FOMC economic data earnings"
    
    def _analyze_with_ai(self, news_context: str) -> MarketEventScan:
        """Use AI to analyze news and extract market events"""
        client = self._get_openai_client()
        
        if not client:
            log.warning("OpenAI client not available, returning minimal scan")
            return MarketEventScan(
                scan_time=datetime.now(),
                events=[],
                summary="AI analysis unavailable - please check API key configuration",
                overall_sentiment="neutral",
                risk_level="normal"
            )
        
        today = datetime.now().strftime("%B %d, %Y")
        
        prompt = f"""Analyze the following market news/context and identify significant events that could impact US stock indices (S&P 500, NASDAQ, Dow Jones).

TODAY'S DATE: {today}

NEWS/CONTEXT:
{news_context}

Identify and categorize market-moving events. For each event, assess:
1. Event type: fed (Fed/FOMC related), economic (economic data), earnings (company earnings), geopolitical, market_structure (options expiration, etc.)
2. Impact level: high, medium, or low
3. Expected market direction: bullish, bearish, or neutral

Return your analysis in JSON format:
{{
    "events": [
        {{
            "event_type": "fed|economic|earnings|geopolitical|market_structure",
            "title": "Brief event title",
            "description": "1-2 sentence description of the event and its market implications",
            "impact_level": "high|medium|low",
            "expected_direction": "bullish|bearish|neutral"
        }}
    ],
    "summary": "2-3 sentence overall market summary for today",
    "overall_sentiment": "bullish|bearish|neutral",
    "risk_level": "low|normal|elevated|high"
}}

Focus on events happening TODAY or in the next few days that active traders should know about.
If no significant events are happening, return an empty events list with appropriate summary."""

        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a professional market analyst providing concise, actionable market intelligence. Focus on events that move indices."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                max_tokens=1500,
                temperature=0.3
            )
            
            result = json.loads(response.choices[0].message.content)
            
            events = []
            for e in result.get("events", []):
                events.append(MarketEvent(
                    event_type=e.get("event_type", "unknown"),
                    title=e.get("title", "Unknown Event"),
                    description=e.get("description", ""),
                    impact_level=e.get("impact_level", "medium"),
                    expected_direction=e.get("expected_direction", "neutral"),
                    event_date=date.today(),
                    source="AI Analysis"
                ))
            
            return MarketEventScan(
                scan_time=datetime.now(),
                events=events,
                summary=result.get("summary", "No significant market events identified."),
                overall_sentiment=result.get("overall_sentiment", "neutral"),
                risk_level=result.get("risk_level", "normal")
            )
            
        except Exception as e:
            log.error(f"AI analysis failed: {e}")
            return MarketEventScan(
                scan_time=datetime.now(),
                events=[],
                summary=f"AI analysis error: {str(e)}",
                overall_sentiment="neutral",
                risk_level="normal"
            )
    
    def _get_static_calendar_events(self) -> List[MarketEvent]:
        """Get known scheduled events from a static calendar"""
        today = date.today()
        events = []
        
        # Check for known recurring events
        weekday = today.weekday()
        day_of_month = today.day
        
        # Options expiration (third Friday of month)
        if weekday == 4:  # Friday
            # Check if it's the third Friday (days 15-21)
            if 15 <= day_of_month <= 21:
                events.append(MarketEvent(
                    event_type="market_structure",
                    title="Monthly Options Expiration (OPEX)",
                    description="Monthly options expiration day - expect increased volatility and potential gamma-driven price moves near key strikes.",
                    impact_level="high",
                    expected_direction="neutral",
                    event_date=today,
                    source="Calendar"
                ))
            # Weekly options expire every Friday
            events.append(MarketEvent(
                event_type="market_structure",
                title="Weekly Options Expiration",
                description="Weekly options expiring - watch for pin risk at heavily populated strikes.",
                impact_level="medium",
                expected_direction="neutral",
                event_date=today,
                source="Calendar"
            ))
        
        # First Friday of month = Jobs Report
        if weekday == 4 and day_of_month <= 7:
            events.append(MarketEvent(
                event_type="economic",
                title="Monthly Jobs Report (NFP)",
                description="Non-Farm Payrolls release - major market mover, expect volatility around 8:30 AM ET.",
                impact_level="high",
                expected_direction="neutral",
                event_date=today,
                source="Calendar"
            ))
        
        return events
    
    def scan_market_events(self, news_context: Optional[str] = None) -> MarketEventScan:
        """
        Scan for current market events.
        Uses caching to avoid excessive API calls.
        
        Args:
            news_context: Optional pre-fetched news context to analyze.
                         If not provided, uses static calendar + default context.
        
        Returns:
            MarketEventScan with identified events
        """
        now = datetime.now()
        
        # Check cache
        if self._cache and self._cache_expiry and now < self._cache_expiry:
            log.debug("Returning cached market event scan")
            return self._cache
        
        # Get static calendar events first
        calendar_events = self._get_static_calendar_events()
        
        # Build context for AI analysis
        if not news_context:
            # Create a minimal context for AI to work with
            today = datetime.now().strftime("%A, %B %d, %Y")
            news_context = f"""
Today is {today}.

Please analyze the current market environment and identify any significant events that traders should be aware of today.

Consider:
- Is there a Fed meeting or Fed speaker today?
- Are there any major economic data releases (CPI, jobs, GDP)?
- Are there major earnings announcements from index-moving companies?
- Is it an options expiration day?
- Are there any geopolitical events affecting markets?

Provide your analysis of what events are likely impacting markets today.
"""
        
        # Run AI analysis
        scan = self._analyze_with_ai(news_context)
        
        # Merge calendar events (add if not duplicate)
        calendar_titles = {e.title for e in calendar_events}
        ai_titles = {e.title for e in scan.events}
        
        for event in calendar_events:
            if event.title not in ai_titles:
                scan.events.insert(0, event)  # Calendar events first
        
        # Update risk level if we have high-impact events
        high_impact_count = sum(1 for e in scan.events if e.impact_level == "high")
        if high_impact_count >= 2:
            scan.risk_level = "high"
        elif high_impact_count >= 1:
            scan.risk_level = "elevated"
        
        # Cache result
        self._cache = scan
        self._cache_expiry = now + timedelta(minutes=self._cache_duration_minutes)
        
        log.info(f"Market event scan complete: {len(scan.events)} events, sentiment={scan.overall_sentiment}, risk={scan.risk_level}")
        
        return scan
    
    def get_events_summary_for_ai(self) -> str:
        """
        Get a formatted summary of market events for inclusion in AI prediction prompts.
        This is the key integration point for enhancing predictions.
        """
        scan = self.scan_market_events()
        
        if not scan.events:
            return "No significant market events identified for today."
        
        lines = [f"TODAY'S MARKET EVENTS (Risk Level: {scan.risk_level.upper()}):"]
        
        for event in scan.events[:5]:  # Limit to top 5 events
            direction_indicator = ""
            if event.expected_direction == "bullish":
                direction_indicator = "↑"
            elif event.expected_direction == "bearish":
                direction_indicator = "↓"
            else:
                direction_indicator = "↔"
            
            lines.append(f"- [{event.impact_level.upper()}] {direction_indicator} {event.title}")
            lines.append(f"  {event.description}")
        
        lines.append(f"\nOVERALL: {scan.summary}")
        lines.append(f"Sentiment: {scan.overall_sentiment.capitalize()}")
        
        return "\n".join(lines)
    
    def clear_cache(self):
        """Clear the event cache to force a fresh scan"""
        self._cache = None
        self._cache_expiry = None


# Global singleton instance
_scanner: Optional[MarketEventScanner] = None


def get_market_event_scanner() -> MarketEventScanner:
    """Get or create the global market event scanner"""
    global _scanner
    if _scanner is None:
        _scanner = MarketEventScanner()
    return _scanner


def scan_market_events(news_context: Optional[str] = None) -> MarketEventScan:
    """Convenience function to scan for market events"""
    return get_market_event_scanner().scan_market_events(news_context)


def get_events_for_ai_prompt() -> str:
    """Convenience function to get event summary for AI prediction prompts"""
    return get_market_event_scanner().get_events_summary_for_ai()
