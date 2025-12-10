
"""
AI-Powered Market Analysis using OpenAI
Provides real-time insights on predictions and streaming data
"""

import os
from openai import OpenAI
import streamlit as st
from datetime import datetime

def get_openai_client():
    """Initialize OpenAI client with API key from environment"""
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        return None
    return OpenAI(api_key=api_key)

def analyze_prediction(ticker_name, current_price, predicted_price, confidence, technical_indicators, gex_data=None, vix_value=None):
    """
    Get AI analysis of a prediction using GPT-4
    
    Args:
        ticker_name: Name of the index (e.g., "S&P 500 (SPX)")
        current_price: Current price
        predicted_price: Predicted EOD price
        confidence: Prediction confidence percentage
        technical_indicators: Dict with RSI, MACD, etc.
        gex_data: Gamma exposure data (optional)
        vix_value: VIX volatility value (optional)
    
    Returns:
        AI-generated analysis text
    """
    client = get_openai_client()
    if not client:
        return "OpenAI API key not configured. Add OPENAI_API_KEY to Secrets."
    
    # Build context for AI
    change_pct = ((predicted_price - current_price) / current_price) * 100
    
    context = f"""Analyze this stock index prediction:

Index: {ticker_name}
Current Price: ${current_price:.2f}
Predicted EOD Price: ${predicted_price:.2f}
Expected Change: {change_pct:+.2f}%
Confidence: {confidence:.1f}%

Technical Indicators:
- RSI: {technical_indicators.get('RSI', 'N/A')}
- MACD: {technical_indicators.get('MACD', 'N/A')}
- Signal Line: {technical_indicators.get('Signal_Line', 'N/A')}
- SMA 20: ${technical_indicators.get('SMA_20', 'N/A')}
- Momentum: {technical_indicators.get('Momentum', 'N/A')}
- VWAP: ${technical_indicators.get('VWAP', 'N/A')}
- AMA: ${technical_indicators.get('AMA', 'N/A')}
"""
    
    if gex_data:
        context += f"""
Gamma Exposure:
- Pin Strike: ${gex_data.get('pin_strike', 'N/A')}
- Direction: {gex_data.get('direction', 'N/A')}
- Pull Strength: {gex_data.get('pull_strength', 'N/A'):.1f}%
- Zero Gamma Level: ${gex_data.get('zero_gamma', 'N/A')}
"""
    
    if vix_value:
        context += f"\nVIX (Volatility): {vix_value:.2f}"
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are an expert financial analyst specializing in technical analysis and options markets. Provide concise, actionable insights based on the data provided. Focus on risk assessment, key support/resistance levels, and trading implications. Keep your response under 150 words."},
                {"role": "user", "content": context}
            ],
            temperature=0.7,
            max_tokens=250
        )
        
        return response.choices[0].message.content
    except Exception as e:
        return f"Error getting AI analysis: {str(e)}"

def analyze_streaming_data(ticker_name, latest_price, price_change_pct, volume_ratio, streaming_stats):
    """
    Analyze real-time streaming data for anomalies and insights
    
    Args:
        ticker_name: Index name
        latest_price: Most recent price from stream
        price_change_pct: Price change percentage
        volume_ratio: Current volume vs average
        streaming_stats: Dict with update counts
    
    Returns:
        AI-generated streaming analysis
    """
    client = get_openai_client()
    if not client:
        return None
    
    context = f"""Real-time market data update:

Index: {ticker_name}
Latest Price: ${latest_price:.2f}
Price Change: {price_change_pct:+.2f}%
Volume Ratio: {volume_ratio:.2f}x average
Data Points: {streaming_stats.get('trade_count', 0)} trades, {streaming_stats.get('quote_count', 0)} quotes
"""
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a real-time market analyst. Identify unusual patterns, volume spikes, or price movements. Be brief and actionable. Maximum 100 words."},
                {"role": "user", "content": context}
            ],
            temperature=0.7,
            max_tokens=150
        )
        
        return response.choices[0].message.content
    except Exception as e:
        return None

def get_risk_assessment(predictions_dict):
    """
    Get overall portfolio risk assessment across multiple predictions
    
    Args:
        predictions_dict: Dictionary of all predictions
    
    Returns:
        AI risk assessment
    """
    client = get_openai_client()
    if not client:
        return None
    
    # Summarize all predictions
    summary = "Portfolio Summary:\n"
    for idx_name, pred in predictions_dict.items():
        summary += f"\n{idx_name}: {pred['change_pct']:+.2f}% (confidence: {pred['confidence']:.0f}%)"
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a risk management analyst. Assess overall market risk based on these index predictions. Consider correlation, volatility, and confidence levels. Maximum 120 words."},
                {"role": "user", "content": summary}
            ],
            temperature=0.7,
            max_tokens=180
        )
        
        return response.choices[0].message.content
    except Exception as e:
        return None

def explain_gamma_exposure(gex_data):
    """
    Explain gamma exposure in simple terms
    
    Args:
        gex_data: Gamma exposure analysis data
    
    Returns:
        Plain English explanation
    """
    client = get_openai_client()
    if not client:
        return None
    
    context = f"""Explain this gamma exposure situation in simple terms:

Pin Strike: ${gex_data.get('pin_strike', 'N/A')}
Current Position: {gex_data.get('direction', 'N/A')} the pin
Pull Strength: {gex_data.get('pull_strength', 0):.1f}%
Zero Gamma Level: ${gex_data.get('zero_gamma', 'N/A')}
Total GEX: ${gex_data.get('total_gex', 0):.1f}B
"""
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a teacher explaining options gamma to retail traders. Use simple language and explain what this means for price movement. Maximum 100 words."},
                {"role": "user", "content": context}
            ],
            temperature=0.7,
            max_tokens=150
        )
        
        return response.choices[0].message.content
    except Exception as e:
        return None
