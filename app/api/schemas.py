"""Pydantic response schemas for API route modules."""

from typing import Dict, List, Optional

from pydantic import BaseModel

class PredictionResponse(BaseModel):
    """0-day close prediction response"""
    symbol: str
    current_price: float
    predicted_close: float
    confidence_level: str  # high, medium, low
    tau_minutes: int
    rmse: Optional[float]
    mae: Optional[float]
    features: dict
    model_metadata: dict
    runtime_metadata: dict
    timestamp: str

class LevelsResponse(BaseModel):
    """Gamma levels response"""
    symbol: str
    current_price: float
    levels: list
    strongest_pin: Optional[float]
    timestamp: str

class EODPredictionResponse(BaseModel):
    """Advanced gamma-based EOD prediction response"""
    symbol: str
    current_price: float
    eod_estimate: float
    wwm: float
    pin_stability_index: float
    zero_gamma: float
    vacp: float
    hv10_points: Optional[float]
    num_walls: int
    num_pins: int
    timestamp: str

class CloseSignalResponse(BaseModel):
    """Individual close predictor signal"""
    emoji: str
    category: str
    message: str
    bias: str
    strength: float

class ClosePredictorResponse(BaseModel):
    """Close predictor overlay response with checklist signals"""
    symbol: str
    pin_strike: float
    spot_price: float
    expected_close: float
    close_range_low: float
    close_range_high: float
    signals: List[CloseSignalResponse]
    net_bias: str
    confidence: float
    drift_adjustment: float
    summary: str
    call_gex: Optional[float]
    put_gex: Optional[float]
    gex_ratio: Optional[float]
    minutes_to_close: int
    timestamp: str


class AISymbolPrediction(BaseModel):
    """Prediction data for a single symbol"""
    symbol: str
    current_price: Optional[float]
    predicted_close: Optional[float]
    gamma_pin: Optional[float]
    max_pain: Optional[float]
    confidence: Optional[str]
    minutes_to_close: Optional[int]
    error: Optional[str] = None

class AIPredictionsResponse(BaseModel):
    """Consolidated predictions for all indices - designed for AI consumption"""
    market_status: str
    timestamp: str
    predictions: List[AISymbolPrediction]
    summary: str
