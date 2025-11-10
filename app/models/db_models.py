"""
Database models for calibration coefficients and RMSE tracking.
Per-symbol and per-time-bucket storage for adaptive predictions.
"""
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from typing import Optional
import os

Base = declarative_base()

class CalibrationCoeff(Base):
    """
    Per-symbol regression coefficients for prediction model.
    Loaded at app startup - missing coefficients cause fail-fast.
    """
    __tablename__ = "calibration_coeffs"
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String(10), nullable=False, unique=True)
    
    # Ridge regression coefficients
    beta_vwap = Column(Float, nullable=False, default=1.0)
    beta_gamma = Column(Float, nullable=False, default=0.0)
    beta_flow = Column(Float, nullable=False, default=0.0)
    beta_microtrend = Column(Float, nullable=False, default=0.0)
    intercept = Column(Float, nullable=False, default=0.0)
    
    # Last calibration
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    sample_size = Column(Integer, nullable=False, default=0)

class RMSEBucket(Base):
    """
    RMSE by time-to-close bucket for confidence intervals.
    Used to provide realistic error bounds in predictions.
    """
    __tablename__ = "rmse_buckets"
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String(10), nullable=False)
    
    # Time bucket (minutes to close)
    tau_min = Column(Integer, nullable=False)  # e.g., 0 for 0-15 min
    tau_max = Column(Integer, nullable=False)  # e.g., 15 for 0-15 min
    
    # Metrics
    rmse = Column(Float, nullable=False)
    mae = Column(Float, nullable=False)
    direction_accuracy = Column(Float, nullable=False)  # % correct direction
    
    # Sample info
    n_samples = Column(Integer, nullable=False, default=0)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    __table_args__ = (
        # Unique constraint on (symbol, tau_min, tau_max)
    )

class PredictionLog(Base):
    """
    Historical predictions for backtesting and accuracy tracking.
    Stores prediction → actual outcome for continuous calibration.
    """
    __tablename__ = "prediction_logs"
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String(10), nullable=False)
    
    # Prediction metadata
    prediction_ts = Column(DateTime, nullable=False)
    prediction_date = Column(Date, nullable=False)
    tau_minutes = Column(Integer, nullable=False)  # Minutes to close
    
    # Prediction values
    predicted_close = Column(Float, nullable=False)
    current_price = Column(Float, nullable=False)
    vwap = Column(Float, nullable=False)
    
    # Feature values (for debugging)
    gamma_pin = Column(Float)
    flow_urgency = Column(Float)
    microtrend = Column(Float)
    
    # Actual outcome (filled after close)
    actual_close = Column(Float)
    error_abs = Column(Float)
    error_pct = Column(Float)
    direction_correct = Column(Boolean)
    
    # Model version tracking
    model_version = Column(String(50), default="v1")

# Database connection
def get_engine():
    """Get database engine from environment"""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL not set")
    return create_engine(db_url)

def init_db():
    """Initialize database tables"""
    engine = get_engine()
    Base.metadata.create_all(engine)
    return engine

def get_session():
    """Get database session"""
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()

def load_coefficients(symbol: str) -> CalibrationCoeff:
    """
    Load calibration coefficients for symbol.
    Returns None if not found - caller should use defaults.
    """
    session = get_session()
    try:
        coeff = session.query(CalibrationCoeff).filter_by(symbol=symbol).first()
        return coeff
    finally:
        session.close()

def save_coefficients(symbol: str, beta_vwap: float, beta_gamma: float, 
                      beta_flow: float, beta_microtrend: float, 
                      intercept: float, sample_size: int):
    """Save or update calibration coefficients"""
    session = get_session()
    try:
        coeff = session.query(CalibrationCoeff).filter_by(symbol=symbol).first()
        
        if coeff:
            coeff.beta_vwap = beta_vwap
            coeff.beta_gamma = beta_gamma
            coeff.beta_flow = beta_flow
            coeff.beta_microtrend = beta_microtrend
            coeff.intercept = intercept
            coeff.sample_size = sample_size
            coeff.updated_at = datetime.utcnow()
        else:
            coeff = CalibrationCoeff(
                symbol=symbol,
                beta_vwap=beta_vwap,
                beta_gamma=beta_gamma,
                beta_flow=beta_flow,
                beta_microtrend=beta_microtrend,
                intercept=intercept,
                sample_size=sample_size
            )
            session.add(coeff)
        
        session.commit()
    finally:
        session.close()

def get_rmse_for_tau(symbol: str, tau: int) -> Optional[RMSEBucket]:
    """Get RMSE bucket for given time-to-close"""
    session = get_session()
    try:
        bucket = session.query(RMSEBucket).filter(
            RMSEBucket.symbol == symbol,
            RMSEBucket.tau_min <= tau,
            RMSEBucket.tau_max > tau
        ).first()
        return bucket
    finally:
        session.close()
