"""
Database models for calibration coefficients and RMSE tracking.
Per-symbol and per-time-bucket storage for adaptive predictions.
"""
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import make_url
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
    Now includes regime flags for regime-aware error analysis.
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
    
    # Regime flags (new) - for regime-aware error analysis
    regime_half_day = Column(Boolean, default=False)
    regime_holiday_adjacent = Column(Boolean, default=False)
    regime_eom = Column(Boolean, default=False)  # End of month
    regime_eow = Column(Boolean, default=False)  # End of week

def _build_engine():
    """Build a shared SQLAlchemy engine with dialect-aware settings."""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        db_url = 'sqlite:///./market_predictor.db'
        print(f"⚠️ DATABASE_URL not set, using SQLite: {db_url}")

    parsed = make_url(db_url)
    backend = parsed.get_backend_name()
    drivername = parsed.drivername

    engine_kwargs = {}
    connect_args = {}

    if backend == "sqlite":
        connect_args = {"check_same_thread": False}
    elif backend == "postgresql":
        engine_kwargs = {
            "pool_pre_ping": True,
            "pool_recycle": 3600,
        }
        if (
            drivername == "postgresql"
            or "+psycopg2" in drivername
            or "+psycopg" in drivername
        ):
            connect_args = {
                "connect_timeout": int(os.getenv("DB_CONNECT_TIMEOUT", "10")),
                "application_name": "MarketPinPredictor",
            }

    if connect_args:
        return create_engine(db_url, connect_args=connect_args, **engine_kwargs)
    return create_engine(db_url, **engine_kwargs)


ENGINE = _build_engine()
SessionLocal = sessionmaker(bind=ENGINE)


# Database connection
def get_engine():
    """Return module-level shared engine."""
    return ENGINE

def init_db():
    """Initialize database tables"""
    Base.metadata.create_all(ENGINE)
    return ENGINE

def get_session():
    """Get database session"""
    return SessionLocal()

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


def get_mae_by_regime(symbol: str = None, days: int = 30) -> dict:
    """
    Compute MAE (Mean Absolute Error) by regime type.
    Helps identify if model has systematic bias in specific session types.
    
    Args:
        symbol: Optional symbol filter (None = all symbols)
        days: Number of days to look back (default 30)
    
    Returns:
        Dict with MAE statistics by regime:
        {
            'overall': {'mae': float, 'mae_pct': float, 'n_samples': int, 'bias': float},
            'half_day': {'mae': float, 'mae_pct': float, 'n_samples': int, 'bias': float},
            'regular_day': {'mae': float, 'mae_pct': float, 'n_samples': int, 'bias': float},
            'eom': {'mae': float, 'mae_pct': float, 'n_samples': int, 'bias': float},
            'eow': {'mae': float, 'mae_pct': float, 'n_samples': int, 'bias': float},
            'holiday_adjacent': {'mae': float, 'mae_pct': float, 'n_samples': int, 'bias': float},
        }
    """
    from datetime import timedelta
    from sqlalchemy import and_, func
    
    session = get_session()
    try:
        cutoff_date = datetime.utcnow().date() - timedelta(days=days)
        
        # Base query filter
        base_filter = [
            PredictionLog.prediction_date >= cutoff_date,
            PredictionLog.actual_close.isnot(None),
            PredictionLog.error_abs.isnot(None)
        ]
        if symbol:
            base_filter.append(PredictionLog.symbol == symbol)
        
        def compute_stats(filter_conditions) -> dict:
            """Compute MAE stats for given filter conditions"""
            query = session.query(
                func.avg(PredictionLog.error_abs).label('mae'),
                func.avg(PredictionLog.error_pct).label('mae_pct'),
                func.count(PredictionLog.id).label('n_samples'),
                func.avg(PredictionLog.predicted_close - PredictionLog.actual_close).label('bias')
            ).filter(and_(*filter_conditions))
            
            result = query.first()
            if result and result.n_samples > 0:
                return {
                    'mae': round(float(result.mae or 0), 2),
                    'mae_pct': round(float(result.mae_pct or 0), 4),
                    'n_samples': result.n_samples,
                    'bias': round(float(result.bias or 0), 2)
                }
            return {'mae': 0.0, 'mae_pct': 0.0, 'n_samples': 0, 'bias': 0.0}
        
        return {
            'overall': compute_stats(base_filter),
            'half_day': compute_stats(base_filter + [PredictionLog.regime_half_day == True]),
            'regular_day': compute_stats(base_filter + [PredictionLog.regime_half_day != True]),
            'eom': compute_stats(base_filter + [PredictionLog.regime_eom == True]),
            'eow': compute_stats(base_filter + [PredictionLog.regime_eow == True]),
            'holiday_adjacent': compute_stats(base_filter + [PredictionLog.regime_holiday_adjacent == True]),
        }
    except Exception as e:
        return {
            'overall': {'mae': 0.0, 'mae_pct': 0.0, 'n_samples': 0, 'bias': 0.0, 'error': str(e)},
            'half_day': {'mae': 0.0, 'mae_pct': 0.0, 'n_samples': 0, 'bias': 0.0},
            'regular_day': {'mae': 0.0, 'mae_pct': 0.0, 'n_samples': 0, 'bias': 0.0},
            'eom': {'mae': 0.0, 'mae_pct': 0.0, 'n_samples': 0, 'bias': 0.0},
            'eow': {'mae': 0.0, 'mae_pct': 0.0, 'n_samples': 0, 'bias': 0.0},
            'holiday_adjacent': {'mae': 0.0, 'mae_pct': 0.0, 'n_samples': 0, 'bias': 0.0},
        }
    finally:
        session.close()
