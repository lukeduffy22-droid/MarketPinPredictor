import os
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, Text, Date, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, date
import time
import pytz

# Get database URL from environment
DATABASE_URL = os.getenv('DATABASE_URL')

# Create engine with connection pooling and retry settings
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,  # Verify connections before using them
    pool_recycle=3600,   # Recycle connections after 1 hour
    connect_args={
        "connect_timeout": 10,
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
    }
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class Prediction(Base):
    __tablename__ = 'predictions'
    
    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String(10), index=True)
    index_name = Column(String(50))
    prediction_date = Column(DateTime, default=datetime.utcnow)
    target_date = Column(DateTime)
    current_price = Column(Float)
    predicted_price = Column(Float)
    actual_price = Column(Float, nullable=True)
    confidence = Column(Float)
    model_type = Column(String(50))
    change_pct = Column(Float)
    actual_change_pct = Column(Float, nullable=True)
    accuracy = Column(Float, nullable=True)
    
class Alert(Base):
    __tablename__ = 'alerts'
    
    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String(10), index=True)
    index_name = Column(String(50))
    alert_type = Column(String(50))
    threshold = Column(Float)
    current_value = Column(Float)
    triggered = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    triggered_at = Column(DateTime, nullable=True)
    message = Column(Text)

class GammaPinSnapshot(Base):
    __tablename__ = 'gamma_pin_snapshots'
    __table_args__ = (
        UniqueConstraint('ticker', 'trading_date', 'interval_timestamp', name='uix_gamma_snapshot'),
    )
    
    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String(10), nullable=False, index=True)
    trading_date = Column(Date, nullable=False, index=True)  # Native DATE type
    interval_timestamp = Column(DateTime, nullable=False, index=True)  # Rounded to 15-min boundary
    pin_strike = Column(Float, nullable=False)
    pull_strength = Column(Float, nullable=False)
    spot_price = Column(Float, nullable=False)
    total_gex = Column(Float, nullable=False)
    net_gex = Column(Float, nullable=False)
    is_mock_data = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

def init_db():
    """Initialize database tables with retry logic"""
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            Base.metadata.create_all(bind=engine)
            return True
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            else:
                # Log error but don't crash the app
                print(f"Warning: Database initialization failed after {max_retries} attempts: {str(e)}")
                print("App will continue without database - some features may be limited")
                return False

def get_db():
    """Get database session"""
    db = SessionLocal()
    try:
        return db
    finally:
        pass

def save_prediction(ticker, index_name, current_price, predicted_price, confidence, model_type, change_pct, target_date):
    """Save a prediction to the database"""
    try:
        db = SessionLocal()
        prediction = Prediction(
            ticker=ticker,
            index_name=index_name,
            current_price=current_price,
            predicted_price=predicted_price,
            confidence=confidence,
            model_type=model_type,
            change_pct=change_pct,
            target_date=target_date
        )
        db.add(prediction)
        db.commit()
        db.refresh(prediction)
        return prediction
    except Exception as e:
        print(f"Database error saving prediction: {str(e)}")
        return None
    finally:
        try:
            db.close()
        except:
            pass

def update_prediction_actual(prediction_id, actual_price):
    """Update prediction with actual price and calculate accuracy"""
    db = SessionLocal()
    try:
        prediction = db.query(Prediction).filter(Prediction.id == prediction_id).first()
        if prediction:
            prediction.actual_price = actual_price
            prediction.actual_change_pct = ((actual_price - prediction.current_price) / prediction.current_price) * 100
            
            # Calculate accuracy as inverse of MAPE
            mape = abs((actual_price - prediction.predicted_price) / actual_price) * 100
            prediction.accuracy = max(0, 100 - mape)
            
            db.commit()
            return prediction
        return None
    finally:
        db.close()

def get_predictions_by_ticker(ticker, limit=50):
    """Get recent predictions for a ticker"""
    try:
        db = SessionLocal()
        predictions = db.query(Prediction).filter(
            Prediction.ticker == ticker
        ).order_by(Prediction.prediction_date.desc()).limit(limit).all()
        return predictions
    except Exception as e:
        print(f"Database error fetching predictions: {str(e)}")
        return []
    finally:
        try:
            db.close()
        except:
            pass

def get_all_predictions(limit=100):
    """Get all recent predictions"""
    try:
        db = SessionLocal()
        predictions = db.query(Prediction).order_by(
            Prediction.prediction_date.desc()
        ).limit(limit).all()
        return predictions
    except Exception as e:
        print(f"Database error fetching all predictions: {str(e)}")
        return []
    finally:
        try:
            db.close()
        except:
            pass

def save_alert(ticker, index_name, alert_type, threshold, current_value, message):
    """Save an alert to the database"""
    db = SessionLocal()
    try:
        alert = Alert(
            ticker=ticker,
            index_name=index_name,
            alert_type=alert_type,
            threshold=threshold,
            current_value=current_value,
            message=message
        )
        db.add(alert)
        db.commit()
        db.refresh(alert)
        return alert
    finally:
        db.close()

def get_active_alerts():
    """Get all active (non-triggered) alerts"""
    db = SessionLocal()
    try:
        alerts = db.query(Alert).filter(
            Alert.triggered == False
        ).order_by(Alert.created_at.desc()).all()
        return alerts
    finally:
        db.close()

def trigger_alert(alert_id):
    """Mark an alert as triggered"""
    db = SessionLocal()
    try:
        alert = db.query(Alert).filter(Alert.id == alert_id).first()
        if alert:
            alert.triggered = True
            alert.triggered_at = datetime.utcnow()
            db.commit()
            return alert
        return None
    finally:
        db.close()

def get_prediction_accuracy_stats(ticker=None):
    """Get accuracy statistics for predictions"""
    db = SessionLocal()
    try:
        query = db.query(Prediction).filter(Prediction.actual_price.isnot(None))
        
        if ticker:
            query = query.filter(Prediction.ticker == ticker)
        
        predictions = query.all()
        
        if not predictions:
            return None
        
        accuracies = [p.accuracy for p in predictions if p.accuracy is not None]
        
        return {
            'count': len(predictions),
            'avg_accuracy': sum(accuracies) / len(accuracies) if accuracies else 0,
            'min_accuracy': min(accuracies) if accuracies else 0,
            'max_accuracy': max(accuracies) if accuracies else 0
        }
    finally:
        db.close()

def get_historical_accuracy_for_ai(ticker, model_type=None, limit=20):
    """
    Get historical prediction accuracy data to feed to AI for better predictions.
    
    This provides the AI with insight into how accurate past predictions were,
    helping it calibrate its adjustments more effectively.
    
    Args:
        ticker: Stock ticker (e.g., 'SPX')
        model_type: Optional filter by model type (e.g., 'time_adaptive', 'random_forest')
        limit: Maximum number of recent predictions to analyze
        
    Returns:
        dict with:
            - avg_accuracy: Average accuracy percentage (0-100)
            - recent_predictions: List of recent prediction details
            - bias: Whether predictions tend to be high or low
            - consistency: How consistent the accuracy is
    """
    db = SessionLocal()
    try:
        query = db.query(Prediction).filter(
            Prediction.ticker == ticker,
            Prediction.actual_price.isnot(None),
            Prediction.accuracy.isnot(None)
        )
        
        if model_type:
            query = query.filter(Prediction.model_type == model_type)
        
        predictions = query.order_by(Prediction.prediction_date.desc()).limit(limit).all()
        
        if not predictions:
            return {
                'avg_accuracy': None,
                'recent_predictions': [],
                'bias': 'unknown',
                'consistency': 'unknown',
                'sample_size': 0
            }
        
        accuracies = [p.accuracy for p in predictions if p.accuracy is not None]
        
        errors = []
        for p in predictions:
            if p.predicted_price and p.actual_price:
                error_pct = ((p.predicted_price - p.actual_price) / p.actual_price) * 100
                errors.append(error_pct)
        
        avg_error = sum(errors) / len(errors) if errors else 0
        if avg_error > 0.1:
            bias = 'bullish_bias'
        elif avg_error < -0.1:
            bias = 'bearish_bias'
        else:
            bias = 'neutral'
        
        if accuracies:
            accuracy_std = (sum((a - (sum(accuracies)/len(accuracies)))**2 for a in accuracies) / len(accuracies)) ** 0.5
            if accuracy_std < 5:
                consistency = 'highly_consistent'
            elif accuracy_std < 15:
                consistency = 'moderately_consistent'
            else:
                consistency = 'variable'
        else:
            consistency = 'unknown'
        
        recent_predictions = []
        for p in predictions[:10]:
            recent_predictions.append({
                'date': p.prediction_date.isoformat() if p.prediction_date else None,
                'predicted': p.predicted_price,
                'actual': p.actual_price,
                'accuracy': p.accuracy,
                'error_pct': ((p.predicted_price - p.actual_price) / p.actual_price * 100) if p.actual_price else None
            })
        
        return {
            'avg_accuracy': sum(accuracies) / len(accuracies) if accuracies else None,
            'recent_predictions': recent_predictions,
            'bias': bias,
            'consistency': consistency,
            'sample_size': len(predictions),
            'avg_error_pct': avg_error
        }
    finally:
        db.close()

def get_predictions_needing_actuals(limit=100):
    """
    Find predictions that need actual EOD prices populated.
    
    Returns predictions where:
    - target_date has passed
    - actual_price is still NULL
    
    Args:
        limit: Maximum number of predictions to return
        
    Returns:
        List of Prediction objects needing actuals
    """
    db = SessionLocal()
    try:
        now = datetime.utcnow()
        predictions = db.query(Prediction).filter(
            Prediction.actual_price.is_(None),
            Prediction.target_date <= now
        ).order_by(Prediction.target_date.desc()).limit(limit).all()
        return predictions
    finally:
        db.close()

def batch_update_prediction_actuals(updates):
    """
    Batch update multiple predictions with their actual prices.
    
    Args:
        updates: List of dicts with {'prediction_id': int, 'actual_price': float}
        
    Returns:
        dict with 'updated' count and 'errors' list
    """
    db = SessionLocal()
    results = {'updated': 0, 'errors': []}
    
    try:
        for update in updates:
            try:
                prediction = db.query(Prediction).filter(
                    Prediction.id == update['prediction_id']
                ).first()
                
                if prediction and update.get('actual_price'):
                    prediction.actual_price = update['actual_price']
                    prediction.actual_change_pct = (
                        (update['actual_price'] - prediction.current_price) / 
                        prediction.current_price * 100
                    )
                    mape = abs(
                        (update['actual_price'] - prediction.predicted_price) / 
                        update['actual_price']
                    ) * 100
                    prediction.accuracy = max(0, 100 - mape)
                    results['updated'] += 1
            except Exception as e:
                results['errors'].append({
                    'prediction_id': update.get('prediction_id'),
                    'error': str(e)
                })
        
        db.commit()
    except Exception as e:
        db.rollback()
        results['errors'].append({'batch_error': str(e)})
    finally:
        db.close()
    
    return results

def round_to_15min(dt):
    """
    Round datetime to nearest 15-minute boundary in US/Eastern timezone.
    
    Ensures proper alignment with market intervals (9:30, 9:45, 10:00, etc.)
    and handles timezone conversions correctly.
    
    Args:
        dt: datetime object (can be naive or timezone-aware)
    
    Returns:
        Timezone-aware datetime in US/Eastern, rounded to 15-min boundary
    """
    et_tz = pytz.timezone('US/Eastern')
    
    # Convert to ET timezone
    if dt.tzinfo is None:
        # Assume naive datetime is already in ET
        dt_et = et_tz.localize(dt)
    else:
        # Convert to ET
        dt_et = dt.astimezone(et_tz)
    
    # Round down to nearest 15 minutes
    minutes = (dt_et.minute // 15) * 15
    rounded_et = dt_et.replace(minute=minutes, second=0, microsecond=0)
    
    # Convert to UTC for database storage (best practice for multi-timezone apps)
    return rounded_et.astimezone(pytz.UTC)

def save_gamma_snapshot(ticker, interval_timestamp, pin_strike, pull_strength, spot_price, total_gex, net_gex, is_mock_data=False):
    """
    Save a gamma pin snapshot to the database with automatic deduplication.
    
    Args:
        ticker: Stock ticker (e.g., 'SPX')
        interval_timestamp: Timestamp of the snapshot (will be rounded to 15-min boundary)
        pin_strike: Gamma pin strike price
        pull_strength: Pull strength toward pin
        spot_price: Current spot price
        total_gex: Total gamma exposure
        net_gex: Net gamma exposure
        is_mock_data: Whether this is simulated data (default False)
    
    Returns:
        GammaPinSnapshot object or None on error
    """
    from app.core.gex import validate_gex_invariant
    
    try:
        db = SessionLocal()
        
        # Convert numpy types to Python native types (fixes psycopg2 adapter errors)
        pin_strike = float(pin_strike)
        pull_strength = float(pull_strength)
        spot_price = float(spot_price)
        total_gex = float(total_gex)
        net_gex = float(net_gex)
        
        # Validate GEX invariant before saving (fail closed on invalid data)
        if not validate_gex_invariant(net_gex, total_gex):
            error_msg = f"GEX invariant violated for {ticker}: total_gex ({total_gex}) < |net_gex| ({abs(net_gex)}). Snapshot discarded."
            print(f"ERROR: {error_msg}")
            import logging
            logging.getLogger("database").error(error_msg)
            raise ValueError(error_msg)
        
        # CRITICAL: Normalize timestamp to 15-minute boundary to prevent duplicates
        normalized_timestamp = round_to_15min(interval_timestamp)
        
        # Extract trading date in ET timezone (before UTC conversion for accurate date)
        et_tz = pytz.timezone('US/Eastern')
        trading_date = normalized_timestamp.astimezone(et_tz).date()
        
        # Check if snapshot already exists (database UNIQUE constraint will also enforce this)
        existing = db.query(GammaPinSnapshot).filter(
            GammaPinSnapshot.ticker == ticker,
            GammaPinSnapshot.trading_date == trading_date,
            GammaPinSnapshot.interval_timestamp == normalized_timestamp
        ).first()
        
        if existing:
            # Update existing snapshot (upsert behavior)
            existing.pin_strike = pin_strike
            existing.pull_strength = pull_strength
            existing.spot_price = spot_price
            existing.total_gex = total_gex
            existing.net_gex = net_gex
            existing.is_mock_data = is_mock_data
            db.commit()
            db.refresh(existing)
            return existing
        else:
            # Create new snapshot
            snapshot = GammaPinSnapshot(
                ticker=ticker,
                trading_date=trading_date,
                interval_timestamp=normalized_timestamp,
                pin_strike=pin_strike,
                pull_strength=pull_strength,
                spot_price=spot_price,
                total_gex=total_gex,
                net_gex=net_gex,
                is_mock_data=is_mock_data
            )
            db.add(snapshot)
            db.commit()
            db.refresh(snapshot)
            return snapshot
    except Exception as e:
        print(f"Database error saving gamma snapshot: {str(e)}")
        return None
    finally:
        try:
            db.close()
        except:
            pass

def get_gamma_snapshots_for_day(ticker, trading_date_obj):
    """
    Get all gamma snapshots for a ticker on a specific trading day
    
    Args:
        ticker: Stock ticker (e.g., 'SPX')
        trading_date_obj: datetime.date object or datetime object
    
    Returns:
        List of GammaPinSnapshot objects ordered by time
    """
    try:
        db = SessionLocal()
        
        # Ensure we have a date object
        if isinstance(trading_date_obj, datetime):
            trading_date_obj = trading_date_obj.date()
        
        snapshots = db.query(GammaPinSnapshot).filter(
            GammaPinSnapshot.ticker == ticker,
            GammaPinSnapshot.trading_date == trading_date_obj
        ).order_by(GammaPinSnapshot.interval_timestamp.asc()).all()
        return snapshots
    except Exception as e:
        print(f"Database error fetching gamma snapshots: {str(e)}")
        return []
    finally:
        try:
            db.close()
        except:
            pass

def get_latest_gamma_snapshot(ticker):
    """Get the most recent gamma snapshot for a ticker"""
    try:
        db = SessionLocal()
        snapshot = db.query(GammaPinSnapshot).filter(
            GammaPinSnapshot.ticker == ticker
        ).order_by(GammaPinSnapshot.interval_timestamp.desc()).first()
        return snapshot
    except Exception as e:
        print(f"Database error fetching latest gamma snapshot: {str(e)}")
        return None
    finally:
        try:
            db.close()
        except:
            pass
