import os
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, Text, Date, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, date
import time
import pytz

# Get database URL from environment or use SQLite as fallback
DATABASE_URL = os.getenv('DATABASE_URL')

if not DATABASE_URL:
    # Fallback to SQLite for local development/testing
    DATABASE_URL = 'sqlite:///./market_predictor.db'
    print(f"⚠️ DATABASE_URL not set, using SQLite: {DATABASE_URL}")
    engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
else:
    # PostgreSQL with connection pooling and retry settings
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
    is_valid = Column(Boolean, default=False, nullable=True)  # Validity flag for model training
    validation_reasons = Column(String, nullable=True)  # Reasons if invalid
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class GammaAuditSnapshot(Base):
    """
    Comprehensive gamma audit snapshot storage.
    Stores all fields from the audit snapshot for historical analysis and model training.
    """
    __tablename__ = 'gamma_audit_snapshots'
    __table_args__ = (
        UniqueConstraint('symbol', 'generated_at_utc', name='uix_audit_snapshot'),
    )
    
    id = Column(Integer, primary_key=True, index=True)
    
    symbol = Column(String(10), nullable=False, index=True)
    trading_date = Column(Date, nullable=False, index=True)
    generated_at_utc = Column(DateTime, nullable=False, index=True)
    
    spot_last = Column(Float, nullable=False)
    spot_source = Column(String(50))
    
    primary_gamma_pin_strike = Column(Float)
    primary_gamma_pin_abs_gex = Column(Float)
    zero_gamma_level = Column(Float)
    
    call_gex_total = Column(Float)
    put_gex_total = Column(Float)
    gross_gex = Column(Float)
    net_gex = Column(Float)
    
    contracts_count = Column(Integer)
    expirations_min_days = Column(Integer)
    expirations_max_days = Column(Integer)
    
    validation_is_valid = Column(Boolean, default=True)
    validation_failure_reasons = Column(Text)
    
    pin_drift_points_per_hour = Column(Float)
    pin_change_points = Column(Float)
    prev_pin_strike = Column(Float)
    
    confidence = Column(Float)
    dispersion_ratio = Column(Float)
    vol_regime = Column(String(20))
    vol_regime_iv = Column(Float)
    
    top_strikes_json = Column(Text)
    
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

def save_gamma_snapshot(ticker, interval_timestamp, pin_strike, pull_strength, spot_price, total_gex, net_gex, is_mock_data=False, is_valid=True, validation_reasons=None):
    """
    Save a gamma pin snapshot to the database with automatic deduplication.
    
    NOW SAVES ALL SNAPSHOTS with validity flag - no snapshots are discarded.
    
    Args:
        ticker: Stock ticker (e.g., 'SPX')
        interval_timestamp: Timestamp of the snapshot (will be rounded to 15-min boundary)
        pin_strike: Gamma pin strike price
        pull_strength: Pull strength toward pin
        spot_price: Current spot price
        total_gex: Total gamma exposure
        net_gex: Net gamma exposure
        is_mock_data: Whether this is simulated data (default False)
        is_valid: Whether this snapshot passed validation (default True)
        validation_reasons: List or string of validation failure reasons (default None)
    
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
        
        # Convert validation_reasons to string if it's a list
        if isinstance(validation_reasons, list):
            validation_reasons = ', '.join(validation_reasons) if validation_reasons else None
        
        # Log GEX invariant issues but DON'T discard - we save all snapshots now
        if not validate_gex_invariant(net_gex, total_gex):
            warning_msg = f"GEX invariant violated for {ticker}: total_gex ({total_gex}) < |net_gex| ({abs(net_gex)}). Saving with is_valid=False."
            print(f"WARNING: {warning_msg}")
            import logging
            logging.getLogger("database").warning(warning_msg)
            is_valid = False
            if validation_reasons:
                validation_reasons += f", GEX_INVARIANT_VIOLATED"
            else:
                validation_reasons = "GEX_INVARIANT_VIOLATED"
        
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
            # PROTECT VALID SNAPSHOTS: 
            # - If new snapshot is VALID: always update (better data wins)
            # - If new snapshot is INVALID and existing is VALID (or NULL/legacy): DO NOT overwrite
            # - If new snapshot is INVALID and existing is explicitly INVALID: update (same quality)
            existing_is_valid = existing.is_valid is True  # NULL/False treated as "protected legacy"
            
            if is_valid:
                # Valid snapshots always allowed to update
                should_update = True
            elif existing_is_valid:
                # Invalid snapshot cannot overwrite valid/legacy data
                should_update = False
            else:
                # Both invalid - allow update (same quality, newer data)
                should_update = True
            
            if should_update:
                existing.pin_strike = pin_strike
                existing.pull_strength = pull_strength
                existing.spot_price = spot_price
                existing.total_gex = total_gex
                existing.net_gex = net_gex
                existing.is_mock_data = is_mock_data
                existing.is_valid = is_valid
                existing.validation_reasons = validation_reasons
                db.commit()
                db.refresh(existing)
            else:
                import logging
                logging.getLogger("database").info(
                    f"Preserving valid snapshot for {ticker} at {normalized_timestamp} - not overwriting with invalid"
                )
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
                is_mock_data=is_mock_data,
                is_valid=is_valid,
                validation_reasons=validation_reasons
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


def save_audit_snapshot_to_db(snapshot_dict):
    """
    Save an audit snapshot to the database.
    
    Args:
        snapshot_dict: Dictionary containing snapshot fields
        
    Returns:
        GammaAuditSnapshot object or None on error
    """
    try:
        db = SessionLocal()
        
        generated_at_str = snapshot_dict.get('generated_at_utc', '')
        if isinstance(generated_at_str, str) and generated_at_str:
            generated_at_utc = datetime.fromisoformat(generated_at_str.replace('Z', '+00:00'))
        elif isinstance(generated_at_str, datetime):
            generated_at_utc = generated_at_str
        else:
            generated_at_utc = datetime.utcnow()
        
        et_tz = pytz.timezone('US/Eastern')
        trading_date = generated_at_utc.astimezone(et_tz).date() if generated_at_utc.tzinfo else generated_at_utc.date()
        
        symbol = snapshot_dict.get('symbol', '')
        
        existing = db.query(GammaAuditSnapshot).filter(
            GammaAuditSnapshot.symbol == symbol,
            GammaAuditSnapshot.generated_at_utc == generated_at_utc
        ).first()
        
        validation_reasons = snapshot_dict.get('validation_failure_reasons', [])
        if isinstance(validation_reasons, list):
            validation_reasons = ','.join(validation_reasons)
        
        top_strikes = snapshot_dict.get('top_strikes_by_abs_gex', [])
        if isinstance(top_strikes, list):
            import json
            top_strikes = json.dumps(top_strikes)
        
        if existing:
            existing.spot_last = float(snapshot_dict.get('spot_last', 0))
            existing.spot_source = snapshot_dict.get('spot_source', '')
            existing.primary_gamma_pin_strike = snapshot_dict.get('primary_gamma_pin_strike')
            existing.primary_gamma_pin_abs_gex = snapshot_dict.get('primary_gamma_pin_abs_gex')
            existing.zero_gamma_level = snapshot_dict.get('zero_gamma_level')
            existing.call_gex_total = snapshot_dict.get('call_gex_total')
            existing.put_gex_total = snapshot_dict.get('put_gex_total')
            existing.gross_gex = snapshot_dict.get('gross_gex')
            existing.net_gex = snapshot_dict.get('net_gex')
            existing.contracts_count = snapshot_dict.get('contracts_count')
            existing.expirations_min_days = snapshot_dict.get('expirations_min_days')
            existing.expirations_max_days = snapshot_dict.get('expirations_max_days')
            existing.validation_is_valid = snapshot_dict.get('validation_is_valid', True)
            existing.validation_failure_reasons = validation_reasons
            existing.pin_drift_points_per_hour = snapshot_dict.get('pin_drift_points_per_hour')
            existing.pin_change_points = snapshot_dict.get('pin_change_points')
            existing.prev_pin_strike = snapshot_dict.get('prev_pin_strike')
            existing.confidence = snapshot_dict.get('confidence')
            existing.dispersion_ratio = snapshot_dict.get('dispersion_ratio')
            existing.vol_regime = snapshot_dict.get('vol_regime')
            existing.vol_regime_iv = snapshot_dict.get('vol_regime_iv')
            existing.top_strikes_json = top_strikes
            db.commit()
            db.refresh(existing)
            return existing
        else:
            audit_snapshot = GammaAuditSnapshot(
                symbol=symbol,
                trading_date=trading_date,
                generated_at_utc=generated_at_utc,
                spot_last=float(snapshot_dict.get('spot_last', 0)),
                spot_source=snapshot_dict.get('spot_source', ''),
                primary_gamma_pin_strike=snapshot_dict.get('primary_gamma_pin_strike'),
                primary_gamma_pin_abs_gex=snapshot_dict.get('primary_gamma_pin_abs_gex'),
                zero_gamma_level=snapshot_dict.get('zero_gamma_level'),
                call_gex_total=snapshot_dict.get('call_gex_total'),
                put_gex_total=snapshot_dict.get('put_gex_total'),
                gross_gex=snapshot_dict.get('gross_gex'),
                net_gex=snapshot_dict.get('net_gex'),
                contracts_count=snapshot_dict.get('contracts_count'),
                expirations_min_days=snapshot_dict.get('expirations_min_days'),
                expirations_max_days=snapshot_dict.get('expirations_max_days'),
                validation_is_valid=snapshot_dict.get('validation_is_valid', True),
                validation_failure_reasons=validation_reasons,
                pin_drift_points_per_hour=snapshot_dict.get('pin_drift_points_per_hour'),
                pin_change_points=snapshot_dict.get('pin_change_points'),
                prev_pin_strike=snapshot_dict.get('prev_pin_strike'),
                confidence=snapshot_dict.get('confidence'),
                dispersion_ratio=snapshot_dict.get('dispersion_ratio'),
                vol_regime=snapshot_dict.get('vol_regime'),
                vol_regime_iv=snapshot_dict.get('vol_regime_iv'),
                top_strikes_json=top_strikes
            )
            db.add(audit_snapshot)
            db.commit()
            db.refresh(audit_snapshot)
            return audit_snapshot
    except Exception as e:
        print(f"Database error saving audit snapshot: {str(e)}")
        return None
    finally:
        try:
            db.close()
        except:
            pass


def get_audit_snapshots_for_day(symbol, trading_date_obj):
    """
    Get all audit snapshots for a symbol on a specific trading day.
    
    Args:
        symbol: Stock ticker (e.g., 'SPX')
        trading_date_obj: datetime.date object or datetime object
    
    Returns:
        List of GammaAuditSnapshot objects ordered by time
    """
    try:
        db = SessionLocal()
        
        if isinstance(trading_date_obj, datetime):
            trading_date_obj = trading_date_obj.date()
        
        snapshots = db.query(GammaAuditSnapshot).filter(
            GammaAuditSnapshot.symbol == symbol,
            GammaAuditSnapshot.trading_date == trading_date_obj
        ).order_by(GammaAuditSnapshot.generated_at_utc.asc()).all()
        return snapshots
    except Exception as e:
        print(f"Database error fetching audit snapshots: {str(e)}")
        return []
    finally:
        try:
            db.close()
        except:
            pass


def get_audit_snapshots_for_date_range(symbol, start_date, end_date):
    """
    Get audit snapshots for a symbol within a date range.
    
    Args:
        symbol: Stock ticker (e.g., 'SPX')
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
    
    Returns:
        List of GammaAuditSnapshot objects
    """
    try:
        db = SessionLocal()
        
        if isinstance(start_date, datetime):
            start_date = start_date.date()
        if isinstance(end_date, datetime):
            end_date = end_date.date()
        
        snapshots = db.query(GammaAuditSnapshot).filter(
            GammaAuditSnapshot.symbol == symbol,
            GammaAuditSnapshot.trading_date >= start_date,
            GammaAuditSnapshot.trading_date <= end_date
        ).order_by(GammaAuditSnapshot.generated_at_utc.asc()).all()
        return snapshots
    except Exception as e:
        print(f"Database error fetching audit snapshots: {str(e)}")
        return []
    finally:
        try:
            db.close()
        except:
            pass


def get_latest_audit_snapshot(symbol):
    """Get the most recent audit snapshot for a symbol"""
    try:
        db = SessionLocal()
        snapshot = db.query(GammaAuditSnapshot).filter(
            GammaAuditSnapshot.symbol == symbol
        ).order_by(GammaAuditSnapshot.generated_at_utc.desc()).first()
        return snapshot
    except Exception as e:
        print(f"Database error fetching latest audit snapshot: {str(e)}")
        return None
    finally:
        try:
            db.close()
        except:
            pass


def export_audit_snapshots_to_csv(symbol=None, start_date=None, end_date=None):
    """
    Export audit snapshots to a pandas DataFrame for analysis/training.
    
    Args:
        symbol: Optional symbol filter
        start_date: Optional start date
        end_date: Optional end date
    
    Returns:
        pandas DataFrame with snapshot data
    """
    import pandas as pd
    
    try:
        db = SessionLocal()
        
        query = db.query(GammaAuditSnapshot)
        
        if symbol:
            query = query.filter(GammaAuditSnapshot.symbol == symbol)
        if start_date:
            if isinstance(start_date, datetime):
                start_date = start_date.date()
            query = query.filter(GammaAuditSnapshot.trading_date >= start_date)
        if end_date:
            if isinstance(end_date, datetime):
                end_date = end_date.date()
            query = query.filter(GammaAuditSnapshot.trading_date <= end_date)
        
        snapshots = query.order_by(GammaAuditSnapshot.generated_at_utc.asc()).all()
        
        data = []
        for s in snapshots:
            data.append({
                'symbol': s.symbol,
                'timestamp_utc': s.generated_at_utc,
                'trading_date': s.trading_date,
                'spot': s.spot_last,
                'gamma_pin': s.primary_gamma_pin_strike,
                'distance_to_pin': (s.spot_last - s.primary_gamma_pin_strike) if s.primary_gamma_pin_strike else None,
                'gross_gex': s.gross_gex,
                'net_gex': s.net_gex,
                'call_gex': s.call_gex_total,
                'put_gex': s.put_gex_total,
                'is_valid': s.validation_is_valid,
                'confidence': s.confidence,
                'vol_regime': s.vol_regime
            })
        
        return pd.DataFrame(data)
    except Exception as e:
        print(f"Database error exporting audit snapshots: {str(e)}")
        return pd.DataFrame()
    finally:
        try:
            db.close()
        except:
            pass
