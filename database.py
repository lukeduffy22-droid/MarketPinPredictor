import os
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import time

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
