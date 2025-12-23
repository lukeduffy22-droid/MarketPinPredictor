# validate_gamma_predictions.py
import sys
import pandas as pd
import numpy as np

def validate_predictions(predictions_csv, actuals_csv=None, output_csv=None):
    """
    Validate gamma model predictions against actual closes.
    
    If actuals_csv is provided, it should have columns: timestamp_utc, symbol, actual_close
    If not provided, predictions_csv should have an 'actual_close' column already.
    """
    
    print(f"Loading predictions: {predictions_csv}")
    df = pd.read_csv(predictions_csv)
    
    if actuals_csv:
        print(f"Loading actuals: {actuals_csv}")
        actuals = pd.read_csv(actuals_csv)
        df = df.merge(actuals[['timestamp_utc', 'symbol', 'actual_close']], 
                      on=['timestamp_utc', 'symbol'], 
                      how='left')
    
    if 'actual_close' not in df.columns:
        raise ValueError("actual_close column not found. Provide actuals_csv or include it in predictions file.")
    
    df = df.dropna(subset=['actual_close', 'predicted_close'])
    
    if df.empty:
        print("No valid rows with both predicted and actual close values.")
        return None
    
    print(f"Validating {len(df)} predictions")
    
    df['error_points'] = df['predicted_close'] - df['actual_close']
    df['error_abs'] = np.abs(df['error_points'])
    df['error_bps'] = (df['error_points'] / df['actual_close']) * 10000
    df['error_bps_abs'] = np.abs(df['error_bps'])
    
    df['actual_distance_to_pin'] = df['actual_close'] - df['gamma_pin']
    df['predicted_distance_to_pin'] = df.get('predicted_distance_to_pin', 
                                              df['predicted_close'] - df['gamma_pin'])
    df['distance_error'] = df['predicted_distance_to_pin'] - df['actual_distance_to_pin']
    
    df['direction_correct'] = (
        (df['predicted_close'] > df['spot']) == (df['actual_close'] > df['spot'])
    )
    
    print("\n" + "="*60)
    print("VALIDATION REPORT")
    print("="*60)
    
    print(f"\n📊 Dataset Summary")
    print(f"   Total Predictions: {len(df)}")
    print(f"   Symbols: {df['symbol'].unique().tolist()}")
    
    print(f"\n📏 Error Metrics (Points)")
    print(f"   Mean Error:       {df['error_points'].mean():+.2f}")
    print(f"   Mean Abs Error:   {df['error_abs'].mean():.2f}")
    print(f"   Median Abs Error: {df['error_abs'].median():.2f}")
    print(f"   Max Abs Error:    {df['error_abs'].max():.2f}")
    print(f"   Std Dev:          {df['error_points'].std():.2f}")
    
    print(f"\n📐 Error Metrics (Basis Points)")
    print(f"   Mean Error:       {df['error_bps'].mean():+.1f} bps")
    print(f"   Mean Abs Error:   {df['error_bps_abs'].mean():.1f} bps")
    print(f"   Median Abs Error: {df['error_bps_abs'].median():.1f} bps")
    
    print(f"\n🎯 Distance to Pin Analysis")
    print(f"   Mean Predicted Distance: {df['predicted_distance_to_pin'].mean():+.2f}")
    print(f"   Mean Actual Distance:    {df['actual_distance_to_pin'].mean():+.2f}")
    print(f"   Mean Distance Error:     {df['distance_error'].mean():+.2f}")
    
    direction_acc = df['direction_correct'].mean() * 100
    print(f"\n🧭 Direction Accuracy: {direction_acc:.1f}%")
    
    if 'confidence' in df.columns:
        high_conf = df[df['confidence'] > 0.7]
        med_conf = df[(df['confidence'] >= 0.4) & (df['confidence'] <= 0.7)]
        low_conf = df[df['confidence'] < 0.4]
        
        print(f"\n📈 Accuracy by Confidence Level")
        if len(high_conf) > 0:
            print(f"   High (>70%):  {high_conf['error_abs'].mean():.2f} pts avg error ({len(high_conf)} predictions)")
        if len(med_conf) > 0:
            print(f"   Medium (40-70%): {med_conf['error_abs'].mean():.2f} pts avg error ({len(med_conf)} predictions)")
        if len(low_conf) > 0:
            print(f"   Low (<40%):   {low_conf['error_abs'].mean():.2f} pts avg error ({len(low_conf)} predictions)")
    
    for symbol in df['symbol'].unique():
        sym_df = df[df['symbol'] == symbol]
        print(f"\n📌 {symbol}")
        print(f"   Count: {len(sym_df)}")
        print(f"   MAE: {sym_df['error_abs'].mean():.2f} pts")
        print(f"   MAPE: {sym_df['error_bps_abs'].mean():.1f} bps")
        print(f"   Direction: {sym_df['direction_correct'].mean()*100:.1f}%")
    
    print("\n" + "="*60)
    
    report = df[[
        'timestamp_utc', 'symbol', 'spot', 'gamma_pin',
        'predicted_close', 'actual_close', 'error_points', 'error_bps',
        'predicted_distance_to_pin', 'actual_distance_to_pin', 'distance_error',
        'direction_correct'
    ]].copy()
    
    if 'confidence' in df.columns:
        report['confidence'] = df['confidence']
    
    if output_csv:
        report.to_csv(output_csv, index=False)
        print(f"\nValidation report saved to {output_csv}")
    
    return report

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python validate_gamma_predictions.py <predictions_csv> [actuals_csv] [output_csv]")
        print("")
        print("If actuals_csv is not provided, predictions_csv must contain 'actual_close' column.")
        print("")
        print("Examples:")
        print("  python validate_gamma_predictions.py predictions_SPX.csv actuals.csv validation_report.csv")
        print("  python validate_gamma_predictions.py predictions_with_actuals.csv - validation_report.csv")
        sys.exit(1)
    
    predictions_csv = sys.argv[1]
    actuals_csv = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] != '-' else None
    output_csv = sys.argv[3] if len(sys.argv) > 3 else None
    
    validate_predictions(predictions_csv, actuals_csv, output_csv)
