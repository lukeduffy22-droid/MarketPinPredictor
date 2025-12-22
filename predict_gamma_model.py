import sys
import json
import pandas as pd
import numpy as np
import torch
import torch.nn as nn

# ============================================================
# MODEL DEFINITION (must match training)
# ============================================================

class MLP(nn.Module):
    def __init__(self, input_dim):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(input_dim, 64),
            nn.ReLU(),
            nn.Linear(64, 64),
            nn.ReLU(),
            nn.Linear(64, 1)
        )

    def forward(self, x):
        return self.net(x)

# ============================================================
# INFERENCE FUNCTION
# ============================================================

def run_inference(full_csv, index_name, output_csv=None):

    # Load metadata
    meta_file = f"gamma_model_{index_name}_meta.json"
    model_file = f"gamma_model_{index_name}.pt"

    print(f"Loading model: {model_file}")
    print(f"Loading metadata: {meta_file}")

    with open(meta_file, "r") as f:
        meta = json.load(f)

    feature_cols = meta["feature_columns"]

    # NOTE: If scaling was used during training, apply it here
    # scaling = meta.get("scaling")
    # if scaling:
    #     X = (X - scaling["mean"]) / scaling["std"]

    # Load model
    model = MLP(input_dim=len(feature_cols))
    model.load_state_dict(torch.load(model_file, map_location="cpu"))
    model.eval()

    # Load unified dataset
    print(f"Loading data: {full_csv}")
    df = pd.read_csv(full_csv)

    # Strict index filtering
    if "symbol" not in df.columns:
        raise ValueError("symbol column missing — cannot filter index safely.")

    df = df[df["symbol"].str.strip() == index_name]
    if df.empty:
        raise ValueError(f"No rows found for index: {index_name}")

    print(f"Found {len(df)} rows for {index_name}")

    # Sort by timestamp for proper temporal ordering
    if "timestamp_utc" in df.columns:
        df = df.sort_values("timestamp_utc").reset_index(drop=True)
        print(f"Sorted by timestamp_utc")

    # Check for NaN gamma_pin before computing target
    if df["gamma_pin"].isna().any():
        nan_count = df["gamma_pin"].isna().sum()
        raise ValueError(f"gamma_pin contains {nan_count} NaN values — cannot compute distance_to_pin")

    # Compute distance_to_pin (same as training)
    df["distance_to_pin"] = df["spot"] - df["gamma_pin"]

    # Check for missing features
    missing_cols = [c for c in feature_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing feature columns: {missing_cols}")

    # Extract features
    X = df[feature_cols].values.astype(np.float32)
    X_tensor = torch.tensor(X, dtype=torch.float32)

    # Predict distance to pin
    with torch.no_grad():
        pred_distance = model(X_tensor).numpy().flatten()

    # Reconstruct predicted close
    predicted_close = pred_distance + df["gamma_pin"].values

    # Confidence metric (simple inverse error proxy)
    confidence = 1 / (1 + np.abs(pred_distance))

    # Build output DataFrame
    out = pd.DataFrame({
        "timestamp_utc": df["timestamp_utc"],
        "symbol": df["symbol"],
        "spot": df["spot"],
        "gamma_pin": df["gamma_pin"],
        "predicted_distance_to_pin": pred_distance,
        "predicted_close": predicted_close,
        "confidence": confidence
    })

    print("\nPredictions (last 10 rows):")
    print(out.tail(10))

    # Optionally save to CSV
    if output_csv:
        out.to_csv(output_csv, index=False)
        print(f"\nSaved predictions to {output_csv}")

    return out

# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python predict_gamma_model.py <full_dataset_csv> <index_name> [output_csv]")
        print("")
        print("Examples:")
        print("  python predict_gamma_model.py all_indices_2025-12-22.csv SPX")
        print("  python predict_gamma_model.py all_indices_2025-12-22.csv NDX predictions_NDX.csv")
        print("  python predict_gamma_model.py all_indices_2025-12-22.csv RUT")
        sys.exit(1)

    full_csv = sys.argv[1]
    index_name = sys.argv[2]
    output_csv = sys.argv[3] if len(sys.argv) > 3 else None

    run_inference(full_csv, index_name, output_csv)
