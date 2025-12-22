import sys
import json
import pandas as pd
import numpy as np
from typing import List

import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader

# ============================================================
# CONFIG
# ============================================================

TARGET_COLUMN = "distance_to_pin"
BATCH_SIZE = 64
EPOCHS = 40
LR = 1e-3
VALIDATION_SPLIT = 0.2
RANDOM_SEED = 42

torch.manual_seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

# ============================================================
# DATASET
# ============================================================

class TabularDataset(Dataset):
    def __init__(self, X, y):
        self.X = torch.tensor(X, dtype=torch.float32)
        self.y = torch.tensor(y, dtype=torch.float32).view(-1, 1)

    def __len__(self):
        return len(self.X)

    def __getitem__(self, idx):
        return self.X[idx], self.y[idx]

# ============================================================
# MODEL
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
# FEATURE SELECTION
# ============================================================

def select_feature_columns(df):
    exclude = {
        TARGET_COLUMN,
        "predicted_price",
        "confidence",
        "index_name",
        "timestamp",
        "prediction_date",
        "Time",
        "Gamma Pin"
    }
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    return [c for c in numeric_cols if c not in exclude]

# ============================================================
# MAIN TRAINING FUNCTION
# ============================================================

def train_model(price_csv, gamma_csv, index_name, model_out, meta_out):

    # Load price data
    df = pd.read_csv(price_csv)

    # Ensure index_name column exists
    if "index_name" not in df.columns:
        raise ValueError("index_name column missing — cannot safely filter index.")

    # Strict index filtering
    df = df[df["index_name"].str.strip() == index_name]
    if df.empty:
        raise ValueError(f"No rows found for index: {index_name}")

    # Load gamma snapshots
    gamma = pd.read_csv(gamma_csv)

    # Clean gamma fields
    gamma["Spot"] = gamma["Spot"].replace("[\$,]", "", regex=True).astype(float)
    gamma["Gross GEX"] = gamma["Gross GEX"].replace("[\$,B]", "", regex=True).astype(float)
    gamma["Net GEX"] = gamma["Net GEX"].replace("[\$,B]", "", regex=True).astype(float)

    # Merge on nearest timestamp
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    gamma["Time"] = pd.to_datetime(gamma["Time"])
    df = pd.merge_asof(df.sort_values("timestamp"),
                       gamma.sort_values("Time"),
                       left_on="timestamp",
                       right_on="Time",
                       direction="nearest")

    # Add gamma features
    df["gamma_pin"] = df["Gamma Pin"].replace("[\$,]", "", regex=True).astype(float)
    df["distance_to_pin"] = df["close"] - df["gamma_pin"]
    df["zero_gamma_distance"] = df["close"] - df["Spot"]
    df["net_gex_norm"] = df["Net GEX"] / df["close"]

    # Drop rows missing target
    df = df.dropna(subset=["distance_to_pin"])

    # Select features
    feature_cols = select_feature_columns(df)
    X = df[feature_cols].values
    y = df[TARGET_COLUMN].values

    # Train/val split
    n = len(X)
    idx = np.arange(n)
    np.random.shuffle(idx)
    split = int(n * (1 - VALIDATION_SPLIT))
    train_idx, val_idx = idx[:split], idx[split:]

    X_train, y_train = X[train_idx], y[train_idx]
    X_val, y_val = X[val_idx], y[val_idx]

    train_ds = TabularDataset(X_train, y_train)
    val_ds = TabularDataset(X_val, y_val)

    train_loader = DataLoader(train_ds, batch_size=BATCH_SIZE, shuffle=True)
    val_loader = DataLoader(val_ds, batch_size=BATCH_SIZE, shuffle=False)

    # Model
    model = MLP(input_dim=X.shape[1])
    optimizer = torch.optim.Adam(model.parameters(), lr=LR)
    loss_fn = nn.MSELoss()

    # Training loop
    for epoch in range(1, EPOCHS + 1):
        model.train()
        train_losses = []
        for xb, yb in train_loader:
            optimizer.zero_grad()
            pred = model(xb)
            loss = loss_fn(pred, yb)
            loss.backward()
            optimizer.step()
            train_losses.append(loss.item())

        model.eval()
        val_losses = []
        with torch.no_grad():
            for xb, yb in val_loader:
                pred = model(xb)
                loss = loss_fn(pred, yb)
                val_losses.append(loss.item())

        print(f"Epoch {epoch:03d} | Train {np.mean(train_losses):.4f} | Val {np.mean(val_losses):.4f}")

    # Save model + metadata
    torch.save(model.state_dict(), model_out)
    meta = {
        "feature_columns": feature_cols,
        "target": TARGET_COLUMN,
        "index_name": index_name
    }
    with open(meta_out, "w") as f:
        json.dump(meta, f, indent=2)

    print("Training complete.")

# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python train_gamma_model.py <price_csv> <gamma_csv> <index_name>")
        print("")
        print("Examples:")
        print("  python train_gamma_model.py prices.csv gamma.csv SPX")
        print("  python train_gamma_model.py prices.csv gamma.csv NDX")
        print("  python train_gamma_model.py prices.csv gamma.csv RUT")
        sys.exit(1)

    price_csv = sys.argv[1]
    gamma_csv = sys.argv[2]
    index_name = sys.argv[3]

    model_out = f"gamma_model_{index_name.replace(' ', '_').replace('(', '').replace(')', '')}.pt"
    meta_out = f"gamma_model_{index_name.replace(' ', '_').replace('(', '').replace(')', '')}_meta.json"

    train_model(price_csv, gamma_csv, index_name, model_out, meta_out)
