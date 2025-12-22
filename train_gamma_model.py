import sys
import json
import pandas as pd
import numpy as np
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
        "index_name",
        "symbol",
        "timestamp_utc",
        "date",
        "prediction_date",
        "gamma_pin",
        "max_pain"
    }
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    return [c for c in numeric_cols if c not in exclude]

# ============================================================
# MAIN TRAINING FUNCTION
# ============================================================

def train_model(full_csv, index_name, model_out, meta_out):

    print(f"Loading unified dataset: {full_csv}")
    df = pd.read_csv(full_csv)

    # Strict index filtering
    if "symbol" not in df.columns:
        raise ValueError("symbol column missing — cannot filter index safely.")

    df = df[df["symbol"].str.strip() == index_name]
    if df.empty:
        raise ValueError(f"No rows found for index: {index_name}")

    # Sort by timestamp for proper temporal ordering
    if "timestamp_utc" in df.columns:
        df = df.sort_values("timestamp_utc").reset_index(drop=True)
        print(f"Sorted {len(df)} rows by timestamp_utc")

    # Check for NaN gamma_pin before computing target
    if df["gamma_pin"].isna().any():
        nan_count = df["gamma_pin"].isna().sum()
        print(f"Warning: {nan_count} rows have NaN gamma_pin — dropping them")
        df = df.dropna(subset=["gamma_pin"])

    if df.empty:
        raise ValueError(f"No valid rows remaining after dropping NaN gamma_pin")

    # Compute target
    df["distance_to_pin"] = df["spot"] - df["gamma_pin"]

    # Drop rows missing target (safety net)
    df = df.dropna(subset=["distance_to_pin"])

    # Select features
    feature_cols = select_feature_columns(df)
    X = df[feature_cols].values.astype(np.float32)
    y = df[TARGET_COLUMN].values.astype(np.float32)

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
    # NOTE: If you add feature scaling (StandardScaler, MinMaxScaler),
    # save the scaler parameters here and load them during inference.
    meta = {
        "feature_columns": feature_cols,
        "target": TARGET_COLUMN,
        "index_name": index_name,
        "scaling": None  # Future: {"mean": [...], "std": [...]} for StandardScaler
    }
    with open(meta_out, "w") as f:
        json.dump(meta, f, indent=2)

    print("Training complete.")

# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python train_gamma_model.py <full_dataset_csv> <index_name>")
        print("")
        print("Examples:")
        print("  python train_gamma_model.py all_indices_2025-12-22.csv SPX")
        print("  python train_gamma_model.py all_indices_2025-12-22.csv NDX")
        print("  python train_gamma_model.py all_indices_2025-12-22.csv RUT")
        sys.exit(1)

    full_csv = sys.argv[1]
    index_name = sys.argv[2]

    model_out = f"gamma_model_{index_name}.pt"
    meta_out = f"gamma_model_{index_name}_meta.json"

    train_model(full_csv, index_name, model_out, meta_out)
