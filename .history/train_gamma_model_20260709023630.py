import argparse
import glob
import json
import os
from typing import List, Tuple

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, Dataset

# ============================================================
# CONFIG
# ============================================================

TARGET_COLUMN = "distance_to_pin"
DEFAULT_BATCH_SIZE = 1024
DEFAULT_EPOCHS = 60
DEFAULT_LR = 1e-3
DEFAULT_VALIDATION_SPLIT = 0.2
DEFAULT_PATIENCE = 8
RANDOM_SEED = 42

torch.manual_seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

# ============================================================
# DEVICE SETUP
# ============================================================

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Using device: {device}")
if torch.cuda.is_available():
    print(f"CUDA device: {torch.cuda.get_device_name(0)}")

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


def _discover_csv_files(path: str) -> List[str]:
    """Discover CSV files from a file path, directory, or glob pattern."""
    if os.path.isfile(path):
        return [path]

    if os.path.isdir(path):
        files = glob.glob(os.path.join(path, "**", "*.csv"), recursive=True)
        return sorted(files)

    # Treat as glob pattern
    files = glob.glob(path, recursive=True)
    files = [f for f in files if f.lower().endswith(".csv") and os.path.isfile(f)]
    return sorted(files)


def _load_dataset(path: str, max_rows: int = 0) -> pd.DataFrame:
    """Load all matching CSV files and concatenate into one DataFrame."""
    files = _discover_csv_files(path)
    if not files:
        raise ValueError(f"No CSV files found for: {path}")

    print(f"Discovered {len(files)} CSV file(s)")

    frames = []
    total_rows = 0
    for f in files:
        df = pd.read_csv(f)
        rows = len(df)
        total_rows += rows
        frames.append(df)
        print(f"  Loaded {f}: {rows} rows")

        if max_rows > 0 and total_rows >= max_rows:
            break

    out = pd.concat(frames, ignore_index=True)
    if max_rows > 0 and len(out) > max_rows:
        out = out.tail(max_rows).reset_index(drop=True)

    print(f"Total loaded rows: {len(out)}")
    return out


def _prepare_dataset(df: pd.DataFrame, index_name: str) -> Tuple[pd.DataFrame, List[str]]:
    """Validate and transform raw data to train-ready rows and selected feature columns."""
    required_cols = {"symbol", "spot", "gamma_pin"}
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Required column(s) missing: {missing}")

    # Strict index filtering
    df = df[df["symbol"].astype(str).str.strip() == index_name].copy()
    if df.empty:
        raise ValueError(f"No rows found for index: {index_name}")

    # Keep validated rows when available
    if "is_valid" in df.columns:
        before = len(df)
        df = df[df["is_valid"] == True].copy()  # noqa: E712
        print(f"Kept validated rows: {len(df)}/{before}")

    if "timestamp_utc" in df.columns:
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], errors="coerce", utc=True)
        df = df.dropna(subset=["timestamp_utc"]).sort_values("timestamp_utc").reset_index(drop=True)
        print(f"Chronologically sorted rows: {len(df)}")

    # Target construction
    df = df.dropna(subset=["spot", "gamma_pin"]).copy()
    df[TARGET_COLUMN] = df["spot"] - df["gamma_pin"]
    df = df.dropna(subset=[TARGET_COLUMN]).copy()

    feature_cols = select_feature_columns(df)
    if not feature_cols:
        raise ValueError("No usable numeric feature columns found after preprocessing")

    # Keep only rows with complete feature vectors
    df = df.dropna(subset=feature_cols + [TARGET_COLUMN]).reset_index(drop=True)
    if df.empty:
        raise ValueError("No rows remaining after dropping missing feature values")

    return df, feature_cols


def _chronological_split(df: pd.DataFrame, validation_split: float) -> Tuple[np.ndarray, np.ndarray]:
    """Create chronological train/validation indices."""
    n = len(df)
    split = int(n * (1 - validation_split))
    split = max(1, min(split, n - 1))

    idx = np.arange(n)
    return idx[:split], idx[split:]

# ============================================================
# MAIN TRAINING FUNCTION
# ============================================================

def train_model(
    data_path: str,
    index_name: str,
    model_out: str,
    meta_out: str,
    batch_size: int,
    epochs: int,
    learning_rate: float,
    validation_split: float,
    early_stopping_patience: int,
    max_rows: int,
):
    print(f"Loading dataset from: {data_path}")
    raw_df = _load_dataset(data_path, max_rows=max_rows)
    df, feature_cols = _prepare_dataset(raw_df, index_name=index_name)

    train_idx, val_idx = _chronological_split(df, validation_split=validation_split)

    X = df[feature_cols].values.astype(np.float32)
    y = df[TARGET_COLUMN].values.astype(np.float32)

    X_train, y_train = X[train_idx], y[train_idx]
    X_val, y_val = X[val_idx], y[val_idx]

    # Fit scaler on training data only to prevent leakage.
    feature_mean = X_train.mean(axis=0)
    feature_std = X_train.std(axis=0)
    feature_std[feature_std < 1e-8] = 1.0

    X_train = (X_train - feature_mean) / feature_std
    X_val = (X_val - feature_mean) / feature_std

    train_ds = TabularDataset(X_train, y_train)
    val_ds = TabularDataset(X_val, y_val)

    num_workers = min(4, os.cpu_count() or 1)
    pin_memory = device.type == "cuda"

    train_loader = DataLoader(
        train_ds,
        batch_size=batch_size,
        shuffle=True,
        num_workers=num_workers,
        pin_memory=pin_memory,
        persistent_workers=num_workers > 0,
    )
    val_loader = DataLoader(
        val_ds,
        batch_size=batch_size,
        shuffle=False,
        num_workers=num_workers,
        pin_memory=pin_memory,
        persistent_workers=num_workers > 0,
    )

    model = MLP(input_dim=X.shape[1]).to(device)
    optimizer = torch.optim.AdamW(model.parameters(), lr=learning_rate, weight_decay=1e-4)
    loss_fn = nn.MSELoss()

    use_amp = device.type == "cuda"
    scaler = torch.cuda.amp.GradScaler(enabled=use_amp)

    best_val_loss = float("inf")
    best_state = None
    epochs_without_improvement = 0

    for epoch in range(1, epochs + 1):
        model.train()
        train_losses = []

        for xb, yb in train_loader:
            xb = xb.to(device, non_blocking=pin_memory)
            yb = yb.to(device, non_blocking=pin_memory)

            optimizer.zero_grad(set_to_none=True)
            with torch.cuda.amp.autocast(enabled=use_amp):
                pred = model(xb)
                loss = loss_fn(pred, yb)

            scaler.scale(loss).backward()
            scaler.step(optimizer)
            scaler.update()
            train_losses.append(loss.item())

        model.eval()
        val_losses = []
        with torch.no_grad():
            for xb, yb in val_loader:
                xb = xb.to(device, non_blocking=pin_memory)
                yb = yb.to(device, non_blocking=pin_memory)

                with torch.cuda.amp.autocast(enabled=use_amp):
                    pred = model(xb)
                    loss = loss_fn(pred, yb)
                val_losses.append(loss.item())

        mean_train = float(np.mean(train_losses)) if train_losses else float("nan")
        mean_val = float(np.mean(val_losses)) if val_losses else float("nan")

        print(f"Epoch {epoch:03d} | Train {mean_train:.6f} | Val {mean_val:.6f}")

        if mean_val < best_val_loss:
            best_val_loss = mean_val
            best_state = {k: v.detach().cpu() for k, v in model.state_dict().items()}
            epochs_without_improvement = 0
        else:
            epochs_without_improvement += 1

        if epochs_without_improvement >= early_stopping_patience:
            print(f"Early stopping triggered at epoch {epoch}")
            break

    if best_state is None:
        best_state = model.state_dict()

    torch.save(best_state, model_out)

    meta = {
        "feature_columns": feature_cols,
        "target": TARGET_COLUMN,
        "index_name": index_name,
        "rows_total": int(len(df)),
        "rows_train": int(len(train_idx)),
        "rows_validation": int(len(val_idx)),
        "best_validation_mse": float(best_val_loss),
        "device": str(device),
        "scaling": {
            "mean": feature_mean.tolist(),
            "std": feature_std.tolist(),
        },
    }

    with open(meta_out, "w") as f:
        json.dump(meta, f, indent=2)

    print(f"Training complete. Saved model: {model_out}")
    print(f"Saved metadata: {meta_out}")

# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train institutional-grade gamma model")
    parser.add_argument("data_path", help="CSV file, directory of CSVs, or glob pattern")
    parser.add_argument("index_name", help="Index symbol (SPX, NDX, DJI, RUT)")
    parser.add_argument("--model-out", default=None, help="Model output .pt path")
    parser.add_argument("--meta-out", default=None, help="Metadata output .json path")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--epochs", type=int, default=DEFAULT_EPOCHS)
    parser.add_argument("--lr", type=float, default=DEFAULT_LR)
    parser.add_argument("--validation-split", type=float, default=DEFAULT_VALIDATION_SPLIT)
    parser.add_argument("--patience", type=int, default=DEFAULT_PATIENCE)
    parser.add_argument("--max-rows", type=int, default=0, help="Optional cap for very large corpora")
    args = parser.parse_args()

    index_name = args.index_name.strip().upper()
    model_out = args.model_out or f"gamma_model_{index_name}.pt"
    meta_out = args.meta_out or f"gamma_model_{index_name}_meta.json"

    train_model(
        data_path=args.data_path,
        index_name=index_name,
        model_out=model_out,
        meta_out=meta_out,
        batch_size=args.batch_size,
        epochs=args.epochs,
        learning_rate=args.lr,
        validation_split=args.validation_split,
        early_stopping_patience=args.patience,
        max_rows=args.max_rows,
    )
