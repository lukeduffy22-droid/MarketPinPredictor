from options_gamma import fetch_snapshot
from gamma_scheduler import validate_snapshot
from train_gamma_model import save_snapshot
from predict_gamma_model import predict_from_dataframe

import logging
import os
from pathlib import Path


def setup_logging(log_dir="logs", log_file="predictions.log"):
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_path = Path(log_dir) / log_file
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_path)
        ]
    )


def run(index):
    setup_logging()
    logging.info(f"Starting snapshot pipeline for {index}")

    snapshot = fetch_snapshot(index)
    if validate_snapshot(snapshot):
        save_snapshot(snapshot, index)
        logging.info(f"✅ Snapshot saved for {index}")

        # Run prediction from in-memory DataFrame
        try:
            preds = predict_from_dataframe(snapshot, index)
            out_path = os.path.join("exports", f"predictions_{index}.csv")
            preds.to_csv(out_path, index=False)
            logging.info(f"✅ Predictions saved to {out_path}")
        except Exception as e:
            logging.exception(f"Prediction failed for {index}: {e}")
    else:
        logging.warning(f"❌ Snapshot rejected for {index}")


if __name__ == "__main__":
    run("SPX")
    