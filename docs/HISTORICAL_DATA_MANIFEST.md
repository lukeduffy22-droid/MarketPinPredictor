# Historical Training Data Manifest

This file describes the local training corpus without placing the corpus in the
Git repository.

## Dataset location

- Local path: `C:\Cprojectsgpu_app\MarketPinPredictor.worktrees\copilot\ensure-app-runs-correctly\historical_data`
- Repository policy: local-only; the `historical_data/` rule in `.gitignore` prevents accidental staging.
- Inventory source: recovery worktree on branch `friday-1/9`.
- Inventory date: 2026-09-18.

> **Warning:** This location is temporary and data-bearing. The historical
> files must not be deleted, moved, or assumed to be disposable until a
> verified migration and training-readiness check is complete. Do not remove
> the old recovery worktree or its dataset until that migration has been
> independently verified.

## Inventory

- Files: 10,227 CSV files and 2 pickle files.
- Size: approximately 114.64 GiB.
- Top-level groups: `aggregates/`, `combined_indices/`, `options/`, `s3_indices/`, and `training_datasets/`.
- File timestamps observed: 2026-09-16 00:52 through 2026-09-16 01:04 local time.

## Use

The corpus is retained for historical feature generation, chronological model
training, validation, and backtesting. Training jobs must document the dataset
location and use chronological train, validation, and test splits to avoid
future-data leakage.

## Versioning rule

Git tracks the training code, schemas, configuration templates, and this
manifest. It does not track the raw CSV corpus, generated features, model
weights, caches, or runtime databases. A future dataset refresh should update
this manifest with the inventory date, file count, size, date range, and a
separate checksum manifest before older data is archived.