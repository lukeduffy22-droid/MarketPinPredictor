import os
import json
import pandas as pd
from datetime import datetime, timezone
from collections import Counter, defaultdict
import boto3

# S3 config from env
S3_BUCKET = os.environ.get("QC_S3_BUCKET")
S3_KEY = os.environ.get("QC_S3_KEY", "qc_status.json")
S3_REGION = os.environ.get("QC_S3_REGION")
S3_ENABLED = bool(S3_BUCKET)


def write_qc_status(
    qc_parquet_path,
    status_path,
    sample_count=3,
    push_s3=True
):
    now_utc = datetime.now(timezone.utc).isoformat()
    df = pd.read_parquet(qc_parquet_path)
    total = len(df)
    valid = int(df["is_valid_qc"].sum())
    invalid = total - valid
    reason_counts = Counter(df["invalid_reason"].dropna())
    top_reasons = reason_counts.most_common(5)
    sample_files = defaultdict(list)
    for reason, _ in top_reasons:
        files = df[df["invalid_reason"] == reason]["source_file"].head(sample_count).tolist()
        sample_files[reason] = files
    status = {
        "last_run_utc": now_utc,
        "total_snapshots": total,
        "valid_count": valid,
        "invalid_count": invalid,
        "invalid_reason_counts": dict(top_reasons),
        "sample_invalid_reasons": dict(sample_files),
        "qc_parquet_path": qc_parquet_path,
        "pushed_to_s3": False,
        "s3_bucket": S3_BUCKET,
        "s3_key": S3_KEY,
    }
    with open(status_path, "w") as f:
        json.dump(status, f, indent=2)
    if push_s3 and S3_ENABLED:
        try:
            s3 = boto3.client("s3", region_name=S3_REGION) if S3_REGION else boto3.client("s3")
            s3.upload_file(status_path, S3_BUCKET, S3_KEY)
            status["pushed_to_s3"] = True
        except Exception as e:
            status["s3_error"] = str(e)
    return status

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--qc-parquet", required=True)
    p.add_argument("--status-path", required=True)
    p.add_argument("--no-s3", action="store_true")
    args = p.parse_args()
    write_qc_status(args.qc_parquet, args.status_path, push_s3=not args.no_s3)
