from internal_cloud_api import s3api as s3
import pandas as pd
import polars as pl
import requests
import io
import asyncio
import os
import sys
from datetime import datetime, timedelta
import time

import boto3
import pyarrow.parquet as pq
from botocore.config import Config as BotoConfig
from boto3.s3.transfer import TransferConfig

from fdc_config import (
    _BUCKET,
    DATE_TO,
    WINDOW_DAYS,
    CALCULATIONS_FILES_PATH,
    FIXED_WEEKLY_REPORT,
    FIXED_WEEKLY_REPORT_KEY,
    PREVIOUS_WW_KEY
)
from fdc_compliance_fixed import (
    get_fixed_data,
    fdc_fixed_calculations,
    previous_work_week_fails,
    _release_memory_to_os,
    get_pod_memory_usage,
)


STORAGE_OPTIONS = {
    "aws_access_key_id": os.getenv("S3_ACCESS_KEY"),
    "aws_secret_access_key": os.getenv("S3_SECRET_KEY"),
    "aws_endpoint_url": "http://s3.internal.example.com:9090",
    "aws_region": "us-east-1", # or whatever placeholder your endpoint expects
    "aws_allow_http": "true", # because your endpoint is http not https
}

def make_s3_client(max_pool_connections=128):
    return boto3.client(
        service_name="s3",
        region_name="us-east-1",
        aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
        endpoint_url="http://s3.internal.example.com:9090",
        config=BotoConfig(
            retries={"max_attempts": 3},
            max_pool_connections=max_pool_connections,
        ),
    )

S3_CLIENT = make_s3_client(
    max_pool_connections=max(64, 4 * 4)
)

# ----------------------------
# Rolling Window Utilities
# ----------------------------

def get_window_dates(end_date_str=DATE_TO, days=WINDOW_DAYS):
    """Return the exact ``days``-day inclusive list of date strings ending at
    ``end_date_str`` (newest first). This is the custom X-day window."""
    try:
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    except (ValueError, TypeError):
        end_date = datetime.now()
    return [(end_date - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(days)]

# ----------------------------
# S3 parquet Helpers
# ----------------------------

def build_eqp_s3_paths(
    eqp_id: str,
    date_strs: list[str],
) -> list[str]:
    """
    Example output:
    s3://my-bucket/eqp_raw/2026-06-03/eqp_001.parquet
    s3://my-bucket/eqp_raw/2026-06-04/eqp_001.parquet
    ...
    """
    return [
        f"http://s3.internal.example.com:9090/{_BUCKET}/" + f"/{date_str}/{eqp_id}.parquet"
        for date_str in date_strs
    ]

async def _download_one_eqp_day_df(
    eqp_id: str,
    date_str: str,
    bucket: str,
    prefix: str,
    columns: list[str] | None = None,
    add_partition_date: bool = True,
) -> pl.DataFrame | None:
    key = f"{prefix}/{date_str}/{eqp_id}.parquet"
    bio = io.BytesIO()

    try:
        await asyncio.to_thread(
            S3_CLIENT.download_fileobj,
            bucket,
            key,
            bio,
        )
    except Exception as e:
        print(f"[WARN] Failed to download s3://{bucket}/{key}: {e}")
        return None

    bio.seek(0)
    df = pl.read_parquet(bio)

    if columns is not None:
        keep = [c for c in columns if c in df.columns]
        df = df.select(keep)

    if add_partition_date:
        df = df.with_columns(pl.lit(date_str).alias("partition_date"))

    return df


async def load_eqp_window_from_s3_boto3_async(
    eqp_id: str,
    date_strs: list[str],
    bucket: str,
    prefix: str,
    columns: list[str] | None = None,
    add_partition_date: bool = True,
    sort_by: str | None = None,
    max_concurrency: int = 8,
) -> pl.DataFrame:
    sem = asyncio.Semaphore(max_concurrency)

    async def _task(date_str: str):
        async with sem:
            return await _download_one_eqp_day_df(
                eqp_id=eqp_id,
                date_str=date_str,
                bucket=bucket,
                prefix=prefix,
                columns=columns,
                add_partition_date=add_partition_date,
            )

    dfs = await asyncio.gather(*[_task(d) for d in date_strs])
    dfs = [df for df in dfs if df is not None]

    if not dfs:
        return pl.DataFrame()

    out = pl.concat(dfs, how="vertical_relaxed")

    if sort_by is not None and sort_by in out.columns:
        out = out.sort(sort_by)

    return out

async def test_one_eqp():
    window_dates = get_window_dates(DATE_TO, 3)
    # window_dates = get_window_dates(end_date_str='2026-07-06', days=14)

    df_eqp = await load_eqp_window_from_s3_boto3_async(
        eqp_id="TOOL01",
        date_strs=window_dates,
        bucket=_BUCKET,
        prefix="eqp_raw_by_eqp", # should be "eqp_raw"
        columns=[
            "eqp_id",
            "create_time",
            "spec_id",
            "sensor_name",
            "sensor_value",
        ],
        add_partition_date=True,
        sort_by="create_time",
        max_concurrency=8,
    )

    print(df_eqp.shape)
    print(df_eqp.head())

    # asyncio.run(test_one_eqp())



def load_eqp_by_eqp_from_s3_sync(
    eqp_id: str,
    date_strs: list[str],
    bucket: str = _BUCKET,
    prefix: str = "eqp_raw_by_eqp",
    columns: list[str] | None = None,
    add_partition_date: bool = True,
    sort_by: str | None = None,
) -> pl.DataFrame:
    dfs = []

    for date_str in date_strs:
        key = f"{prefix}/{eqp_id}/{date_str}.parquet"
        bio = io.BytesIO()

        try:
            S3_CLIENT.download_fileobj(bucket, key, bio)
        except Exception as e:
            print(f"[WARN] Failed to download s3://{bucket}/{key}: {e}")
            continue

        bio.seek(0)
        df = pl.read_parquet(bio, columns=columns)

        if add_partition_date:
            df = df.with_columns(pl.lit(date_str).alias("partition_date"))

        dfs.append(df)

    if not dfs:
        return pl.DataFrame()

    out = pl.concat(dfs, how="vertical_relaxed")

    if sort_by is not None and sort_by in out.columns:
        out = out.sort(sort_by)

    return out


# ----------------------------
# Report generation (S3-by-eqp pipeline)
# ----------------------------

# Only these raw columns are consumed by fdc_fixed_calculations (verified against
# fdc_compliance_fixed.py). Loading just these shrinks df_raw and the row-level
# intermediate the calc builds, without changing results.
FIXED_CALC_RAW_COLS = [
    'spec_id', 'param_value', 'processid', 'act_time',
    'upper_spec', 'lower_spec', 'root_lot_id', 'wafer_id',
]

# Above this raw-row count, an eqp is processed in spec_id batches so a
# single .collect() can't spike past the pod's memory limit.
FIXED_CALC_CHUNK_ROWS = 3_000_000


def _chunk_specs_by_rows(df_raw: pl.DataFrame, row_budget: int) -> list[list]:
    """Greedily bin spec_id values so each batch's total raw rows <=
    row_budget. A single spec exceeding the budget becomes its own batch (can't be
    split further — the per-spec aggregations need all of its rows). Calculations are
    independent per spec_id, so batching + concatenating is exact."""
    counts = (
        df_raw.group_by('spec_id')
        .len()
        .sort('len', descending=True)
    )
    bins: list[list] = []  # each entry: [ [spec, ...], total_rows ]
    for spec, n in counts.iter_rows():
        for b in bins:
            if b[1] + n <= row_budget:
                b[0].append(spec)
                b[1] += n
                break
        else:
            bins.append([[spec], n])
    return [b[0] for b in bins]


def process_eqp_report(eqp_id: str, window_dates: list[str]) -> str:
    """Load one eqp's rolling window from S3 and write its calculation file.

    Mirrors fdc_compliance_fixed.py's process_file(), but sources raw data from
    the S3 eqp_raw_by_eqp/<eqp_id>/<date_str>.parquet layout instead of a local
    BDE-fetched raw file.
    """
    t0 = time.perf_counter()
    calc_path = CALCULATIONS_FILES_PATH / f'{eqp_id}.parquet.gzip'
    if calc_path.exists():
        return f'[{eqp_id}] - Already processed'

    t1 = time.perf_counter()
    # Load only the columns the calc uses (see FIXED_CALC_RAW_COLS) and skip the unused
    # partition column, to keep df_raw and the calc intermediate as small as possible.
    df_raw = load_eqp_by_eqp_from_s3_sync(
        eqp_id=eqp_id,
        date_strs=window_dates,
        columns=FIXED_CALC_RAW_COLS,
        add_partition_date=False,
        sort_by="act_time",
    )
    print(f"[{eqp_id}] downloaded from s3 | {time.perf_counter() - t1}s")
    if df_raw.is_empty():
        return f'[{eqp_id}] - No data in window'

    # Log the raw size so a memory spike can be tied to a specific eqp.
    print(f"[{eqp_id}] raw rows={df_raw.height}", flush=True)

    if df_raw.height > FIXED_CALC_CHUNK_ROWS:
        # Large eqp: process its specs in row-budgeted batches so a single .collect()
        # can't exceed the pod limit. Per-batch results are tiny (one row per spec),
        # so concatenating them reconstructs the same calc file as the whole-eqp path.
        spec_batches = _chunk_specs_by_rows(df_raw, FIXED_CALC_CHUNK_ROWS)
        print(f"[{eqp_id}] chunking {df_raw.height} rows into {len(spec_batches)} spec-batches", flush=True)
        parts = []
        for specs in spec_batches:
            sub = df_raw.filter(pl.col('spec_id').is_in(specs))
            r = fdc_fixed_calculations(sub.lazy())
            if r is not None:
                r = r.collect()
                if not r.is_empty():
                    parts.append(r)
            del sub
            get_pod_memory_usage()
            sys.stdout.flush()
        if not parts:
            del df_raw
            return f'[{eqp_id}] - No calculations'
        df_calc = pl.concat(parts, how='vertical_relaxed')
        del parts
    else:
        df_calc = fdc_fixed_calculations(df_raw.lazy())
        if df_calc is None:
            del df_raw
            return f'[{eqp_id}] - Calculation error'
        # Collect once (the plan was previously materialized twice: an emptiness
        # check and the write). Drop refs promptly so the periodic
        # _release_memory_to_os() in the caller can hand the pages back to the OS.
        df_calc = df_calc.collect()
    # Sample memory here (before any del) — the closest proxy to the intra-eqp peak,
    # unlike the between-eqp trough logged by the caller. Names the culprit if one spikes.
    print(f"[{eqp_id}] post-collect mem (rows={df_calc.height}):", flush=True)
    get_pod_memory_usage()
    sys.stdout.flush()
    if df_calc.is_empty():
        del df_calc, df_raw
        return f'[{eqp_id}] - No calculations'

    df_calc.write_parquet(
        str(calc_path),
        compression="gzip",
        row_group_size=500_000,
    )
    del df_calc, df_raw
    print(f"[{eqp_id}] Finished processing | {time.perf_counter() - t0}s")
    return f'[{eqp_id}] - Calculations written to {calc_path}'


def generate_fixed_weekly_report():
    """Drive the full report pipeline: per-eqp calculations -> combine -> write.

    Mirrors the tail of fdc_compliance_fixed.py's main() (the lazy_df combine,
    previous-work-week join, and fail-column derivation), reusing its functions
    directly instead of reimplementing them.
    """
    if FIXED_WEEKLY_REPORT.exists():
        print('fdc fixed report already exists')
        return

    t0 = time.perf_counter()
    CALCULATIONS_FILES_PATH.mkdir(parents=True, exist_ok=True)

    window_dates = get_window_dates(DATE_TO, WINDOW_DAYS)
    # window_dates = get_window_dates(end_date_str="2026-07-01", days=29)
    eqps = get_fixed_data()['eqp_id'].unique().to_list()

    print(f"Processing {len(eqps)} eqps over a {len(window_dates)}-day window...")

    total_time = 0
    process_t0 = time.perf_counter()
    for i, eqp_id in enumerate(eqps):
        print(process_eqp_report(eqp_id, window_dates))
        # Log pod RSS after every eqp and flush, so the last line before an OOM
        # kill names the culprit eqp.
        get_pod_memory_usage()
        sys.stdout.flush()
        if i % 10 == 0:
            # Return freed Arrow/glibc pages to the OS every 10 eqps to counter
            # the cross-eqp RSS creep (mirrors the fetch pipeline's reclaim cadence).
            _release_memory_to_os()
            elapsed = time.perf_counter() - process_t0
            total_time += elapsed
            print(f"[[Progress]] {i}/{len(eqps)} | time elapsed: {elapsed}s | total time: {total_time}s")
            process_t0 = time.perf_counter()

    # Trim once more before the combine, and log RSS so the crash location is
    # unambiguous (loop vs combine) in the pod logs.
    _release_memory_to_os()
    print("[mem] before combine/sink:", flush=True)
    get_pod_memory_usage()
    sys.stdout.flush()

    # Keep the combine lazy and stream to disk. Calling .collect() here would
    # materialize every eqp's calc file into a single in-memory DataFrame at once
    # (the OOM peak on the pod). The pww_fail / fail derivations below are all
    # row-wise and streaming-compatible, so sink_parquet can scan -> transform ->
    # write in bounded batches instead.
    lazy_df = pl.scan_parquet(str(CALCULATIONS_FILES_PATH / "*.parquet.gzip"))

    # The previous work-week report may not exist yet (e.g. the first run, or the
    # week before was never produced). Guard the S3 read so a missing file degrades
    # to pww_fail=0 (nothing confirmed as a repeat fail) instead of crashing.
    if s3.chk_file_exist(_BUCKET, PREVIOUS_WW_KEY):
        pww_dict = previous_work_week_fails()
    else:
        print(
            f"[WARN] previous work-week report not found at "
            f"s3://{_BUCKET}/{PREVIOUS_WW_KEY}; treating pww_fail=0",
            flush=True,
        )
        pww_dict = {}

    # With no previous-week data, set pww_fail=0 directly (avoids relying on
    # .replace() accepting an empty mapping across polars versions).
    pww_fail_expr = (
        pl.col("spec_id").replace(pww_dict, default=0).alias("pww_fail")
        if pww_dict
        else pl.lit(0).alias("pww_fail")
    )
    lazy_out = (
        lazy_df
        .with_columns(pww_fail_expr)
        .with_columns(
            pl.col("pww_fail").fill_null(0)
        )
        .with_columns(
            pl.when((pl.col("cww_fail") == 1) & (pl.col("pww_fail") == 1))
            .then(1)
            .when((pl.col("cww_fail") == 1) & (pl.col("pww_fail") == 0))
            .then(0)
            .otherwise(pl.col("cww_fail"))
            .alias("fail")
        )
    )

    lazy_out.sink_parquet(FIXED_WEEKLY_REPORT)
    del pww_dict

    # Persist to S3: the pod's local dir is wiped after the run. upload_file streams
    # the finished parquet from disk (no full-file load into memory), so the OOM fix
    # is preserved. autoDeleteFile frees the local temp copy after a successful push.
    if s3.chk_file_exist(_BUCKET, FIXED_WEEKLY_REPORT_KEY):
        s3.delete_file(_BUCKET, FIXED_WEEKLY_REPORT_KEY)
    s3.upload_file(
        local_file=str(FIXED_WEEKLY_REPORT),
        bucket=_BUCKET,
        key=FIXED_WEEKLY_REPORT_KEY,
        autoDeleteFile=True,
    )
    print("[mem] after combine/upload:", flush=True)
    get_pod_memory_usage()
    print(f"[[Total]] Overall Report | {time.perf_counter() - t0}s")
    print(f"Report uploaded to s3://{_BUCKET}/{FIXED_WEEKLY_REPORT_KEY}")


async def main():
    generate_fixed_weekly_report()


if __name__ == "__main__":
    window_dates = get_window_dates(DATE_TO, WINDOW_DAYS)
    print(window_dates)
    load_eqp_by_eqp_from_s3_sync(eqp_id='TOOL01', date_strs=window_dates)
