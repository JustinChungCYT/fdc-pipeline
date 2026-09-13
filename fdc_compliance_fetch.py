import asyncio
import gc
import os
import time
import tempfile
import traceback
import logging
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta

import boto3
import pyarrow.parquet as pq
from botocore.config import Config as BotoConfig
from boto3.s3.transfer import TransferConfig

from fdc_config import (
    SENSOR_GRADES,
    RAW_S3_PREFIX,
    WINDOW_DAYS,
    DATE_TO,
    PREFILL_CACHE,
    CURRENT_WEEK_DAYS,
    _BUCKET,
    LOG_DESTINATION,

    DB_CONCURRENCY,
    UPLOAD_CONCURRENCY,
    QUEUE_MAXSIZE,
    SPOOL_MAX_SIZE,
    LOG_PATH
)
from fdc_compliance_fixed import (
    get_fixed_data,
    fetch_tool_sensor_readings,
)


_DONE_MARKER = '_DONE'
LOG_PATH = Path(LOG_PATH)

logger = logging.getLogger("eqp_s3_pipeline")
logger.setLevel(logging.INFO)
logger.propagate = False

if not logger.handlers:
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    if LOG_DESTINATION in ("console", "both"):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    if LOG_DESTINATION in ("file", "both"):
        file_handler = logging.FileHandler(LOG_PATH, mode="a")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)


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
    max_pool_connections=max(64, UPLOAD_CONCURRENCY * 4)
)

TRANSFER_CONFIG = TransferConfig(
    use_threads=True,
    max_concurrency=4, # multipart concurrency per large file
)


# ----------------------------
# Helpers
# ----------------------------

async def s3_key_exists(s3_key: str) -> bool:
    try:
        await asyncio.to_thread(
            S3_CLIENT.head_object,
            Bucket=_BUCKET,
            Key=s3_key,
        )
        return True
    except Exception:
        return False


async def fetch_and_serialize_to_parquet(eqp_id, indices, date_str):
    """
    Fetch one eqp/day from DB and serialize to a spooled parquet file.
    Returns:
    None if no rows or already skipped upstream
    dict with file handle + metadata otherwise
    """
    table = None
    spooled_file = None

    query_elapsed = 0.0
    parquet_elapsed = 0.0

    try:
        # --------------------------
        # 1) DB query timing
        # --------------------------
        query_t0 = time.perf_counter()

        table = await fetch_tool_sensor_readings(
            eqp_id=eqp_id,
            spec_ids=indices,
            dateFrom=date_str,
            dateTo=date_str,
            sensor_grades=SENSOR_GRADES,
        )

        query_elapsed = time.perf_counter() - query_t0

        if table is None or table.num_rows == 0:
            return {
                "empty": True,
                "db_query_s": query_elapsed,
                "parquet_s": 0.0,
                "producer_total_s": query_elapsed,
            }

        rows = table.num_rows

        # --------------------------
        # 2) Parquet serialization timing
        # --------------------------
        spooled_file = tempfile.SpooledTemporaryFile(max_size=SPOOL_MAX_SIZE)

        parquet_t0 = time.perf_counter()

        pq.write_table(
            table,
            spooled_file,
            compression="zstd",
            compression_level=3,
        )

        parquet_elapsed = time.perf_counter() - parquet_t0

        # Compressed size of the finished parquet; the write left the handle at EOF.
        byte_size = spooled_file.tell()

        spooled_file.seek(0)

        return {
            "empty": False,
            "eqp_id": eqp_id,
            "date_str": date_str,
            "rows": rows,
            "byte_size": byte_size,
            "fileobj": spooled_file,
            "s3_key": f"{RAW_S3_PREFIX}/{eqp_id}/{date_str}.parquet",

            # timings
            "db_query_s": query_elapsed,
            "parquet_s": parquet_elapsed,
            "producer_total_s": query_elapsed + parquet_elapsed,
        }

    except Exception as e:
        if spooled_file is not None:
            spooled_file.close()
        raise e

    finally:
        if table is not None:
            del table
        gc.collect()


async def upload_parquet_item(item):
    fileobj = item["fileobj"]
    s3_key = item["s3_key"]

    try:
        await asyncio.to_thread(
            S3_CLIENT.upload_fileobj,
            fileobj,
            _BUCKET,
            s3_key,
            ExtraArgs={
                "Metadata": {
                    "row_count": str(item["rows"]),
                    "byte_size": str(item["byte_size"]),
                    # EOD bit: was the calendar day already over when BDE was queried?
                    # "0" marks a same-day partial fetch - a valid parquet holding
                    # only part of the day.
                    "day_closed": "1" if item["date_str"] < DATE_TO else "0",
                }
            },
            Config=TRANSFER_CONFIG,
        )
    finally:
        fileobj.close()

    # Confirm the stored object is the size we uploaded. On a mismatch delete it: an
    # absent key makes the next run re-fetch, whereas a short object would be read as
    # valid data by the report stage. Raising lets upload_worker's existing handler
    # record it as a failure.
    head = await asyncio.to_thread(
        S3_CLIENT.head_object,
        Bucket=_BUCKET,
        Key=s3_key,
    )
    if head["ContentLength"] != item["byte_size"]:
        await asyncio.to_thread(
            S3_CLIENT.delete_object,
            Bucket=_BUCKET,
            Key=s3_key,
        )
        raise ValueError(
            f"size mismatch for {s3_key}: uploaded {item['byte_size']} bytes, "
            f"stored {head['ContentLength']}; deleted for re-fetch"
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


def get_dates_to_fetch(window_dates, cached, prefill):
    """Dates within the window that still need fetching.

    Warm runs only miss the newest day(s), so prefill is irrelevant. On a cold
    start (older window days missing), ``prefill=False`` restricts the fetch to the
    most recent ``CURRENT_WEEK_DAYS`` (current work week); the rest of the window
    fills in over subsequent weekly runs.
    """
    # missing = [d for d in window_dates if d not in cached]
    missing = [d for d in window_dates]
    if not prefill:
        cutoff = datetime.strptime(DATE_TO, '%Y-%m-%d') - timedelta(days=CURRENT_WEEK_DAYS - 1)
        print(f"cutoff: {cutoff}")
        missing = [d for d in missing if datetime.strptime(d, '%Y-%m-%d') >= cutoff]
        print(f"missing: {missing}")
    return sorted(missing)

def purge_expired_eqp_raw(window_end=DATE_TO, window_days=WINDOW_DAYS):
    """Delete S3 objects under RAW_S3_PREFIX whose date_str has rolled out of
    the WINDOW_DAYS window (the rolling right-shift), for the eqp_id/date_str.parquet
    layout. Unlike the old date-folder layout, there is no per-date sentinel to key
    off of, so this lists every object under the prefix and parses the date_str out
    of its key (``{RAW_S3_PREFIX}/{eqp_id}/{date_str}.parquet``).
    """
    cutoff = datetime.strptime(window_end, '%Y-%m-%d') - timedelta(days=window_days - 1)
    logger.info(f"Purging {RAW_S3_PREFIX}/ objects older than {cutoff.date()}...")

    to_delete = []
    paginator = S3_CLIENT.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=_BUCKET, Prefix=f"{RAW_S3_PREFIX}/"):
        for obj in page.get('Contents', []):
            key = obj['Key']
            date_part = key.rsplit('/', 1)[-1]
            if not date_part.endswith('.parquet'):
                continue
            date_str = date_part[:-len('.parquet')]
            try:
                obj_date = datetime.strptime(date_str, '%Y-%m-%d')
            except ValueError:
                continue
            if obj_date < cutoff:
                to_delete.append({'Key': key})

    if not to_delete:
        logger.info("No expired eqp_raw objects to purge.")
        return 0

    # Delete one key at a time with delete_object rather than the batched
    # delete_objects. The S3 gateway rejects the multi-object DeleteObjects
    # request with 400 Bad Request (its body/checksum handling differs from AWS);
    # a single-object DELETE carries no request body, so it is accepted.
    deleted = 0
    for obj in to_delete:
        key = obj['Key']
        try:
            S3_CLIENT.delete_object(Bucket=_BUCKET, Key=key)
            deleted += 1
        except Exception as e:
            logger.error(f"Failed to delete {key}: {e}")

    logger.info(f"Purged {deleted}/{len(to_delete)} expired eqp_raw objects.")
    return deleted

# ----------------------------
# Main pipeline
# ----------------------------

async def upload_eqp_raw_direct_to_s3(
    units,
    date_strs,
    db_concurrency=DB_CONCURRENCY,
    upload_concurrency=UPLOAD_CONCURRENCY,
):
    """
    Two-stage bounded pipeline:
    1. DB fetchers query DB and serialize parquet into spooled temp files.
    2. Upload workers upload spooled parquet files to S3.
    """

    all_tasks = [
        (eqp_id, indices, date_str)
        for date_str in date_strs
        for eqp_id, indices in units
    ]

    total_tasks = len(all_tasks)

    logger.info("Starting direct DB -> Parquet -> S3 pipeline")
    logger.info(f"Total eqp/day tasks: {total_tasks}")
    logger.info(f"DB concurrency: {db_concurrency}")
    logger.info(f"Upload concurrency: {upload_concurrency}")
    logger.info(f"Queue max size: {QUEUE_MAXSIZE}")

    upload_queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)

    counters = {
        "checked": 0,
        "skipped_existing": 0,
        "empty": 0,
        "fetched": 0,
        "uploaded": 0,
        "failed_fetch": 0,
        "failed_upload": 0,

        # producer stage timings
        "db_query_seconds_total": 0.0,
        "db_query_count": 0,
        "parquet_seconds_total": 0.0,
        "parquet_count": 0,

        # e2e producer stage timing(query + parquet)
        "db_fetch_seconds_total": 0.0,
        "db_fetch_count": 0.0,

        # upload stage timing
        "upload_seconds_total": 0.0,
        "upload_count": 0.0,

        # backpressure metrics
        "queue_put_wait_seconds_total": 0.0,
        "queue_put_wait_count": 0,
        "queue_high_watermark": 0,
    }

    failures = []
    counter_lock = asyncio.Lock()
    done_event = asyncio.Event()

    async def update_counter(name, amount=1):
        async with counter_lock:
            counters[name] += amount

    async def progress_reporter():
        start = time.time()

        while not done_event.is_set():
            await asyncio.sleep(60)  # report every 1 min

            async with counter_lock:
                snapshot = dict(counters)

            elapsed_min = (time.time() - start) / 60

            completed = (
                snapshot["skipped_existing"]
                + snapshot["empty"]
                + snapshot["uploaded"]
                + snapshot["failed_fetch"]
                + snapshot["failed_upload"]
            )

            avg_db_query = (
                counters["db_query_seconds_total"] / counters["db_query_count"]
                if counters["db_query_count"] else 0.0
            )
            avg_parquet = (
                counters["parquet_seconds_total"] / counters["parquet_count"]
                if counters["parquet_count"] else 0.0
            )
            avg_db_total = (
                counters["db_fetch_seconds_total"] / counters["db_fetch_count"]
                if counters["db_fetch_count"] else 0.0
            )
            avg_upload = (
                counters["upload_seconds_total"] / counters["upload_count"]
                if counters["upload_count"] else 0.0
            )
            avg_queue_wait = (
                counters["queue_put_wait_seconds_total"] / counters["queue_put_wait_count"]
                if counters["queue_put_wait_count"] else 0.0
            )
            files_per_min = counters["uploaded"] / elapsed_min if elapsed_min > 0 else 0.0

            logger.info(
                f"Progress | completed={completed}/{total_tasks}, "
                f"checked={snapshot['checked']}, "
                f"skipped_existing={snapshot['skipped_existing']}, "
                f"empty={snapshot['empty']}, "
                f"fetched={snapshot['fetched']}, "
                f"uploaded={snapshot['uploaded']}, "
                f"failed_fetch={snapshot['failed_fetch']}, "
                f"failed_upload={snapshot['failed_upload']}, "
                f"queue_size={upload_queue.qsize()}, "
                f"queue_hwm={counters['queue_high_watermark']}, "
                f"avg_db_query_s={avg_db_query:.2f}, "
                f"avg_parquet_s={avg_parquet:.2f}, "
                f"avg_db_total_s={avg_db_total:.2f}, "
                f"avg_upload_s={avg_upload:.2f}, "
                f"avg_queue_wait_s={avg_queue_wait:.2f}, "
                f"files_per_min={files_per_min:.1f}, "
                f"elapsed={elapsed_min:.1f} min"
            )

    async def db_worker(worker_id, task_queue):
        while True:
            task = await task_queue.get()

            if task is None:
                task_queue.task_done()
                return

            eqp_id, indices, date_str = task
            s3_key = f"{RAW_S3_PREFIX}/{eqp_id}/{date_str}.parquet"

            try:
                await update_counter("checked")

                if await s3_key_exists(s3_key):
                    await update_counter("skipped_existing")
                    continue

                item = await fetch_and_serialize_to_parquet(
                    eqp_id=eqp_id,
                    indices=indices,
                    date_str=date_str,
                )

                # timing measurement
                await update_counter("db_query_seconds_total", item["db_query_s"])
                await update_counter("db_query_count", 1)

                await update_counter("parquet_seconds_total", item["parquet_s"])
                await update_counter("parquet_count", 1)

                await update_counter("db_fetch_seconds_total", item["producer_total_s"])
                await update_counter("db_fetch_count", 1)

                if item['empty']:
                    await update_counter("empty")
                    continue

                await update_counter("fetched")

                # Backpressure happens here if uploaders are slower.
                put_t0 = time.perf_counter()
                await upload_queue.put(item)
                put_elapsed = time.perf_counter() - put_t0

                await update_counter("queue_put_wait_seconds_total", put_elapsed)
                await update_counter("queue_put_wait_count", 1)

                async with counter_lock:
                    counters["queue_high_watermark"] = max(
                        counters["queue_high_watermark"],
                        upload_queue.qsize()
                    )

            except Exception as e:
                await update_counter("failed_fetch")
                tb = traceback.format_exc()
                failures.append({
                    "stage": "fetch",
                    "eqp_id": eqp_id,
                    "date_str": date_str,
                    "s3_key": s3_key,
                    "error": str(e),
                    "traceback": tb,
                })
                logger.error(f"Fetch failed | {eqp_id} {date_str} | {e}")

            finally:
                task_queue.task_done()

    async def upload_worker(worker_id):
        while True:
            item = await upload_queue.get()

            if item is None:
                upload_queue.task_done()
                return

            try:
                # timing measurement
                upload_t0 = time.perf_counter()
                await upload_parquet_item(item)
                upload_elapsed = time.perf_counter() - upload_t0

                # timing measurement
                await update_counter("upload_seconds_total", upload_elapsed)
                await update_counter("upload_count", 1)
                await update_counter("uploaded")

                print(
                    f"Uploaded | {item['date_str']} {item['eqp_id']} | "
                    f"{item['rows']:,} rows | s3://{_BUCKET}/{item['s3_key']}"
                )

            except Exception as e:
                await update_counter("failed_upload")
                tb = traceback.format_exc()
                failures.append({
                    "stage": "upload",
                    "eqp_id": item["eqp_id"],
                    "date_str": item["date_str"],
                    "s3_key": item["s3_key"],
                    "rows": item["rows"],
                    "error": str(e),
                    "traceback": tb,
                })
                logger.exception(
                    f"Upload failed | {item['date_str']} {item['eqp_id']} | "
                    f"s3://{_BUCKET}/{item['s3_key']}"
                )

                try:
                    item["fileobj"].close()
                except Exception:
                    pass

            finally:
                upload_queue.task_done()

    task_queue = asyncio.Queue()

    for task in all_tasks:
        await task_queue.put(task)

    for _ in range(db_concurrency):
        await task_queue.put(None)

    reporter = asyncio.create_task(progress_reporter())

    db_workers = [
        asyncio.create_task(db_worker(i, task_queue))
        for i in range(db_concurrency)
    ]

    upload_workers = [
        asyncio.create_task(upload_worker(i))
        for i in range(upload_concurrency)
    ]

    t0 = time.time()

    await task_queue.join()

    # Tell upload workers to stop after upload queue drains.
    for _ in range(upload_concurrency):
        await upload_queue.put(None)

    await upload_queue.join()

    await asyncio.gather(*db_workers)
    await asyncio.gather(*upload_workers)

    done_event.set()
    reporter.cancel()

    total_elapsed = (time.time() - t0) / 60

    logger.info("Pipeline complete")
    logger.info(f"Elapsed: {total_elapsed:.2f} min")
    logger.info(f"Final counters: {counters}")
    logger.info(f"Failures: {len(failures)}")
    logger.info(f"Log path: {LOG_PATH.resolve()}")

    return counters, failures

async def main():
    purge_expired_eqp_raw(DATE_TO, WINDOW_DAYS)
    window = get_window_dates(DATE_TO, WINDOW_DAYS)
    # window = get_window_dates(DATE_TO, 7)
    to_fetch = get_dates_to_fetch(window, None, PREFILL_CACHE)
    if to_fetch:
        print(f"Window: {window[-1]} .. {window[0]} ({WINDOW_DAYS}d) | "
          f"prefill={PREFILL_CACHE} | days to fetch: {len(to_fetch)}")

        # eqp -> spec indices, computed once.
        df_fixed = get_fixed_data()
        eqp_info = (
            df_fixed.select(['eqp_id', 'spec_id'])
            .group_by('eqp_id')
            .agg(pl.col('spec_id').alias('indices'))
        )
        units = list(zip(eqp_info['eqp_id'].to_list(), eqp_info['indices'].to_list()))
        await upload_eqp_raw_direct_to_s3(units, to_fetch)
    else:
        print("Cache is fresh - nothing to fetch.")
