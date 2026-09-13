import numpy as np
import polars as pl
import asyncio
import os
import csv
import time
import functools
import gc
import traceback
from datetime import datetime
from fdc_config import DATA_LOADER_FIXED_PATH, \
    S3_CLIENT, _BUCKET, DATE_FROM, DATE_TO, SENSOR_GRADES,\
        STDEV, MIN_POINTS, FAIL_CONDITION, CALCULATIONS_FILES_PATH,\
        FIXED_WEEKLY_REPORT, MEMORY_PROFILE_PATH,\
        FIXED_RAW_FILES_PATH, PREVIOUS_WW_KEY,\
        LG_FILE_CHUNKS, MAX_ROWS, MAX_CONCURRENT_FETCHES, ROW_BUDGET,\
        FIXED_WORKER_LOG_PATH
from tqdm import  tqdm
from fdc_utilities import get_rss_memory_gb, format_list_to_quoted_string, format_float_list
# from math import ceil
import ctypes
from concurrent.futures import ThreadPoolExecutor
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Final, List
from pathlib import Path
from io import BytesIO
from internal_data_loader import getData
import re
from fdc_util import get_pod_memory_usage

# ----------------------------------------------------------------------
# Constants (keep them grouped with the other globals at the top of the file)
# ----------------------------------------------------------------------
DATA_LOADER_FIXED_PATH: Final[Path] = Path(DATA_LOADER_FIXED_PATH)  # noqa: W605

# ----------------------------------------------------------------------
# Cached loader – lazy, column‑pruned, cast‑once
# ----------------------------------------------------------------------
@functools.lru_cache(maxsize=512)
def get_fixed_data() -> pl.DataFrame:
    """Load the fixed-range spec catalog written by the spec stage.

    Lazily scans the parquet, prunes to the columns the calculations consume, and
    casts the join key once before collecting. Cached for the lifetime of the
    process so repeated per-tool calls are free.
    """
    # TODO: scan the catalog parquet, pruning to the consumed columns.
    pass

def fdc_fixed_statistical_calculations(df: pl.LazyFrame) -> pl.LazyFrame:
    """Per-spec descriptive statistics with outlier rejection.

    For each sensor spec: derive robust outlier bounds from the value
    distribution, drop the points that fall outside them, then aggregate the
    surviving points into count / mean / standard deviation / coefficient of
    variation, carrying the most recent spec limits alongside. Specs with too
    few surviving points are dropped.

    Operates on a LazyFrame and returns a LazyFrame so the caller controls when
    the plan is materialised.
    """
    # TODO: implement outlier rejection and the per-spec aggregations.
    pass

def fdc_fixed_pattern_calculations(df: pl.LazyFrame):
    """Per-pattern control-limit derivation and compliance verdict.

    Derives upper/lower control limits using the formula appropriate to each
    sensor's behavioural pattern, compares the configured spec window against the
    sensor's observed variability, and flags specs whose window is too loose for
    the noise they exhibit.

    Also suppresses known false-positive classes - dead or flatlined sensors,
    specs with too little data, and a configuration carve-out - so that only
    actionable findings reach the report.
    """
    # TODO: implement the per-pattern control-limit formulas and verdict rules.
    # fail/suppression conditions removed.
    pass

def fdc_fixed_calculations(df: pl.DataFrame) -> pl.DataFrame:
    """Run the statistics then the pattern verdict for one tool's raw data.

    Joins the spec catalog onto the per-spec statistics before the pattern stage.
    The catalog is reduced to distinct spec rows first: it carries several rows
    per spec, and without the reduction this join fans out the already row-level
    frame and was the source of a per-tool memory blow-up.
    """
    # TODO: join the catalog onto the statistics, then run the pattern stage.
    pass

async def fetch_tool_sensor_readings(eqp_id, dateFrom, dateTo, spec_ids, sensor_grades):
    """Fetch one tool's raw sensor readings for a date range.

    Runs the blocking loader call in a worker thread and returns an Arrow table.
    Re-raises on failure so the caller can record the task as failed.
    """
    # TODO: query the sensor-reading table for this tool and date range.
    pass

def bde_query_memory_profile(df):
    """Extract the planner's row/byte estimates from a query plan.

    Used to build the per-tool size profile that drives admission control in the
    collector below.
    """
    # TODO: parse the planner's row and byte estimates out of the query plan.
    pass

def parse_output_strings(data_list):
    """
    Parses a list of output strings to extract the number of rows,
    size value, and size unit (B, kB, MB, or GB).

    Args:
        data_list: A list of strings like ['Output:1row(295B)', ...]

    Returns:
        A list of dictionaries with keys 'rows', 'size_value', and 'size_unit'.
    """
    parsed_data = []
    # Regex pattern explanation:
    # r'(\d+)\s*rows?': Matches and captures one or more digits (\d+) 
    #                   followed by optional whitespace (\s*) and "row" or "rows" (rows?).
    # r'\(([\d.]+)([a-zA-Z]+)\)': Matches and captures a value inside parentheses, 
    #                             including potential decimals ([\d.]+), 
    #                             and the size unit ([a-zA-Z]+).
    pattern = re.compile(r'Output:(\d+)\s*rows?\(([\d.]+)([a-zA-Z]+)\)')

    for item in data_list:
        match = pattern.match(item)
        if match:
            # Extract the matched groups
            rows = int(match.group(1))
            size_value = float(match.group(2))
            size_unit = match.group(3)

            parsed_data.append({
                'rows': rows,
                'size_value': size_value,
                'size_unit': size_unit
            })
        else:
            print(f"Warning: Could not parse '{item}'")
            
    return parsed_data

def get_files(file_path):
    files = []
    try:
        with os.scandir(file_path) as entries:
            for entry in entries:
                if entry.is_file():  # Check if the entry is a file
                    files.append(entry.name)  # Add the filename to the list
        return files
    except FileNotFoundError:
        print(f"Error: Directory '{file_path}' not found.")
        return []
    except OSError as e:
        print(f"Error accessing directory of '{file_path}': {e}")
        return []


def get_completed_eqps(file_path):
    files = get_files(file_path)
    if files:
        eqps = [f[:6] for f in files]
        return eqps
    return []

def _download_parquet(key: str) -> pl.DataFrame:
    """
    Download a single Parquet object from S3 and convert it to a Polars DataFrame.
    """
    obj = S3_CLIENT.get_object(Bucket=_BUCKET, Key=key)
    # ``io.BytesIO`` avoids writing to disk and works with the Arrow reader directly
    with BytesIO(obj["Body"].read()) as buf:
        table = pq.read_table(buf)
    return pl.from_arrow(table)

def previous_work_week_fails():
    """Load the prior work week's verdicts as {spec id: fail flag}.

    Used to confirm repeat findings: a spec is only reported as failing when it
    failed in both the current and the previous week, which filters single-week
    noise.
    """
    # TODO: read the prior week's report; return {spec id: fail flag}.
    pass

def prepare_query_units(eqp_list, rows_list, df_fixed):
    """
    Flattens equipment IDs into individual query units.
    If an equipment's row count exceeds MAX_ROWS, it's split into chunks.
    Returns a list of units: (eqp_id, indices, est_rows, chunk_idx)
    """
    units = []
    for eqp_id, rows in zip(eqp_list, rows_list):
        df_temp_up = df_fixed.filter(pl.col('eqp_id') == eqp_id)
        indices = df_temp_up['spec_id'].unique().to_list()
        
        if rows >= MAX_ROWS:
            # print(f'Preparing chunks for large Eqp {eqp_id} ({rows} rows)')
            chunks = np.array_split(indices, LG_FILE_CHUNKS)
            est_rows_per_chunk = rows / LG_FILE_CHUNKS
            for j, chunk in enumerate(chunks):
                if chunk.size == 0: continue
                units.append((eqp_id, chunk.tolist(), est_rows_per_chunk, j))
        else:
            units.append((eqp_id, indices, rows, None))
            
    return units


def group_items_by_sum_inplace(units, max_sum):
    """
    Groups query units based on the sum of their estimated rows <= max_sum.
    units: List of tuples (eqp_id, indices, est_rows, chunk_idx)
    """
    # Sort units by estimated rows in descending order for better packing
    combined = sorted(units, key=lambda x: x[2], reverse=True)
    
    groups = []  # Store lists of lists: [[ [unit1, unit2], current_sum ], ...]
    
    for unit in combined:
        row_val = unit[2]
        found_group = False
        for group in groups:
            if group[1] + row_val <= max_sum:
                group[0].append(unit)
                group[1] += row_val
                found_group = True
                break
        if not found_group:
            groups.append([[unit], row_val])
            
    # # Print groups and their row counts in a pretty format
    # print("\n" + "="*50)
    # print(f"{'Group':<10} | {'Total Rows':<15} | {'Units'}")
    # print("-" * 50)
    # for i, group in enumerate(groups):
    #     unit_ids = [f"{u[0]}" + (f"_c{u[3]}" if u[3] is not None else "") for u in group[0]]
    #     print(f"{i+1:<10} | {group[1]:<15.2f} | {', '.join(unit_ids)}")
    # print("="*50 + "\n")
    
    return [group[0] for group in groups]

    

def _release_memory_to_os():
    """
    Return freed heap memory to the OS.

    Arrow/Polars/glibc retain freed pages, so RSS creeps up across groups even
    after ``gc.collect()`` (which does not trim malloc arenas). Releasing the
    Arrow pool and calling ``malloc_trim`` hands the memory back, preventing the
    cross-group creep that shrinks our 30 GB headroom.
    """
    try:
        pa.default_memory_pool().release_unused()
    except Exception:
        pass
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        pass


async def _fetch_and_write_unit(eqp_id, indices, chunk_idx):
    """
    Fetch a single query unit from BDE and write it straight to disk.

    The Arrow table is held only for the lifetime of this coroutine and is
    dropped as soon as it is written, so it is never retained by the caller.
    Admission (how many of these run at once) is governed by the caller's
    row-budget loop, not by this coroutine.

    Both the fetch and the parquet write run in worker threads, so neither
    blocks the event loop and writes overlap other units' fetches. We use the
    ``zstd`` codec instead of ``gzip``: it compresses several times faster at a
    similar ratio. (The file name keeps the historical ``.parquet.gzip`` suffix
    because the rest of the pipeline globs on it; the on-disk codec is recorded
    in the parquet metadata, so ``pl.scan_parquet`` reads it back transparently.)

    Returns a per-unit metrics dict (never a table) so the caller can log it for
    tuning ROW_BUDGET / MAX_ROWS / worker count.
    """
    out_name = f'{eqp_id}_chunk_{chunk_idx}' if chunk_idx is not None else eqp_id
    rec = {
        'out_name': out_name, 'eqp_id': eqp_id, 'chunk_idx': chunk_idx,
        'status': 'failed', 'actual_rows': 0, 'nbytes_mb': 0.0, 'disk_mb': 0.0,
        'fetch_sec': 0.0, 'write_sec': 0.0, 'error': '',
    }
    table = None
    try:
        t0 = time.perf_counter()
        table = await fetch_tool_sensor_readings(
            eqp_id=eqp_id,
            spec_ids=indices,
            dateFrom=DATE_FROM,
            dateTo=DATE_TO,
            sensor_grades=SENSOR_GRADES,
        )

        # Debug
        if table is None:
            print(f"fetch and write {eqp_id} returns empty table!")


        rec['fetch_sec'] = round(time.perf_counter() - t0, 3)
        if table is not None and len(table) > 0:
            rec['actual_rows'] = table.num_rows
            rec['nbytes_mb'] = round(table.nbytes / (1024 ** 2), 3)
            raw_path = FIXED_RAW_FILES_PATH / f'{out_name}.parquet.gzip'
            t1 = time.perf_counter()
            await asyncio.to_thread(
                pq.write_table, table, raw_path,
                compression='zstd', compression_level=3,
            )
            rec['write_sec'] = round(time.perf_counter() - t1, 3)
            rec['disk_mb'] = round(raw_path.stat().st_size / (1024 ** 2), 3)
            rec['status'] = 'wrote'
        else:
            rec['status'] = 'empty'
    except Exception as e:
        rec['status'] = 'failed'
        rec['error'] = repr(e)
        print(f'Task for {out_name} failed with error: {e}')
    finally:
        del table
    return rec


async def collect_fdc_fixed_data_from_bde():
    print("DEBUG: Inside collect_fdc_fixed_data_from_bde()")
    print("DEBUG: Calling get_fixed_data()...")
    df_fixed = get_fixed_data()
    print(f"DEBUG: get_fixed_data() finished. Rows: {len(df_fixed)}")
    # Ensure output directories exist. Git does not track empty dirs and Docker
    # COPY won't recreate them, so a fresh pod can be missing fixed_raw/ etc.,
    # which made every pq.write_table fail with ENOENT. exist_ok=True is a no-op
    # when they are already present.
    eqps_to_process = set(df_fixed['eqp_id'].unique().to_list())
    raw_completed_eqps = set(get_completed_eqps(FIXED_RAW_FILES_PATH))
    calc_completed_eqps = set(get_completed_eqps(CALCULATIONS_FILES_PATH))
    completed_eqps = raw_completed_eqps | calc_completed_eqps

    print(f'Total completed eqps: {len(completed_eqps)}')

    if completed_eqps:
        eqps_to_process -= completed_eqps
        print(f'Total eqps to process: {len(eqps_to_process)}')
        if len(eqps_to_process) == 0:
            print('No eqps to process...')
            return

    df_memory = pl.read_parquet(MEMORY_PROFILE_PATH)
    print('memory read...')
    # Ensure memory profile only considers EQP IDs we actually intend to process
    df_memory_filtered = df_memory.filter(pl.col("eqp_id").is_in(list(eqps_to_process))).filter(pl.col("rows") > 0)
    df_sorted = df_memory_filtered.sort("rows", descending=True)
    
    eqp_list = df_sorted["eqp_id"].to_list()
    rows_list = df_sorted["rows"].to_list()
    
    # 1. Flatten equipment into individual query units (Splitting Giants)
    query_units = prepare_query_units(eqp_list, rows_list, df_fixed)

    # Process the units biggest-first so a giant is never stuck behind a wall of
    # small units that have filled the budget; small units backfill the gaps.
    units = sorted(query_units, key=lambda u: u[2], reverse=True)
    total_units = len(units)
    processed_count = 0

    # --- Streaming, weighted-concurrency execution ---------------------------
    # The old design bin-packed units into ~140 static groups and ran each group
    # behind a barrier (the next group could not start until every unit of the
    # current one finished), so we paid each group's slowest-fetch *tail* 140
    # times. Here there are no groups: units are admitted continuously as soon as
    # budget frees up, keeping the pipeline saturated. Memory stays bounded
    # because we only admit while the estimated rows already in flight + the new
    # unit stay <= ROW_BUDGET (and never more than MAX_CONCURRENT_FETCHES units),
    # i.e. the same memory ceiling a single group used to enforce.
    loop = asyncio.get_running_loop()
    # Size the thread pool to the concurrency we actually want; the default
    # executor is only min(32, cpu+4), which silently caps fetches on a pod with
    # few cores. Headroom (x2) lets a unit's write run without waiting for a
    # fetch slot to free up.
    executor = ThreadPoolExecutor(max_workers=max(MAX_CONCURRENT_FETCHES * 2, 8))
    loop.set_default_executor(executor)

    # Per-unit profiling log, streamed (and flushed) row-by-row so the data
    # survives a crash/OOM. Append mode keeps earlier rows across resumed runs.
    log_fields = [
        'timestamp', 'out_name', 'eqp_id', 'chunk_idx', 'status',
        'est_rows', 'actual_rows', 'est_over_actual', 'nbytes_mb', 'disk_mb',
        'fetch_sec', 'write_sec', 'total_sec',
        'concurrency_at_admit', 'inflight_rows_at_admit',
        'concurrency_at_complete', 'inflight_rows_at_complete',
        'pod_mem_mb', 'max_concurrent_fetches', 'row_budget', 'error',
    ]
    _need_header = not (FIXED_WORKER_LOG_PATH.exists()
                        and FIXED_WORKER_LOG_PATH.stat().st_size > 0)
    log_file = open(FIXED_WORKER_LOG_PATH, 'a', newline='')
    log_writer = csv.DictWriter(log_file, fieldnames=log_fields)
    if _need_header:
        log_writer.writeheader()
        log_file.flush()

    # task -> (est_rows, admit_perf, concurrency_at_admit, inflight_rows_at_admit)
    in_flight = {}
    in_flight_rows = 0
    idx = 0
    RECLAIM_EVERY = 25        # trim RSS back to the OS every N completions

    try:
        while idx < total_units or in_flight:
            # Admit as many units as the row budget and count cap allow. Always
            # admit at least one when nothing is in flight so we make progress
            # even if a lone unit's estimate exceeds the budget.
            while idx < total_units:
                eqp_id, indices, est_rows, chunk_idx = units[idx]
                if not eqp_id:
                    idx += 1
                    continue
                over_budget = in_flight_rows + est_rows > ROW_BUDGET
                at_count_cap = len(in_flight) >= MAX_CONCURRENT_FETCHES
                if in_flight and (over_budget or at_count_cap):
                    break
                task = asyncio.create_task(
                    _fetch_and_write_unit(eqp_id, indices, chunk_idx),
                    name=eqp_id,
                )
                in_flight[task] = (
                    est_rows, time.perf_counter(),
                    len(in_flight) + 1, in_flight_rows + est_rows,
                )
                in_flight_rows += est_rows
                idx += 1

            if not in_flight:
                break

            done, _ = await asyncio.wait(
                in_flight, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                est_rows, admit_perf, conc_admit, rows_admit = in_flight.pop(task)
                in_flight_rows -= est_rows
                processed_count += 1
                rec = task.result()
                total_sec = round(time.perf_counter() - admit_perf, 3)
                pod_mem_mb = get_pod_memory_usage()

                actual_rows = rec['actual_rows']
                log_writer.writerow({
                    'timestamp': datetime.now().isoformat(timespec='seconds'),
                    'out_name': rec['out_name'],
                    'eqp_id': rec['eqp_id'],
                    'chunk_idx': rec['chunk_idx'],
                    'status': rec['status'],
                    'est_rows': est_rows,
                    'actual_rows': actual_rows,
                    # >1 means we under-estimated this unit's true row count.
                    'est_over_actual': round(est_rows / actual_rows, 3) if actual_rows else '',
                    'nbytes_mb': rec['nbytes_mb'],
                    'disk_mb': rec['disk_mb'],
                    'fetch_sec': rec['fetch_sec'],
                    'write_sec': rec['write_sec'],
                    'total_sec': total_sec,
                    'concurrency_at_admit': conc_admit,
                    'inflight_rows_at_admit': rows_admit,
                    'concurrency_at_complete': len(in_flight),
                    'inflight_rows_at_complete': in_flight_rows,
                    'pod_mem_mb': pod_mem_mb,
                    'max_concurrent_fetches': MAX_CONCURRENT_FETCHES,
                    'row_budget': ROW_BUDGET,
                    'error': rec['error'],
                })
                log_file.flush()

                print(f"-- {rec['out_name']}: {rec['status']} "
                      f"est(rows)={est_rows:,} actual(rows)={actual_rows:,} "
                      f"{rec['nbytes_mb']:.1f}MB->{rec['disk_mb']:.1f}MB "
                      f"fetch={rec['fetch_sec']}s write={rec['write_sec']}s "
                      f"({processed_count}/{total_units}, "
                      f"in-flight: {len(in_flight)} units / "
                      f"{in_flight_rows:,} est rows)")
                if processed_count % RECLAIM_EVERY == 0:
                    gc.collect()
                    _release_memory_to_os()
                    get_pod_memory_usage()
    finally:
        log_file.close()
        gc.collect()
        _release_memory_to_os()
        executor.shutdown(wait=True)

    print(f"--- Data collection finished (worker log: {FIXED_WORKER_LOG_PATH}) ---")


def combine_chunked_files():
    raw_file_paths = [entry.path for entry in os.scandir(FIXED_RAW_FILES_PATH) if entry.is_file() and 'chunk' in entry.name and entry.name != '.gitkeep']
    if not raw_file_paths:
        print('No chunked files to process')
        return 
    raw_file_paths.sort()
    df = pl.DataFrame({'paths':  raw_file_paths})
    
    df = df.with_columns(
        pl.col('paths').apply(lambda x: Path(x).name).alias('filename')
    )
    df = df.with_columns(
        pl.col("filename")
        .str.split("_")
        .list.first()
        .alias("eqp_id")
    )
    eqps = df['eqp_id'].unique().to_list()
    aggregated_df = df.group_by('eqp_id').agg(
    pl.col('paths').implode().alias('paths')
    )
    # 2. Convert to a dictionary keyed by 'eqp_id'
    # Use 'unique=True' instead of 'unique_keys=True'
    dupe_dict = aggregated_df.rows_by_key(
        key="eqp_id", 
        unique=True, 
        named=True
    )
    for eqp in eqps:
        print(f'Combining chunks for eqp_id {eqp}')
        chunked_files = dupe_dict[eqp]['paths'][0]
        lf = pl.scan_parquet(chunked_files)
        raw_path = FIXED_RAW_FILES_PATH / f'{eqp}.parquet.gzip'
        lf.sink_parquet(raw_path)
        for f in chunked_files:
            os.remove(f)
    print('- Completed combining chunked files -')


def process_file(_f: str) -> str:
    """
    Reads a Parquet file, performs calculations, and writes the output.
    Returns the name of the processed file.
    """
    try:
        start_time = time.perf_counter()
        eqp_file = os.path.basename(_f)
        eqp = eqp_file.split('.')[0]
        # print(f'[{eqp}] - Reading and calculating stats...')
        calculations_path = CALCULATIONS_FILES_PATH / f'{eqp}.parquet.gzip'
        if os.path.exists(calculations_path):
            return f'{eqp} calculation report located at {calculations_path}'
        # We start with a LazyFrame for memory efficiency
        df = pl.scan_parquet(str(_f))
        df_calc = fdc_fixed_calculations(df)
        
        duration = time.perf_counter() - start_time
        print(f'[{eqp}] - Processing took {duration:.3f}s')

        is_empty = df_calc.limit(1).collect().is_empty()
        if is_empty:
            print(f'No calculations for {eqp}')
        else:
            print(f'****\nBefore .collect().write_parquet():')
            get_pod_memory_usage()
            print(f"*****")
            df_calc.collect().write_parquet(
                str(calculations_path),
                compression="gzip",
                row_group_size=500_000)
            del df_calc

            # print(f'[{eqp}] - Saved stats {calculations_path}..')
    except Exception as e:
        traceback.print_exc()
        print(f"[{eqp}] - Error process_calculations: {e}")
        os.remove(_f)

def remove_calculation_files():
    try:
        print('Removing calculation files..')
        for entry in os.scandir(CALCULATIONS_FILES_PATH):
            if entry.is_file() and entry.name != '.gitkeep':
                os.remove(entry.path) 
        print('Calculation files removed!')
    except Exception as e:
        print(e)

def get_clean_name(path_string):
    """Strips both path and all file extensions (e.g., .parquet.gzip)."""
    p = Path(path_string)
    name = p.name
    while '.' in name:
        name = Path(name).stem
    return name

async def main():
    print("DEBUG: Entering main()")
    if os.path.exists(FIXED_WEEKLY_REPORT):
        print('fdc fixed report exists')
        return
    print("DEBUG: Starting collect_fdc_fixed_data_from_bde()")
    await collect_fdc_fixed_data_from_bde()
    print("DEBUG: collect_fdc_fixed_data_from_bde finished")
    combine_chunked_files()
    raw_file_paths = [entry.path for entry in os.scandir(FIXED_RAW_FILES_PATH) if entry.is_file() and entry.name != '.gitkeep']
    if not raw_file_paths:
        print("No files found to process..")
    for _f in tqdm(raw_file_paths):
        try:
            process_file(_f)
            # os.remove(_f)
            tqdm.write(f"Done processing task {_f}")
        except Exception as e:
            print(f'{_f} exception {e}')
    lazy_df = pl.scan_parquet(str(CALCULATIONS_FILES_PATH / "*.parquet.gzip"))
    # If you just need the combined DataFrame in memory, call .collect()

    df_wr = lazy_df.collect()
    pww_dict = previous_work_week_fails()
    df_wr = df_wr.with_columns(
        pl.col("spec_id").replace(
            pww_dict,  # Pass the dictionary directly as the first argument
            default=0
        ).alias("pww_fail")
    )
    del pww_dict
    df_wr = df_wr.with_columns(
        # Fill null values in the 'pww_fail' column with 0
        pl.col("pww_fail").fill_null(0)
    ).with_columns(
        # Implement the conditional logic for the 'fail' column
        pl.when((pl.col("cww_fail") == 1) & (pl.col("pww_fail") == 1))
        .then(1)  # `Current week fail` and `last week fail` => 1
        .when((pl.col("cww_fail") == 1) & (pl.col("pww_fail") == 0))
        .then(0)  # `Current week fail` and `past week pass` => 0
        .otherwise(pl.col("cww_fail")) # Leave other values unchanged
        .alias("fail")
    )
    df_wr.write_parquet(FIXED_WEEKLY_REPORT)
    # remove_calculation_files()
