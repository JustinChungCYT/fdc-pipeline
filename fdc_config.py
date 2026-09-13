"""
Configuration parameters for the FDC compliance pipeline.

Thresholds and pattern classes are process-specific and must be tuned per
deployment; the values below are placeholders.
"""

# PATTERNS, DAYS_BACK,
# DATA_LOADER_AUTO_PATH, DATA_LOADER_FIXED_PATH, FDC_SPEC_REPORT_PATH, FDC_SPEC_BUCKET_KEY,
# TIMESTAMP

# SENSOR_GRADES,
# RAW_S3_PREFIX,
# WINDOW_DAYS,
# PREFILL_CACHE,
# CURRENT_WEEK_DAYS,
# _BUCKET,
# LOG_DESTINATION

# CALCULATIONS_FILES_PATH,
# FIXED_WEEKLY_REPORT,
# FIXED_WEEKLY_REPORT_KEY,
# PREVIOUS_WW_KEY

from fdc_utilities import time_range_setter, get_current_work_week, work_week, get_s3_client
import pathlib
import datetime

# project path (defined first so the eqp_raw paths below can reference it)
PROJECT_ROOT_DIR = pathlib.Path(__file__).parent.resolve()

# Sensor behavioural pattern classes the report covers.
PATTERNS = ['PATTERN_A', 'PATTERN_B', 'PATTERN_C', 'PATTERN_D']
# Sensor criticality grades included. Adding the lowest grade increases run time
# significantly, so it is excluded.
SENSOR_GRADES = ['G1', 'G2', 'G3', 'G4']
#days back filter for the last used date
date_threshdold = 90
#changing to rolling days
DAYS_BACK = 30
# --- Compliance thresholds -------------------------------------------------
# Tune these per process; the values below are placeholders.

# Minimum standard deviation before a sensor is treated as flatlined.
STDEV = 0.0
# Minimum surviving data points for a spec to be evaluated.
MIN_POINTS = 0
# Per-pattern fail thresholds: {pattern: [sigma_limit, relative_limit]}.
# A spec fails when its spec window is too loose for its observed variability.
FAIL_CONDITION = {
    'PATTERN_A': [0, 0],
    'PATTERN_B': [0, 0],
    'PATTERN_C': [0, 0],
    'PATTERN_D': [0, 0],
}
# this is the time range for the cloud table sensor_readings
DATE_FROM, DATE_TO = time_range_setter(days_back=DAYS_BACK)

# ----------------- Rolling-window cache settings -----------------
# Exact calendar-day window (generalized custom X-day window).
WINDOW_DAYS = 30

# Cold start: if False, a fresh cache only fetches the most recent CURRENT_WEEK_DAYS
# days of data instead of backfilling the whole window's previous weeks.
PREFILL_CACHE = False
CURRENT_WEEK_DAYS = 7
# Rolling cache root: day-partitioned BDE data lives under eqp_raw/<YYYY-MM-DD>/.
RAW_S3_PREFIX = "eqp_raw_by_eqp"
RAW_STORAGE_ROOT = PROJECT_ROOT_DIR / RAW_S3_PREFIX
STORAGE_MODE = 's3'  # Options: 'local', 's3', 'both'
RAW_EQP_DIR = RAW_STORAGE_ROOT
#s3 bucket fdc limits optimization (flo)
_BUCKET = 'sensor-cache'
# fiscal work week
date = datetime.datetime.now()

CURRENT_WORK_WEEK = work_week(date)
# CURRENT_WORK_WEEK = 22

# eqp_dir = 'eqp'
# API_URL = (
# f"http://api.internal.example.com/api/bucket/content?bucket=flo&directory={eqp_dir}/"
# )

S3_CLIENT = get_s3_client()    

MEMORY_THRESHOLD_IN_GB = 10

now = datetime.datetime.now()
# Format the datetime object into a string
TIMESTAMP = now.strftime('%Y-%m-%d %H:%M:%S')

# The number of days back to check if an item has flagged
previous_fails_days = 7
pww_date = datetime.datetime.now() - datetime.timedelta(previous_fails_days)
# the previous work week is needed to pull the logged result
PREVIOUS_WORK_WEEK = work_week(pww_date)
# previous work week cloud key
PREVIOUS_WW_KEY = f'reports/fdc_report_{PREVIOUS_WORK_WEEK}.parquet'
########################  fdc fixed memory #########################
MEMORY_PROFILE_PATH =  PROJECT_ROOT_DIR / f'reports/fixed_fdc_memory_{CURRENT_WORK_WEEK}.parquet.gzip'
FDC_MEMORY_KEY = f'memory/fixed_fdc_memory_{CURRENT_WORK_WEEK}.parquet'
######################## fdc data loader vars ######################
# fixed report name
DATA_LOADER_FIXED_PATH = PROJECT_ROOT_DIR / f'reports/fixed_fdc_spec_{CURRENT_WORK_WEEK}.parquet.gzip'
# auto report name
DATA_LOADER_AUTO_PATH =  PROJECT_ROOT_DIR /f'reports/auto_fdc_spec_{CURRENT_WORK_WEEK}.parquet.gzip'
# main spec report
FDC_SPEC_REPORT_PATH = PROJECT_ROOT_DIR /f'reports/fdc_spec_{CURRENT_WORK_WEEK}.parquet'
FDC_SPEC_BUCKET_KEY = f'spec/fdc_spec_{CURRENT_WORK_WEEK}.parquet'
FDC_SPEC_REPORT_S3_PATH = f'http://{_BUCKET}.s3.internal.example.com:9090/spec/fdc_spec_{CURRENT_WORK_WEEK}.parquet'
########################## fdc compliance fixed #####################
# The processed files will reside in a directory
CALCULATIONS_FILES_PATH = PROJECT_ROOT_DIR / 'fixed_calculations'
# The unprocessed files path
FIXED_RAW_FILES_PATH = PROJECT_ROOT_DIR / 'fixed_raw'
RAW_EQP_FILES_PATH = PROJECT_ROOT_DIR / 'eqp_raw'
# GB before chunking
MAX_FILE_GB_SIZE = 0.5
# The size in GB separating small and large files for efficient processing
SIZE_BIN_THRESHOLD_BIN = 0.04968
# Total chunks to break a large file
LG_FILE_CHUNKS = 20
# Path where the fixed weekly report is temporarily stored
FIXED_WEEKLY_REPORT = PROJECT_ROOT_DIR / f'reports/fixed_fdc_results_{CURRENT_WORK_WEEK}.parquet.gzip'
# S3 key for the fixed weekly report (pod local dir is ephemeral, so persist to S3)
FIXED_WEEKLY_REPORT_KEY = f'reports/fixed_fdc_results_{CURRENT_WORK_WEEK}.parquet.gzip'

FIXED_WORKER_LOG_PATH = PROJECT_ROOT_DIR / f'reports/fixed_worker_log_{CURRENT_WORK_WEEK}.csv'
AUTO_WORKER_LOG_PATH = PROJECT_ROOT_DIR / f'reports/auto_worker_log_{CURRENT_WORK_WEEK}.csv'
# Memory allocations
# MAX_MEMORY_MB = 1000
MAX_ROWS = 20_000_000
# MAX_ROWS = 4000000
TOTAL_QUERY_GROUPS = 4
# Max number of BDE fetches (and therefore resident Arrow tables) at once.
# Bounds peak memory during collect_fdc_fixed_data_from_bde() so a whole group
# is no longer materialized simultaneously.
MAX_CONCURRENT_FETCHES = 30
ROW_BUDGET = 20_000_000
######################### fdc compliance auto ######################
# Define the date format
format_string = '%Y-%m-%d'
# Convert the date string to a datetime object
AUTO_LAST_USED_DATE =  datetime.datetime.strptime(DATE_FROM, format_string)
# PAth where the auto weekly report is temporarily saved
AUTO_WEEKLY_REPORT = PROJECT_ROOT_DIR / f'reports/auto_fdc_results_{CURRENT_WORK_WEEK}.parquet.gzip'
# PROCESSED_FILES_PATH = PROJECT_ROOT_DIR / 'processed'
AUTO_RAW_FILES_PATH = PROJECT_ROOT_DIR / 'auto_raw'
########################### fdc reports ####################################
FDC_REPORT_NAME = f'fdc_report_{CURRENT_WORK_WEEK}.parquet'
FDC_BUCKET_KEY = f'reports/{FDC_REPORT_NAME}'
# FDC_BUCKET_KEY = f's3://SAS/reports/fdc_report_{CURRENT_WORK_WEEK}.parquet'
FDC_REPORTS_PATH = PROJECT_ROOT_DIR / 'reports'
FDC_WEEKLY_REPORT = PROJECT_ROOT_DIR / f'reports/{FDC_REPORT_NAME}'

########################### BDE to S3 ######################################
"""
Pipeline:
Fetch per-equipment data from BDE and store them to S3 bucket in parallel

Architecture:
Producer-consumer. Producers put the fetched data from BDE to a bounded queue, and then the consumers pick up and upload the data in the
queue to S3 buckets. The queue lives on RAM so spawning too many producers will lead to OOM. Current optimal value is 16. Number of
consumers and queue size don't matter that much as BDE request round-trip is the main performance bottleneck, so fine-tuned number of
producers for better performance.
"""
# Max concurrent workers allowed for BDE data fetching
DB_CONCURRENCY = 16
# Max concurrent workers for uploading to S3
UPLOAD_CONCURRENCY = 16
# Queue size for the producer-consumer model
QUEUE_MAXSIZE = 64
# Size of the temp file consumer store the data on before uploading to S3. Safe to disregard (theoretically will never hit)
SPOOL_MAX_SIZE = 10 * 1024 * 1024 # 10 MB before spilling to temp disk

# Where the pipeline logger logs the progress. Options: 'console', 'file', 'both'
LOG_DESTINATION = 'console'
# File path the progress will be logged to if LOG_DESTINATION is set to 'file' or 'both'
LOG_PATH = "runtime_logs/s3_upload.log"
