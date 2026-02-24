"""
ETL Pipeline - Perusahaan XYZ
=============================

Pipeline ini menjalankan proses ETL untuk 3 data source:
1. Sales Data (PostgreSQL) -> Tim Sales
2. Products Data (CSV) -> Tim Product  
3. NLP Training Data (Web Scraping) -> Tim Data Scientist

Strategy:
- INCREMENTAL: Sales pipeline → complete() returns False → always rerun (Extract → Transform → Load UPSERT)
- STATIC: Products, NLP, dim_date → run once with flag

Author: Data Engineer Team
Created: 2026-02-15
"""

import sys
import argparse
import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from Task.extract import ExtractAllData
from Task.transform import TransformAllData
from Task.load import LoadAllData
import luigi

# Load environment variables
load_dotenv()

# Setup logging - force UTF-8 encoding
LOG_DIR = 'logs'
os.makedirs(LOG_DIR, exist_ok=True)

# File handler with UTF-8
file_handler = logging.FileHandler(
    f'{LOG_DIR}/etl_pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    encoding='utf-8'
)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Stream handler with UTF-8
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler, stream_handler]
)

logger = logging.getLogger(__name__)


def run_extract():
    """Run Extract Phase"""
    logger.info("=" * 60)
    logger.info("[PHASE 1] EXTRACT")
    logger.info("  INCREMENTAL → Sales (always re-extract from source)")
    logger.info("  STATIC      → Products, NLP (skip if already done)")
    logger.info("=" * 60)
    
    success = luigi.build(
        [ExtractAllData()],
        local_scheduler=True,
        log_level='WARNING'
    )
    
    return success


def run_transform():
    """Run Transform Phase"""
    logger.info("=" * 60)
    logger.info("[PHASE 2] TRANSFORM")
    logger.info("  INCREMENTAL → Sales (always re-transform)")
    logger.info("  STATIC      → Products, NLP (skip if already done)")
    logger.info("=" * 60)
    
    success = luigi.build(
        [TransformAllData()],
        local_scheduler=True,
        log_level='WARNING'
    )
    
    return success


def run_load():
    """Run Load Phase"""
    logger.info("=" * 60)
    logger.info("[PHASE 3] LOAD")
    logger.info("  INCREMENTAL → Sales (UPSERT - insert new, update existing)")
    logger.info("  STATIC      → Products, NLP, dim_date (skip if already done)")
    logger.info("=" * 60)
    
    success = luigi.build(
        [LoadAllData()],
        local_scheduler=True,
        log_level='WARNING'
    )
    
    return success


def run_full_pipeline():
    """Run full ETL pipeline"""
    start_time = datetime.now()
    
    logger.info("=" * 60)
    logger.info("    FULL ETL PIPELINE STARTED")
    logger.info("=" * 60)
    logger.info(f"  Start Time : {start_time}")
    logger.info(f"  Strategy   : INCREMENTAL (Sales) + STATIC (Products, NLP)")
    logger.info(f"  Sales      : complete() → False → always Extract → Transform → UPSERT")
    logger.info(f"  Others     : complete() → check flag → run once")
    logger.info("=" * 60)
    
    # Run all phases
    success = (
        run_extract() and 
        run_transform() and 
        run_load()
    )
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    if success:
        logger.info("=" * 60)
        logger.info("    ETL PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)
        logger.info(f"  Start Time : {start_time}")
        logger.info(f"  End Time   : {end_time}")
        logger.info(f"  Duration   : {duration:.2f}s")
        logger.info("=" * 60)
    else:
        logger.error("=" * 60)
        logger.error("    ETL PIPELINE FAILED!")
        logger.error("=" * 60)
        logger.error(f"  Duration   : {duration:.2f}s")
        logger.error("=" * 60)
    
    return success


def clean_output_files():
    """Clean all output files to force re-run"""
    logger.info("Cleaning output files...")
    
    # Files to clean
    files_to_clean = [
        # Raw data
        'Data Source/raw_SalesData.csv',
        'Data Source/raw/products_raw.csv',
        'Data Source/raw/reviews_raw.csv',
        # Cleaned data
        'Data Source/cleaned/sales_cleaned.csv',
        'Data Source/cleaned/products_cleaned.csv',
        'Data Source/cleaned/reviews_cleaned.csv',
        # Flags (STATIC tasks only)
        'logs/flags/.extract_products.done',
        'logs/flags/.extract_detik.done',
        'logs/flags/.transform_products.done',
        'logs/flags/.transform_reviews.done',
        # Load flags
        'logs/warehouse_tables_created.flag',
        'logs/dim_date_loaded.flag',
        'logs/dim_products_loaded.flag',
        'logs/fact_sales_loaded.flag',
        'logs/nlp_training_data_loaded.flag',
    ]
    
    cleaned_count = 0
    for filepath in files_to_clean:
        if os.path.exists(filepath):
            os.remove(filepath)
            logger.info(f"  Removed: {filepath}")
            cleaned_count += 1
    
    logger.info(f"\n  Cleaned {cleaned_count} files. All tasks will re-run on next execution.")


def show_help():
    """Show help message"""
    help_text = """
    ==============================================================
    |           ETL PIPELINE - PERUSAHAAN XYZ                    |
    ==============================================================
    |  Usage: python main.py [command]                           |
    |                                                            |
    |  Commands:                                                 |
    |    (no args)    Run full ETL pipeline                      |
    |    extract      Run Extract phase only                     |
    |    transform    Run Transform phase only                   |
    |    load         Run Load phase only                        |
    |    clean        Clean all output files (force re-run)      |
    |    help         Show this help message                     |
    |                                                            |
    |  Arguments (alternative):                                  |
    |    --phase extract|transform|load|full                     |
    |                                                            |
    |  Data Sources:                                             |
    |    1. PostgreSQL  -> Sales Data (Tim Sales) [INCREMENTAL]  |
    |    2. CSV File    -> Electronics Products (Tim Product)    |
    |    3. Web Scraping-> Detik.com Articles (Tim Data Science) |
    |                                                            |
    |  Data Warehouse:                                           |
    |    PostgreSQL (Docker) -> dim_date, dim_products,          |
    |                           fact_sales, nlp_training_data    |
    |                                                            |
    |  Strategy:                                                 |
    |    INCREMENTAL: Sales → complete()=False → always rerun    |
    |    STATIC     : Products, NLP → run once with flag         |
    ==============================================================
    """
    print(help_text)


if __name__ == '__main__':
    # Parse arguments - support both positional and named arguments
    parser = argparse.ArgumentParser(
        description='ETL Pipeline - Perusahaan XYZ',
        add_help=False  # Disable default help to use custom
    )
    
    # Support --phase argument for shell script
    parser.add_argument(
        '--phase', 
        choices=['extract', 'transform', 'load', 'full'],
        help='ETL phase to run'
    )
    
    # Support positional argument for backward compatibility
    parser.add_argument(
        'command',
        nargs='?',  # Optional
        choices=['extract', 'transform', 'load', 'clean', 'help'],
        help='ETL command to run'
    )
    
    args = parser.parse_args()
    
    # Determine which command to run
    command = args.phase or args.command or 'full'
    
    # Execute command
    try:
        if command == 'extract':
            success = run_extract()
        elif command == 'transform':
            success = run_transform()
        elif command == 'load':
            success = run_load()
        elif command == 'clean':
            clean_output_files()
            success = True
        elif command == 'help':
            show_help()
            success = True
        else:  # full or no args
            success = run_full_pipeline()
        
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.error("\n⚠️  Pipeline interrupted by user!")
        sys.exit(130)
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}", exc_info=True)
        sys.exit(1)