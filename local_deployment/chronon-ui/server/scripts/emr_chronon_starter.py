#!/usr/bin/env python3
"""
Chronon EMR Serverless Starter Script

Runs on EMR Serverless to:
1. Download compiled config from S3
2. Execute Chronon Driver JAR via spark-submit

Usage: This script is uploaded to S3 and used as the EMR Serverless entry point.
"""

import os
import sys
import subprocess
import logging
import argparse
import json
import tempfile
import shutil
import boto3

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def download_config(s3_bucket: str, config_path: str, s3_prefix: str, work_dir: str) -> str:
    """Download compiled config from S3. Returns local path."""

    # config_path is like "compiled/group_bys/team/name.v1__1"
    # s3_prefix is like "chronon-files/compiled/"
    # We need to replace "compiled/" with the actual s3_prefix
    if config_path.startswith("compiled/"):
        relative_path = config_path[len("compiled/"):]  # "group_bys/team/name.v1__1"
    else:
        relative_path = config_path
    
    s3_key = f"{s3_prefix.rstrip('/')}/{relative_path}"
    local_config_path = os.path.join(work_dir, "compiled", relative_path)
    os.makedirs(os.path.dirname(local_config_path), exist_ok=True)
    
    logger.info(f"Downloading s3://{s3_bucket}/{s3_key} -> {local_config_path}")
    boto3.client('s3').download_file(s3_bucket, s3_key, local_config_path)
    return local_config_path


def get_app_name(config_path: str, subcommand: str) -> str:
    """Generate application name from config."""
    try:
        with open(config_path, 'r') as f:
            name = json.load(f).get("metaData", {}).get("name", os.path.basename(config_path))
    except Exception:
        name = os.path.basename(config_path)
    return f"chronon_{subcommand}_{name}".replace("-", "_")


def build_spark_command(
    config_path: str,
    subcommand: str,
    ds: str,
    online_class: str = None,
) -> str:
    """Build the spark-submit command for Chronon Driver."""
    driver_jar = os.environ.get("CHRONON_DRIVER_JAR")
    online_jar = os.environ.get("CHRONON_ONLINE_JAR", "")
    
    if not driver_jar:
        raise ValueError("CHRONON_DRIVER_JAR environment variable not set")
    
    app_name = get_app_name(config_path, subcommand)
    spark_submit = os.environ.get("SPARK_SUBMIT_PATH", "spark-submit")
    
    # Build command parts
    cmd_parts = [
        spark_submit,
        "--class", "ai.chronon.spark.Driver",
        "--conf", f"spark.app.name={app_name}",
        # AWS Glue Data Catalog configs
        "--conf", "spark.sql.catalogImplementation=hive",
        "--conf", "spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
    ]
    
    if online_jar:
        cmd_parts.extend(["--jars", online_jar])
    
    # Driver JAR and subcommand
    cmd_parts.extend([driver_jar, subcommand])
    
    # Driver arguments
    cmd_parts.extend([f"--conf-path={config_path}", f"--end-date={ds}"])
    
    # Online args for upload-to-kv mode
    if online_jar and online_class:
        cmd_parts.extend([f"--online-jar={online_jar}", f"--online-class={online_class}"])
    
    return " ".join(cmd_parts)


def run_chronon_job(args):
    """Main job execution logic."""
    work_dir = args.work_dir or tempfile.mkdtemp(prefix="chronon_")
    
    try:
        # Download config
        local_config_path = download_config(args.s3_bucket, args.config_path, args.s3_prefix, work_dir)
        
        # Build and run spark-submit
        cmd = build_spark_command(
            config_path=local_config_path,
            subcommand=args.subcommand,
            ds=args.ds,
            online_class=args.online_class,
        )
        
        logger.info(f"Running: {cmd}")
        subprocess.run(cmd, shell=True, check=True, cwd=work_dir)
        logger.info("✅ Chronon job completed successfully!")
        
    finally:
        if not args.work_dir and os.path.exists(work_dir):
            shutil.rmtree(work_dir)


def main():
    parser = argparse.ArgumentParser(description="Chronon EMR Serverless Runner")
    parser.add_argument("--s3-bucket", required=True, help="S3 bucket with compiled configs")
    parser.add_argument("--s3-prefix", required=True, help="S3 key prefix for compiled configs (e.g., chronon-files/compiled/)")
    parser.add_argument("--config-path", required=True, help="Config path (e.g., compiled/group_bys/quickstart/page_views.v1__1)")
    parser.add_argument("--subcommand", required=True, help="Chronon Driver subcommand (e.g., group-by-backfill, join)")
    parser.add_argument("--ds", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--start-ds", help="Start date for range backfill")
    parser.add_argument("--online-class", help="Online class for upload-to-kv mode")
    parser.add_argument("--work-dir", help="Local working directory (defaults to temp)")
    
    args = parser.parse_args()
    
    logger.info("=" * 50)
    logger.info("Chronon EMR Serverless Runner")
    logger.info(f"  S3 Bucket: {args.s3_bucket}")
    logger.info(f"  S3 Prefix: {args.s3_prefix}")
    logger.info(f"  Config: {args.config_path}")
    logger.info(f"  Subcommand: {args.subcommand}")
    logger.info(f"  Date: {args.ds}")
    logger.info("=" * 50)
    
    try:
        run_chronon_job(args)
    except Exception as e:
        logger.error(f"❌ Job failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
