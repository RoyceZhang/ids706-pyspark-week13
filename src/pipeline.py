"""Week 13: PySpark large-scale taxi trip processing and performance study.

Full project:    python src/pipeline.py --keep-ui
Development:     python src/pipeline.py --months 1 --allow-small-input

The full run requires >= 1,000,000,000 bytes of raw Parquet inputs.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from contextlib import redirect_stdout
from pathlib import Path

os.environ.setdefault('PYSPARK_PYTHON', sys.executable)

from pyspark.sql import SparkSession, functions as F  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / 'data' / 'raw'
OUTPUT = ROOT / 'output'
RESULTS = ROOT / 'results'


def seconds_for(func):
    started = time.perf_counter()
    value = func()
    return round(time.perf_counter() - started, 3), value


def main():
    parser = argparse.ArgumentParser(description='PySpark Week 13 mini assignment.')
    parser.add_argument('--months', type=int, default=4, choices=(1, 2, 3, 4))
    parser.add_argument('--allow-small-input', action='store_true', help='Only for development/testing')
    parser.add_argument('--keep-ui', action='store_true', help='Keep Spark session alive for screenshots')
    args = parser.parse_args()
    RESULTS.mkdir(exist_ok=True, parents=True)
    OUTPUT.mkdir(exist_ok=True, parents=True)

    inputs = [RAW / f'fhvhv_tripdata_2024-{m:02d}.parquet'
              for m in range(1, args.months + 1)]
    zone_csv = RAW / 'taxi_zone_lookup.csv'
    missing = [str(p) for p in inputs + [zone_csv] if not p.is_file()]
    if missing:
        raise FileNotFoundError('Missing inputs. Run python scripts/download_data.py first:\n' + '\n'.join(missing))
    raw_bytes = sum(p.stat().st_size for p in inputs)
    print(f'[DATA] {len(inputs)} Parquet files; {raw_bytes:,} bytes ({raw_bytes / 1e9:.3f} GB)')
    if raw_bytes < 1_000_000_000 and not args.allow_small_input:
        raise ValueError('Dataset must be >=1 GB for the final assignment. Download all four months.')

    spark = (SparkSession.builder
             .master(os.environ.get('SPARK_MASTER', 'local[4]'))
             .appName('Week13-NYC-HVFHV-PySpark')
             .config('spark.driver.memory', '4g')
             .config('spark.sql.adaptive.enabled', 'true')
             .config('spark.sql.shuffle.partitions', '16')
             .config('spark.sql.parquet.filterPushdown', 'true')
             .getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    print('[SPARK] Version:', spark.version)
    print('[SPARK] UI:', spark.sparkContext.uiWebUrl)

    # Reading + projection (column pruning), before any wide operation.
    trips = (spark.read.parquet(*[str(p) for p in inputs])
             .select('pickup_datetime', 'PULocationID', 'trip_miles',
                     'base_passenger_fare', 'tips', 'hvfhs_license_num'))

    # 2+ filters. Simple Parquet-compatible predicates appear early.
    end_month = f'2024-{args.months + 1:02d}-01'
    filtered = (trips
        .filter(F.col('pickup_datetime') >= F.lit('2024-01-01').cast('timestamp'))
        .filter(F.col('pickup_datetime') < F.lit(end_month).cast('timestamp'))
        .filter((F.col('trip_miles') > 0) & (F.col('trip_miles') <= 50))
        .filter(F.col('base_passenger_fare') > 0)
        .filter(F.col('PULocationID').isNotNull())
        .filter(F.col('hvfhs_license_num').isNotNull()))

    # withColumn: data types and the monthly/hourly business dimensions.
    clean = (filtered
        .withColumn('pickup_month', F.date_format('pickup_datetime', 'yyyy-MM'))
        .withColumn('pickup_hour', F.hour('pickup_datetime'))
        .withColumn('fare_usd', F.col('base_passenger_fare').cast('double'))
        .withColumn('tip_usd', F.coalesce(F.col('tips').cast('double'), F.lit(0.0))))

    # Very small dimension table; broadcast avoids a shuffle of large trip records.
    zones = (spark.read.option('header', 'true').csv(str(zone_csv))
        .select(F.col('LocationID').cast('integer').alias('PULocationID'),
                F.col('Borough'), F.col('Zone'))
        .dropDuplicates(['PULocationID']))
    enriched = clean.join(F.broadcast(zones), on='PULocationID', how='inner')

    # Required wide transformation: groupBy + aggregations.
    zone_hourly = (enriched
        .groupBy('pickup_month', 'pickup_hour', 'Borough', 'Zone', 'hvfhs_license_num')
        .agg(F.count('*').alias('trip_count'),
             F.sum('fare_usd').alias('total_base_fare'),
             F.sum('tip_usd').alias('total_tips'),
             F.sum('trip_miles').alias('total_miles')))

    # Capture both optimizer stages and physical scan/join details.
    plan_path = RESULTS / 'execution_plan.txt'
    with plan_path.open('w', encoding='utf-8') as f, redirect_stdout(f):
        print('==== EXTENDED PLAN: parsed/analyzed/optimized/physical ====')
        zone_hourly.explain(mode='extended')
        print('\n==== FORMATTED PHYSICAL PLAN ====')
        zone_hourly.explain(mode='formatted')
    print('[PLAN] Saved to:', plan_path)
    print('[PLAN] Look for PushedFilters, ReadSchema, BroadcastHashJoin, Exchange, AdaptiveSparkPlan.')

    # Demonstrate transformations are lazy: constructing these lines did not
    # launch a Spark job; collect() below is an ACTION and does launch a job.
    start_lazy = time.perf_counter()
    lazy_demo = trips.filter(F.col('trip_miles') > 0).select('trip_miles')
    lazy_build_seconds = round(time.perf_counter() - start_lazy, 6)
    print(f'[LAZY] filter() + select() constructed in {lazy_build_seconds:.6f}s; no action yet.')
    action_seconds, sample = seconds_for(lambda: lazy_demo.limit(5).collect())
    print(f'[ACTION] limit(5).collect() took {action_seconds}s; sample:', sample)

    # Cache the SMALL, reused aggregation rather than the ~1.9-GB raw table.
    # First action materializes cache; repeated count should usually be faster.
    cached = zone_hourly.cache()
    first_seconds, aggregation_rows = seconds_for(lambda: cached.count())
    second_seconds, second_rows = seconds_for(lambda: cached.count())
    if aggregation_rows != second_rows:
        raise AssertionError('Cache benchmark row counts do not match.')
    print(f'[CACHE] First count (populate cache): {first_seconds}s')
    print(f'[CACHE] Repeated count (cached): {second_seconds}s')
    cached.createOrReplaceTempView('zone_hourly')

    # Spark SQL query #1: monthly volume, fare, and weighted average distance.
    sql_monthly = spark.sql('''
        SELECT pickup_month,
               SUM(trip_count) AS trips,
               ROUND(SUM(total_base_fare), 2) AS base_fare_usd,
               ROUND(SUM(total_miles) / SUM(trip_count), 2) AS avg_trip_miles
        FROM zone_hourly
        GROUP BY pickup_month
        ORDER BY pickup_month
    ''')

    # Spark SQL query #2: busiest borough-hour combinations.
    sql_peak_hours = spark.sql('''
        SELECT Borough, pickup_hour,
               SUM(trip_count) AS trips,
               ROUND(SUM(total_base_fare), 2) AS base_fare_usd
        FROM zone_hourly
        GROUP BY Borough, pickup_hour
        ORDER BY trips DESC
        LIMIT 10
    ''')

    monthly_seconds, monthly_rows = seconds_for(lambda: sql_monthly.collect())
    peaks_seconds, peak_rows = seconds_for(lambda: sql_peak_hours.collect())
    print('\n[SQL QUERY 1] Monthly volume / base fare / average distance')
    sql_monthly.show(10, truncate=False)
    print('\n[SQL QUERY 2] Busiest borough-hour combinations')
    sql_peak_hours.show(10, truncate=False)

    # Partitioning by month organizes output by a query-friendly dimension;
    # coalesce after the aggregation bounds the number of small output files.
    write_seconds, _ = seconds_for(lambda:
        cached.coalesce(4).write.mode('overwrite')
        .partitionBy('pickup_month').parquet(str(OUTPUT / 'zone_hourly')))
    sql_monthly.write.mode('overwrite').parquet(str(OUTPUT / 'monthly_summary'))
    sql_peak_hours.write.mode('overwrite').parquet(str(OUTPUT / 'top_borough_hours'))

    # Verify output is actually readable; this is another eager action.
    read_seconds, written_rows = seconds_for(lambda:
        spark.read.parquet(str(OUTPUT / 'zone_hourly')).count())
    assert written_rows == aggregation_rows, 'Parquet output row count mismatch!'

    findings = {
        'monthly_summary': [row.asDict(recursive=True) for row in monthly_rows],
        'top_borough_hours': [row.asDict(recursive=True) for row in peak_rows],
    }
    performance = {
        'dataset_parquet_bytes': raw_bytes,
        'dataset_parquet_gb_decimal': round(raw_bytes / 1e9, 3),
        'input_file_count': len(inputs),
        'spark_version': spark.version,
        'spark_ui': spark.sparkContext.uiWebUrl,
        'aggregation_result_rows': aggregation_rows,
        'lazy_dataframe_construction_seconds': lazy_build_seconds,
        'collect_action_seconds': action_seconds,
        'first_aggregation_count_and_cache_population_seconds': first_seconds,
        'repeat_cached_aggregation_count_seconds': second_seconds,
        'sql_monthly_collect_seconds': monthly_seconds,
        'sql_peak_hours_collect_seconds': peaks_seconds,
        'zone_hourly_parquet_write_seconds': write_seconds,
        'written_parquet_verification_count_seconds': read_seconds,
        'verified_written_rows': written_rows,
    }
    (RESULTS / 'performance_metrics.json').write_text(json.dumps(performance, indent=2), encoding='utf-8')
    (RESULTS / 'query_findings.json').write_text(json.dumps(findings, indent=2), encoding='utf-8')
    print('\n[SUCCESS] Output Parquet:')
    print('  -', OUTPUT / 'zone_hourly')
    print('  -', OUTPUT / 'monthly_summary')
    print('  -', OUTPUT / 'top_borough_hours')
    print('[SUCCESS] Verified aggregate rows in output:', written_rows)
    print('[SUCCESS] Run metrics:', RESULTS / 'performance_metrics.json')
    print('[SUCCESS] Query results:', RESULTS / 'query_findings.json')
    print('[SCREENSHOTS] Open', spark.sparkContext.uiWebUrl, 'to review Spark SQL/Jobs/Stages.')
    if args.keep_ui:
        try:
            input('\nSpark UI is available. Capture screenshots, then press ENTER to stop Spark... ')
        except EOFError:
            print('[INFO] No interactive input detected; closing Spark.')
    cached.unpersist()
    spark.stop()


if __name__ == '__main__':
    main()
