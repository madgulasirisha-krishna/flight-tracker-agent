"""
BTS Historical Enrichment Pipeline — Lakeflow Declarative Pipeline source.

Defines three tables as a dependency chain:
  bts_flights_bronze -> bts_flights_cleaned -> flights_historical_enriched

This now owns the full chain from raw CSVs onward (01_bts_bronze_ingest.py
is superseded by the first table below — archive or delete it once this
pipeline is confirmed working).

This is a separate, TRIGGERED pipeline from both the adsb.lol streaming
pipeline and the RAG ingestion pipeline — different domain, same
reasoning as keeping those two apart (independent scheduling, failure
isolation, a cleaner story than one pipeline doing everything).

On verifying "12 files": pipeline table functions can't call .count() or
.collect() (explicitly disallowed — they're interpreted to build a DAG,
not executed imperatively), so there's no way to assert a file count
inline here. Instead, bts_flights_bronze tracks a source_file column per
row — after running the pipeline, verify file coverage with an ordinary
query:
    SELECT source_file, COUNT(*) FROM flight_tracker.bronze.bts_flights_bronze
    GROUP BY source_file
...which should show exactly 12 distinct source_file values. That's a
validation step you run separately, not something the pipeline itself
can assert.

IMPORTANT: column names for cleaning/aggregation assume the standard BTS
naming convention (Reporting_Airline, Origin, Dest, ArrDelay, Cancelled,
FlightDate). Kaggle mirrors of this dataset sometimes use different names
— check bts_flights_bronze's actual schema after a first run and adjust
the constants below if needed.

Pipeline configuration (set in the pipeline's settings):
  bts_csv_volume_path -> e.g. /Volumes/flight_tracker/raw/landing/bts_2025
"""

import re

from pyspark import pipelines as dp
from pyspark.sql import functions as F

bts_csv_volume_path = spark.conf.get(
    "bts_csv_volume_path", ""
)

# Adjust these if your actual column names differ from BTS-standard naming —
# check via bts_flights_bronze's schema after a first pipeline run.
CARRIER_COL = "Reporting_Airline"
ORIGIN_COL = "Origin"
DEST_COL = "Dest"
ARR_DELAY_COL = "ArrDelay"
CANCELLED_COL = "Cancelled"
FLIGHT_DATE_COL = "FlightDate"


# ============================================================
# Table 1: bts_flights_bronze
# ============================================================

@dp.table(
    comment="Raw ingest of the 12 BTS monthly CSVs directly from the "
            "landing volume, with source_file tracked per row so file "
            "coverage can be verified with a query after the pipeline "
            "runs (a table function can't assert a file count inline — "
            "count()/collect() aren't allowed here)."
)
def bts_flights_bronze():
    df = spark.sql(f"""
        SELECT *, _metadata.file_name AS source_file
        FROM READ_FILES(
            '{bts_csv_volume_path}/*.csv',
            format => 'csv',
            header => true,
            mergeSchema => true
        )
    """)
    # Drop auto-generated junk columns from trailing commas in the source
    # CSVs (e.g. "_c109") — this only inspects column NAMES (df.columns is
    # schema metadata, not a triggering action), so it's safe here.
    junk_cols = [c for c in df.columns if re.fullmatch(r"_c\d+", c)]
    if junk_cols:
        df = df.drop(*junk_cols)
    return df


# ============================================================
# Table 2: bts_flights_cleaned
# ============================================================

@dp.table(
    comment="Deduplicated, null-filtered BTS flight records."
)
def bts_flights_cleaned():
    bronze_df = dp.read("bts_flights_bronze")

    return (
        bronze_df
        .filter(
            F.col(CARRIER_COL).isNotNull()
            & F.col(ORIGIN_COL).isNotNull()
            & F.col(DEST_COL).isNotNull()
        )
        .dropDuplicates([FLIGHT_DATE_COL, CARRIER_COL, ORIGIN_COL, DEST_COL])
        .withColumn(CANCELLED_COL, F.coalesce(F.col(CANCELLED_COL).cast("double"), F.lit(0.0)))
        .withColumn(ARR_DELAY_COL, F.col(ARR_DELAY_COL).cast("double"))
    )


# ============================================================
# Table 3: flights_historical_enriched
# ============================================================

@dp.table(
    comment="Route/carrier delay aggregates. Same table name that "
            "04_create_uc_functions.py's get_historical_delay_stats UC "
            "Function reads from — replaces the placeholder rows with "
            "real BTS-derived stats, no changes needed to the function "
            "or its MCP exposure."
)
def flights_historical_enriched():
    cleaned = dp.read("bts_flights_cleaned")

    return (
        cleaned
        .filter(F.col(CANCELLED_COL) == 0.0)  # exclude cancelled flights from delay stats
        .withColumn("route", F.concat_ws("-", F.col(ORIGIN_COL), F.col(DEST_COL)))
        .withColumn("is_on_time", (F.col(ARR_DELAY_COL) <= 15).cast("int"))
        .groupBy(F.col("route"), F.col(CARRIER_COL).alias("carrier"))
        .agg(
            F.round(F.avg(ARR_DELAY_COL), 1).alias("avg_delay_minutes"),
            F.round(F.avg("is_on_time") * 100, 1).alias("on_time_pct"),
            F.count("*").cast("int").alias("sample_size"),
        )
        .filter(F.col("sample_size") >= 10)  # drop routes/carriers with too small a sample
    )