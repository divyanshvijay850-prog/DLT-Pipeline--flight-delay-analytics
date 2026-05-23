"""
╔══════════════════════════════════════════════════════════════╗
║   US Domestic Flight Delay Analytics — Complete DLT Pipeline ║
║   Bronze → Silver → Gold                                     ║
║   Stack: Databricks DLT | Apache Spark | Delta Lake | S3     ║
╚══════════════════════════════════════════════════════════════╝
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType,
    BooleanType, IntegerType
)

# ══════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════

SOURCE_PATH    = "s3://divyansh-flight-delay-2026/flights/year=2021/month=01/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2021_1.parquet"
AIRPORT_S3_PATH = "s3://divyansh-flight-delay-2026/raw/airports/airports.json"


# ══════════════════════════════════════════════════════════════
#  BRONZE LAYER  (original code — untouched)
# ══════════════════════════════════════════════════════════════

flight_schema_36 = StructType([

    StructField("FlightDate",                       StringType(), True),

    StructField("Reporting_Airline",                StringType(), True),
    StructField("IATA_CODE_Reporting_Airline",       StringType(), True),
    StructField("Tail_Number",                      StringType(), True),
    StructField("Flight_Number_Reporting_Airline",  LongType(),   True),

    StructField("Origin",                           StringType(), True),
    StructField("OriginCityName",                   StringType(), True),
    StructField("OriginState",                      StringType(), True),

    StructField("Dest",                             StringType(), True),
    StructField("DestCityName",                     StringType(), True),
    StructField("DestState",                        StringType(), True),

    StructField("CRSDepTime",                       LongType(),   True),
    StructField("DepTime",                          DoubleType(), True),
    StructField("DepDelay",                         DoubleType(), True),
    StructField("DepDelayMinutes",                  DoubleType(), True),
    StructField("DepDel15",                         DoubleType(), True),
    StructField("DepartureDelayGroups",             DoubleType(), True),

    StructField("CRSArrTime",                       LongType(),   True),
    StructField("ArrTime",                          DoubleType(), True),
    StructField("ArrDelay",                         DoubleType(), True),
    StructField("ArrDelayMinutes",                  DoubleType(), True),
    StructField("ArrivalDelayGroups",               DoubleType(), True),

    StructField("Cancelled",                        DoubleType(), True),
    StructField("CancellationCode",                 StringType(), True),
    StructField("Diverted",                         DoubleType(), True),

    StructField("CRSElapsedTime",                   DoubleType(), True),
    StructField("ActualElapsedTime",                DoubleType(), True),
    StructField("AirTime",                          DoubleType(), True),

    StructField("Flights",                          DoubleType(), True),
    StructField("Distance",                         DoubleType(), True),
    StructField("DistanceGroup",                    LongType(),   True),

    StructField("CarrierDelay",                     DoubleType(), True),
    StructField("WeatherDelay",                     DoubleType(), True),
    StructField("NASDelay",                         DoubleType(), True),
    StructField("SecurityDelay",                    DoubleType(), True),
    StructField("LateAircraftDelay",                DoubleType(), True),
])


@dlt.table(
    name="bronze_flightes",
    comment="Bronze layer: parquet read with required 36-column schema",
    table_properties={"quality": "bronze"}
)
def bronze_flightes():

    df_raw = (
        spark.read
        .schema(flight_schema_36)
        .parquet(SOURCE_PATH)
    )

    return (
        df_raw
        .withColumn("source_file",    F.col("_metadata.file_path").cast("string"))
        .withColumn("ingestion_time", F.current_timestamp())
    )


# ══════════════════════════════════════════════════════════════
#  SILVER LAYER
#  Deduplicated | Typed | Delay-bucketed | Airport-enriched
# ══════════════════════════════════════════════════════════════

@dlt.table(
    name="silver_flights",
    comment="Silver layer: cleaned, typed, deduplicated, airport-enriched flight data",
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true"
    }
)
@dlt.expect("valid_flight_date", "flight_date IS NOT NULL")
@dlt.expect("valid_airline",     "airline_code IS NOT NULL")
@dlt.expect("valid_origin",      "origin_code IS NOT NULL")
@dlt.expect("valid_dest",        "dest_code IS NOT NULL")
@dlt.expect("origin_ne_dest",    "origin_code != dest_code")
@dlt.expect("valid_distance",    "distance IS NULL OR (distance >= 1 AND distance <= 10000)")
def silver_flights():

    df = dlt.read("bronze_flightes")

    # ── 1. Rename columns → clean snake_case ──────────────────
    df = (
        df
        .withColumnRenamed("Reporting_Airline",                "airline_code")
        .withColumnRenamed("Tail_Number",                      "tail_number")
        .withColumnRenamed("Flight_Number_Reporting_Airline",  "flight_number")
        .withColumnRenamed("Origin",                           "origin_code")
        .withColumnRenamed("OriginCityName",                   "origin_city")
        .withColumnRenamed("OriginState",                      "origin_state")
        .withColumnRenamed("Dest",                             "dest_code")
        .withColumnRenamed("DestCityName",                     "dest_city")
        .withColumnRenamed("DestState",                        "dest_state")
        .withColumnRenamed("IATA_CODE_Reporting_Airline",      "iata_code")
    )

    # ── 2. FlightDate STRING → DATE + time columns ────────────
    df = (
        df
        .withColumn("flight_date",        F.to_date(F.col("FlightDate"), "yyyy-MM-dd"))
        .withColumn("flight_year",        F.year("flight_date"))
        .withColumn("flight_month",       F.month("flight_date"))
        .withColumn("flight_day_of_week", F.dayofweek("flight_date"))   # 1=Sun … 7=Sat
        .withColumn("year_month",         F.date_format("flight_date", "yyyy-MM"))
        .drop("FlightDate")
    )

    # ── 3. HHMM integer → minutes since midnight ──────────────
    #   e.g.  1430  →  14*60 + 30 = 870 min
    def hhmm_to_min(c):
        return (
            (F.col(c) / 100).cast(IntegerType()) * 60
            + (F.col(c) % 100)
        ).cast(IntegerType())

    df = (
        df
        .withColumn("crs_dep_min", hhmm_to_min("CRSDepTime"))
        .withColumn("crs_arr_min", hhmm_to_min("CRSArrTime"))
        .drop("CRSDepTime", "CRSArrTime", "DepTime", "ArrTime")
    )

    # ── 4. Boolean casts ──────────────────────────────────────
    df = (
        df
        .withColumn("is_cancelled",  F.col("Cancelled").cast(BooleanType()))
        .withColumn("is_diverted",   F.col("Diverted").cast(BooleanType()))
        .withColumn("is_dep_del15",  F.col("DepDel15").cast(BooleanType()))
        .withColumn("is_arr_del15",
            F.when(F.col("ArrDelay") > 15,          True)
             .when(F.col("ArrDelay").isNotNull(),    False)
             .otherwise(None))
        .drop("Cancelled", "Diverted", "DepDel15",
              "DepartureDelayGroups", "ArrivalDelayGroups")
    )

    # ── 5. Delay bucket ───────────────────────────────────────
    df = df.withColumn(
        "delay_bucket",
        F.when(F.col("is_cancelled") == True,  "CANCELLED")
         .when(F.col("ArrDelay") < 0,           "EARLY")
         .when(F.col("ArrDelay") <= 0,           "ON_TIME")
         .when(F.col("ArrDelay") <= 15,          "MINOR_DELAY")
         .when(F.col("ArrDelay") <= 60,          "MAJOR_DELAY")
         .otherwise("SEVERE_DELAY")
    )

    # ── 6. DQ flag: delay cause mismatch ─────────────────────
    cause_sum = (
        F.coalesce(F.col("CarrierDelay"),      F.lit(0)) +
        F.coalesce(F.col("WeatherDelay"),      F.lit(0)) +
        F.coalesce(F.col("NASDelay"),          F.lit(0)) +
        F.coalesce(F.col("SecurityDelay"),     F.lit(0)) +
        F.coalesce(F.col("LateAircraftDelay"), F.lit(0))
    )
    df = df.withColumn(
        "dq_delay_cause_mismatch",
        F.when(
            (F.col("is_cancelled") == False) &
            (F.col("ArrDelay") > 0) &
            (F.abs(cause_sum - F.col("ArrDelay")) > 5),
            True
        ).otherwise(False)
    )

    # ── 7. DQ flag: cancellation code mismatch ───────────────
    df = df.withColumn(
        "dq_cancel_code_mismatch",
        F.when(
            (F.col("is_cancelled") == True) & F.col("CancellationCode").isNull(), True
        ).when(
            (F.col("is_cancelled") == False) & F.col("CancellationCode").isNotNull(), True
        ).otherwise(False)
    )

    # ── 8. Deduplication (keep latest per natural key) ────────
    w_dedup = Window.partitionBy(
        "flight_date", "airline_code", "flight_number",
        "origin_code", "dest_code"
    ).orderBy(F.col("ingestion_time").desc())

    df = (
        df
        .withColumn("_rn", F.row_number().over(w_dedup))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # ── 9. Rename delay cols → snake_case ────────────────────
    rename_map = {
        "DepDelay":          "dep_delay",
        "DepDelayMinutes":   "dep_delay_min",
        "ArrDelay":          "arr_delay",
        "ArrDelayMinutes":   "arr_delay_min",
        "CarrierDelay":      "carrier_delay",
        "WeatherDelay":      "weather_delay",
        "NASDelay":          "nas_delay",
        "SecurityDelay":     "security_delay",
        "LateAircraftDelay": "late_aircraft_delay",
        "Distance":          "distance",
        "CRSElapsedTime":    "crs_elapsed_time",
        "ActualElapsedTime": "actual_elapsed_time",
        "AirTime":           "air_time",
        "CancellationCode":  "cancellation_code",
        "Flights":           "flights",
        "DistanceGroup":     "distance_group",
    }
    for old_col, new_col in rename_map.items():
        if old_col in df.columns:
            df = df.withColumnRenamed(old_col, new_col)

    # ── 10. Airport enrichment — broadcast join ───────────────
    #   F.trim() fixes hidden-space mismatch between iata & Origin values
    airport_ref = (
        spark.read.option("multiline", "true").json(AIRPORT_S3_PATH)
        .filter(
            F.col("iata").isNotNull() &
            (F.col("iata") != "\\N") &
            (F.col("iata") != "")
        )
        .withColumn("lat",       F.col("latitude").cast("double"))
        .withColumn("lon",       F.col("longitude").cast("double"))
        .withColumn("tz_offset", F.col("timezone").cast("double"))
        .filter(
            F.col("lat").between(-90,  90) &
            F.col("lon").between(-180, 180)
        )
        .select(
            F.trim(F.col("iata")).alias("_iata"),   # trim — key fix
            F.col("name").alias("_airport_name"),
            F.col("city").alias("_airport_city"),
            F.col("country").alias("_airport_country"),
            F.col("lat").alias("_lat"),
            F.col("lon").alias("_lon"),
            F.col("tz_offset").alias("_tz_offset"),
            F.col("tz_database").alias("_tz_database"),
        )
    )

    # Origin join
    origin_ref = F.broadcast(
        airport_ref.select(
            F.col("_iata").alias("origin_iata"),
            F.col("_airport_name").alias("origin_airport_name"),
            F.col("_airport_city").alias("origin_airport_city"),
            F.col("_airport_country").alias("origin_airport_country"),
            F.col("_lat").alias("origin_lat"),
            F.col("_lon").alias("origin_lon"),
            F.col("_tz_offset").alias("origin_tz_offset"),
            F.col("_tz_database").alias("origin_tz_database"),
        )
    )
    df = df.join(
        origin_ref,
        F.trim(df["origin_code"]) == F.col("origin_iata"),
        how="left"
    ).drop("origin_iata")

    # Destination join
    dest_ref = F.broadcast(
        airport_ref.select(
            F.col("_iata").alias("dest_iata"),
            F.col("_airport_name").alias("dest_airport_name"),
            F.col("_airport_city").alias("dest_airport_city"),
            F.col("_airport_country").alias("dest_airport_country"),
            F.col("_lat").alias("dest_lat"),
            F.col("_lon").alias("dest_lon"),
            F.col("_tz_offset").alias("dest_tz_offset"),
            F.col("_tz_database").alias("dest_tz_database"),
        )
    )
    df = df.join(
        dest_ref,
        F.trim(df["dest_code"]) == F.col("dest_iata"),
        how="left"
    ).drop("dest_iata")

    # DQ flags for unknown airport codes
    df = (
        df
        .withColumn("dq_unknown_origin_airport", F.col("origin_lat").isNull())
        .withColumn("dq_unknown_dest_airport",   F.col("dest_lat").isNull())
    )

    return df


# ══════════════════════════════════════════════════════════════
#  GOLD LAYER — Table 1: airline_monthly_kpis
#  Core KPI fact table  |  Power BI scorecard ka source
# ══════════════════════════════════════════════════════════════

@dlt.table(
    name="gold_airline_monthly_kpis",
    comment="Gold: On-time %, avg/P50/P95 delay, cancellation rate — per airline per month",
    table_properties={"quality": "gold"}
)
def gold_airline_monthly_kpis():

    df = dlt.read("silver_flights")

    agg = df.groupBy(
        "year_month", "flight_year", "flight_month", "airline_code"
    ).agg(

        F.count("*").alias("total_flights"),
        F.sum(F.col("flights")).alias("total_flight_ops"),

        # Cancellations
        F.sum(F.when(F.col("is_cancelled") == True, 1).otherwise(0))
         .alias("cancelled_flights"),

        # Delays >15 min (non-cancelled only)
        F.sum(
            F.when(
                (F.col("is_cancelled") == False) & (F.col("is_arr_del15") == True), 1
            ).otherwise(0)
        ).alias("delayed_flights"),

        F.avg(F.when(F.col("is_cancelled") == False, F.col("arr_delay")))
         .alias("avg_arr_delay_min"),

        F.avg(F.when(F.col("is_cancelled") == False, F.col("dep_delay")))
         .alias("avg_dep_delay_min"),

        F.percentile_approx(
            F.when(F.col("is_cancelled") == False, F.col("arr_delay")), 0.50
        ).alias("p50_arr_delay"),

        F.percentile_approx(
            F.when(F.col("is_cancelled") == False, F.col("arr_delay")), 0.95
        ).alias("p95_arr_delay"),

        # Delay cause totals (minutes)
        F.sum("carrier_delay").alias("total_carrier_delay_min"),
        F.sum("weather_delay").alias("total_weather_delay_min"),
        F.sum("nas_delay").alias("total_nas_delay_min"),
        F.sum("security_delay").alias("total_security_delay_min"),
        F.sum("late_aircraft_delay").alias("total_late_aircraft_delay_min"),

        # Delay bucket counts
        F.sum(F.when(F.col("delay_bucket") == "EARLY",        1).otherwise(0)).alias("early_flights"),
        F.sum(F.when(F.col("delay_bucket") == "ON_TIME",      1).otherwise(0)).alias("on_time_flights"),
        F.sum(F.when(F.col("delay_bucket") == "MINOR_DELAY",  1).otherwise(0)).alias("minor_delay_flights"),
        F.sum(F.when(F.col("delay_bucket") == "MAJOR_DELAY",  1).otherwise(0)).alias("major_delay_flights"),
        F.sum(F.when(F.col("delay_bucket") == "SEVERE_DELAY", 1).otherwise(0)).alias("severe_delay_flights"),
    )

    agg = (
        agg
        .withColumn("on_time_pct",
            F.round(
                (F.col("on_time_flights") + F.col("early_flights")) * 100.0
                / F.col("total_flights"), 2
            )
        )
        .withColumn("cancellation_rate_pct",
            F.round(F.col("cancelled_flights") * 100.0 / F.col("total_flights"), 2))
        .withColumn("delay_rate_pct",
            F.round(F.col("delayed_flights") * 100.0 / F.col("total_flights"), 2))
    )

    # Monthly rank by on-time % (1 = best airline that month)
    w_rank = Window.partitionBy("year_month").orderBy(F.col("on_time_pct").desc())
    agg = agg.withColumn("monthly_rank", F.rank().over(w_rank))

    return agg.orderBy("year_month", "monthly_rank")


# ══════════════════════════════════════════════════════════════
#  GOLD — Table 2: route_annual_performance
#  Normalised OD pairs  (ATL→LAX == LAX→ATL = one route)
# ══════════════════════════════════════════════════════════════

@dlt.table(
    name="gold_route_annual_performance",
    comment="Gold: Annual OD-pair route performance (A→B and B→A merged as one route)",
    table_properties={"quality": "gold"}
)
def gold_route_annual_performance():

    df = dlt.read("silver_flights")

    # Normalise: smaller IATA alphabetically = route_origin
    df = (
        df
        .withColumn("route_origin",
            F.when(F.col("origin_code") < F.col("dest_code"), F.col("origin_code"))
             .otherwise(F.col("dest_code")))
        .withColumn("route_dest",
            F.when(F.col("origin_code") < F.col("dest_code"), F.col("dest_code"))
             .otherwise(F.col("origin_code")))
        .withColumn("route_key",
            F.concat_ws("→", F.col("route_origin"), F.col("route_dest")))
    )

    return (
        df.groupBy("flight_year", "route_key", "route_origin", "route_dest").agg(
            F.count("*").alias("total_flights"),
            F.sum(F.when(F.col("is_cancelled") == True, 1).otherwise(0))
             .alias("cancelled_flights"),
            F.avg(F.when(F.col("is_cancelled") == False, F.col("arr_delay")))
             .alias("avg_arr_delay_min"),
            F.percentile_approx(
                F.when(F.col("is_cancelled") == False, F.col("arr_delay")), 0.95
            ).alias("p95_arr_delay"),
            F.avg("distance").alias("avg_distance_miles"),
            F.round(
                F.sum(F.when(
                    (F.col("is_cancelled") == False) & (F.col("is_arr_del15") == True), 1
                ).otherwise(0)) * 100.0 / F.count("*"), 2
            ).alias("delay_rate_pct"),
            F.round(
                F.sum(F.when(F.col("is_cancelled") == True, 1).otherwise(0)) * 100.0
                / F.count("*"), 2
            ).alias("cancellation_rate_pct"),
        )
        .orderBy("flight_year", F.col("total_flights").desc())
    )


# ══════════════════════════════════════════════════════════════
#  GOLD — Table 3: airport_monthly_performance
#  Hub-level departure metrics  |  Power BI map visual ka source
# ══════════════════════════════════════════════════════════════

@dlt.table(
    name="gold_airport_monthly_performance",
    comment="Gold: Monthly airport departure metrics with lat/lon for map visual",
    table_properties={"quality": "gold"}
)
def gold_airport_monthly_performance():

    df = dlt.read("silver_flights")

    return (
        df.groupBy(
            "year_month", "flight_year", "flight_month",
            "origin_code", "origin_city", "origin_state",
            "origin_lat", "origin_lon"
        ).agg(
            F.count("*").alias("total_departures"),
            F.sum(F.when(F.col("is_cancelled") == True, 1).otherwise(0))
             .alias("cancelled_departures"),
            F.avg(F.when(F.col("is_cancelled") == False, F.col("dep_delay")))
             .alias("avg_dep_delay_min"),
            F.avg(F.when(F.col("is_cancelled") == False, F.col("arr_delay")))
             .alias("avg_arr_delay_min"),
            F.percentile_approx(
                F.when(F.col("is_cancelled") == False, F.col("arr_delay")), 0.95
            ).alias("p95_arr_delay"),
            F.round(
                F.sum(F.when(F.col("is_cancelled") == True, 1).otherwise(0)) * 100.0
                / F.count("*"), 2
            ).alias("cancellation_rate_pct"),
            F.round(
                F.sum(F.when(
                    (F.col("is_cancelled") == False) & (F.col("is_arr_del15") == True), 1
                ).otherwise(0)) * 100.0 / F.count("*"), 2
            ).alias("delay_rate_pct"),
            F.countDistinct("airline_code").alias("airlines_operating"),
            F.countDistinct("dest_code").alias("unique_destinations"),
        )
        .orderBy("year_month", F.col("total_departures").desc())
    )


# ══════════════════════════════════════════════════════════════
#  GOLD — Table 4: delay_cause_analysis
#  % breakdown of delay minutes by cause  |  Waterfall chart
# ══════════════════════════════════════════════════════════════

@dlt.table(
    name="gold_delay_cause_analysis",
    comment="Gold: % breakdown of delay causes per airline per month",
    table_properties={"quality": "gold"}
)
def gold_delay_cause_analysis():

    df = dlt.read("silver_flights")

    # Only delayed, non-cancelled flights
    delayed = df.filter(
        (F.col("is_cancelled") == False) & (F.col("arr_delay") > 0)
    )

    agg = delayed.groupBy(
        "year_month", "flight_year", "flight_month", "airline_code"
    ).agg(
        F.count("*").alias("delayed_flights"),
        F.sum("carrier_delay").alias("carrier_delay_min"),
        F.sum("weather_delay").alias("weather_delay_min"),
        F.sum("nas_delay").alias("nas_delay_min"),
        F.sum("security_delay").alias("security_delay_min"),
        F.sum("late_aircraft_delay").alias("late_aircraft_delay_min"),
        F.sum("arr_delay").alias("total_delay_min"),
    )

    agg = (
        agg
        .withColumn("total_cause_min",
            F.coalesce(F.col("carrier_delay_min"),       F.lit(0)) +
            F.coalesce(F.col("weather_delay_min"),       F.lit(0)) +
            F.coalesce(F.col("nas_delay_min"),           F.lit(0)) +
            F.coalesce(F.col("security_delay_min"),      F.lit(0)) +
            F.coalesce(F.col("late_aircraft_delay_min"), F.lit(0))
        )
        .withColumn("carrier_pct",
            F.round(F.col("carrier_delay_min")       * 100.0 / F.col("total_cause_min"), 2))
        .withColumn("weather_pct",
            F.round(F.col("weather_delay_min")       * 100.0 / F.col("total_cause_min"), 2))
        .withColumn("nas_pct",
            F.round(F.col("nas_delay_min")           * 100.0 / F.col("total_cause_min"), 2))
        .withColumn("security_pct",
            F.round(F.col("security_delay_min")      * 100.0 / F.col("total_cause_min"), 2))
        .withColumn("late_aircraft_pct",
            F.round(F.col("late_aircraft_delay_min") * 100.0 / F.col("total_cause_min"), 2))

        # Primary cause — greatest() on scalars, then CASE WHEN map to label
        .withColumn("_max_pct",
            F.greatest(
                F.coalesce(F.col("carrier_pct"),       F.lit(0.0)),
                F.coalesce(F.col("weather_pct"),       F.lit(0.0)),
                F.coalesce(F.col("nas_pct"),           F.lit(0.0)),
                F.coalesce(F.col("security_pct"),      F.lit(0.0)),
                F.coalesce(F.col("late_aircraft_pct"), F.lit(0.0)),
            )
        )
        .withColumn("primary_cause",
            F.when(F.col("_max_pct") == F.col("carrier_pct"),       F.lit("CARRIER"))
             .when(F.col("_max_pct") == F.col("weather_pct"),       F.lit("WEATHER"))
             .when(F.col("_max_pct") == F.col("nas_pct"),           F.lit("NAS"))
             .when(F.col("_max_pct") == F.col("security_pct"),      F.lit("SECURITY"))
             .otherwise(F.lit("LATE_AIRCRAFT"))
        )
        .drop("_max_pct")
    )

    return agg.orderBy("year_month", "airline_code")
