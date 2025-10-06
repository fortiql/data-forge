"""Date dimension builder."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import DateType

from silver.common import surrogate_key


def build_dim_date(spark: SparkSession, _: DataFrame | None) -> DataFrame:
    """Build date dimension with standard date attributes."""
    start_date = "2020-01-01"
    end_date = "2030-12-31"
    dates_df = (spark.sql(f"""
        SELECT explode(sequence(
            to_date('{start_date}'), 
            to_date('{end_date}'), 
            interval 1 day
        )) as date_key
    """))
    return (dates_df
        .withColumn("date_sk", surrogate_key(F.col("date_key")))
        .withColumn("year", F.year("date_key"))
        .withColumn("quarter", F.quarter("date_key"))
        .withColumn("month", F.month("date_key"))
        .withColumn("day", F.dayofmonth("date_key"))
        .withColumn("day_of_week", F.dayofweek("date_key"))
        .withColumn("day_of_year", F.dayofyear("date_key"))
        .withColumn("week_of_year", F.weekofyear("date_key"))
        .withColumn("month_name", F.date_format("date_key", "MMMM"))
        .withColumn("day_name", F.date_format("date_key", "EEEE"))
        .withColumn("is_weekend", F.col("day_of_week").isin([1, 7]))  # Sunday=1, Saturday=7
        .withColumn("is_month_start", F.col("day") == 1)
        .withColumn("is_month_end", F.col("date_key") == F.last_day("date_key"))
        .withColumn("is_quarter_start", F.col("day_of_year").isin([1, 91, 182, 274]))
        .withColumn("is_year_start", F.col("day_of_year") == 1)
        .withColumn("is_year_end", F.col("day_of_year") == F.dayofyear(F.date_add(F.trunc("date_key", "year"), 364)))
        .withColumn("processed_at", F.current_timestamp())
        .select(
            "date_sk",
            "date_key", 
            "year",
            "quarter",
            "month",
            "day",
            "day_of_week",
            "day_of_year", 
            "week_of_year",
            "month_name",
            "day_name",
            "is_weekend",
            "is_month_start",
            "is_month_end",
            "is_quarter_start", 
            "is_year_start",
            "is_year_end",
            "processed_at",
        )
    )