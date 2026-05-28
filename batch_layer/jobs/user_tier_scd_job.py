from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame, SparkSession

SOURCE_VIEW_NAME = "user_events"
TARGET_TABLE_NAME = "dim_user_tier_scd"
REQUIRED_COLUMNS = ("user_id", "event_date", "membership_tier")

SCD_TYPE_2_QUERY = f"""
WITH streak_started AS (
    SELECT
        user_id,
        event_date,
        membership_tier,
        LAG(membership_tier, 1) OVER (
            PARTITION BY user_id
            ORDER BY event_date
        ) AS previous_membership_tier
    FROM {SOURCE_VIEW_NAME}
),
streak_identified AS (
    SELECT
        user_id,
        event_date,
        membership_tier,
        SUM(
            CASE
                WHEN previous_membership_tier IS NULL
                     OR previous_membership_tier <> membership_tier
                THEN 1
                ELSE 0
            END
        ) OVER (
            PARTITION BY user_id
            ORDER BY event_date
        ) AS streak_identifier
    FROM streak_started
),
aggregated AS (
    SELECT
        user_id,
        membership_tier,
        streak_identifier,
        MIN(event_date) AS start_date
    FROM streak_identified
    GROUP BY user_id, membership_tier, streak_identifier
)
SELECT
    user_id,
    membership_tier,
    start_date,
    LEAD(start_date) OVER (
        PARTITION BY user_id 
        ORDER BY start_date
    ) AS end_date,
    CASE 
        WHEN LEAD(start_date) OVER (PARTITION BY user_id ORDER BY start_date) IS NULL THEN True 
        ELSE False 
    END AS is_current
FROM aggregated
ORDER BY user_id, start_date
"""


def _validate_required_columns(dataframe: DataFrame, required_columns: Iterable[str]) -> None:
    missing_columns = [column for column in required_columns if column not in dataframe.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")


def build_user_tier_scd(dataframe: DataFrame) -> DataFrame:
    """
    Build SCD Type 2 timeline for membership_tier changes.

    Output columns:
    - user_id
    - membership_tier
    - start_date
    - end_date
    """
    _validate_required_columns(dataframe, REQUIRED_COLUMNS)
    spark = dataframe.sparkSession
    dataframe.createOrReplaceTempView(SOURCE_VIEW_NAME)
    return spark.sql(SCD_TYPE_2_QUERY)


def write_user_tier_scd(
    spark: SparkSession,
    source_events_df: DataFrame,
    target_table_name: str = TARGET_TABLE_NAME,
) -> None:
    """
    Transform user events to SCD Type 2 and persist to a target table.
    """
    result_df = build_user_tier_scd(source_events_df)
    result_df.write.mode("overwrite").saveAsTable(target_table_name)
