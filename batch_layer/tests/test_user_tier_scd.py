from __future__ import annotations

from collections import namedtuple

from chispa.dataframe_comparer import assert_df_equality

from batch_layer.jobs.user_tier_scd_job import build_user_tier_scd



UserEvent = namedtuple("UserEvent", "user_id event_date membership_tier")
UserTierScd = namedtuple("UserTierScd", "user_id membership_tier start_date end_date is_current")

def test_build_user_tier_scd_tracks_membership_upgrades(spark):
    source_data = [
        UserEvent("user_001", 20260528, "Normal"),
        UserEvent("user_001", 20260529, "Normal"),
        UserEvent("user_001", 20260530, "Silver"),
        UserEvent("user_001", 20260531, "Silver"),
        UserEvent("user_001", 20260602, "Gold"),
        UserEvent("user_001", 20260603, "Gold"),
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = build_user_tier_scd(source_df).orderBy("user_id", "start_date")

    expected_data = [
        UserTierScd("user_001", "Normal", 20260528, 20260530, False),
        UserTierScd("user_001", "Silver", 20260530, 20260602, False),
        UserTierScd("user_001", "Gold", 20260602, None, True), 
    ]
    expected_df = spark.createDataFrame(expected_data).orderBy("user_id", "start_date")

    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
