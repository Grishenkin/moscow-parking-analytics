
from src.jobs.build_marts import build_district_summary


def test_build_district_summary(spark):

    source_data = [
        (
            "Центральный административный округ",
            "район Арбат",
            "PAID_PARKING",
            10,
        ),
        (
            "Центральный административный округ",
            "район Арбат",
            "PAID_PARKING",
            20,
        ),
        (
            "Центральный административный округ",
            "район Арбат",
            "TAXI_PARKING",
            2,
        ),
        (
            "Центральный административный округ",
            "район Хамовники",
            "INTERCEPT_PARKING",
            100,
        ),
    ]

    source_df = spark.createDataFrame(
        source_data,
        [
            "adm_area",
            "district",
            "object_type",
            "capacity",
        ],
    )

    source_df.createOrReplaceTempView(
        "parking_objects"
    )

    result = build_district_summary(spark)

    rows = {
        row["district"]: row
        for row in result.collect()
    }

    # -----------------------------
    # Арбат
    # -----------------------------

    arbat = rows["район Арбат"]

    assert arbat.total_objects == 3

    assert arbat.taxi_parking_count == 1
    assert arbat.intercept_parking_count == 0
    assert arbat.paid_parking_count == 2

    assert arbat.total_capacity == 32
    assert arbat.avg_capacity == 10.67

    # -----------------------------
    # Хамовники
    # -----------------------------

    hamovniki = rows["район Хамовники"]

    assert hamovniki.total_objects == 1

    assert hamovniki.taxi_parking_count == 0
    assert hamovniki.intercept_parking_count == 1
    assert hamovniki.paid_parking_count == 0

    assert hamovniki.total_capacity == 100
    assert hamovniki.avg_capacity == 100.0

    # Всего должно получиться два района
    assert result.count() == 2


from src.jobs.build_marts import build_object_type_summary


def test_build_object_type_summary(spark):

    source_data = [
        ("PAID_PARKING", 10),
        ("PAID_PARKING", 20),
        ("PAID_PARKING", 30),
        ("TAXI_PARKING", 2),
        ("TAXI_PARKING", 4),
        ("INTERCEPT_PARKING", 100),
    ]

    source_df = spark.createDataFrame(
        source_data,
        [
            "object_type",
            "capacity",
        ],
    )

    source_df.createOrReplaceTempView(
        "parking_objects"
    )

    result = build_object_type_summary(spark)

    rows = {
        row["object_type"]: row
        for row in result.collect()
    }

    paid = rows["PAID_PARKING"]

    assert paid.object_count == 3
    assert paid.total_capacity == 60
    assert paid.avg_capacity == 20.0
    assert paid.min_capacity == 10
    assert paid.max_capacity == 30

    taxi = rows["TAXI_PARKING"]

    assert taxi.object_count == 2
    assert taxi.total_capacity == 6
    assert taxi.avg_capacity == 3.0
    assert taxi.min_capacity == 2
    assert taxi.max_capacity == 4

    intercept = rows["INTERCEPT_PARKING"]

    assert intercept.object_count == 1
    assert intercept.total_capacity == 100
    assert intercept.avg_capacity == 100.0
    assert intercept.min_capacity == 100
    assert intercept.max_capacity == 100

    assert result.count() == 3
