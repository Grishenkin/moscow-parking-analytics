
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SILVER_PATH = "data/silver/parking_objects"

DISTRICT_OUTPUT = "data/gold/district_summary"
ADM_AREA_OUTPUT = "data/gold/adm_area_summary"
OBJECT_TYPE_OUTPUT = "data/gold/object_type_summary"
PARKING_MAP_OUTPUT = "data/gold/parking_map"


def get_spark():
    return (
        SparkSession.builder
        .appName("MoscowParkingGoldMarts")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def build_district_summary(spark):
    return spark.sql("""
        SELECT
            adm_area,
            district,

            COUNT(*) AS total_objects,

            SUM(
                CASE
                    WHEN object_type = 'TAXI_PARKING' THEN 1
                    ELSE 0
                END
            ) AS taxi_parking_count,

            SUM(
                CASE
                    WHEN object_type = 'INTERCEPT_PARKING' THEN 1
                    ELSE 0
                END
            ) AS intercept_parking_count,

            SUM(
                CASE
                    WHEN object_type = 'PAID_PARKING' THEN 1
                    ELSE 0
                END
            ) AS paid_parking_count,

            SUM(capacity) AS total_capacity,

            ROUND(AVG(capacity), 2) AS avg_capacity

        FROM parking_objects

        GROUP BY
            adm_area,
            district
    """)


def build_adm_area_summary(spark):
    return spark.sql("""
        SELECT
            adm_area,

            COUNT(*) AS total_objects,

            COUNT(DISTINCT district) AS district_count,

            SUM(
                CASE
                    WHEN object_type = 'TAXI_PARKING' THEN 1
                    ELSE 0
                END
            ) AS taxi_parking_count,

            SUM(
                CASE
                    WHEN object_type = 'INTERCEPT_PARKING' THEN 1
                    ELSE 0
                END
            ) AS intercept_parking_count,

            SUM(
                CASE
                    WHEN object_type = 'PAID_PARKING' THEN 1
                    ELSE 0
                END
            ) AS paid_parking_count,

            SUM(capacity) AS total_capacity,

            ROUND(AVG(capacity), 2) AS avg_capacity

        FROM parking_objects

        GROUP BY adm_area
    """)


def build_object_type_summary(spark):
    return spark.sql("""
        SELECT
            object_type,

            COUNT(*) AS object_count,

            SUM(capacity) AS total_capacity,

            ROUND(AVG(capacity), 2) AS avg_capacity,

            MIN(capacity) AS min_capacity,

            MAX(capacity) AS max_capacity

        FROM parking_objects

        GROUP BY object_type
    """)


def build_parking_map(spark):
    return spark.sql("""
        SELECT
            global_id,
            object_type,
            parking_name,
            adm_area,
            district,
            address,
            longitude,
            latitude,
            capacity

        FROM parking_objects

        WHERE
            longitude IS NOT NULL
            AND latitude IS NOT NULL
    """)


def validate_marts(
    parking_objects,
    district_summary,
    adm_area_summary,
    object_type_summary,
    parking_map,
):
    silver_total = parking_objects.count()

    district_total = (
        district_summary
        .agg(F.sum("total_objects").alias("total"))
        .first()["total"]
    )

    adm_area_total = (
        adm_area_summary
        .agg(F.sum("total_objects").alias("total"))
        .first()["total"]
    )

    object_type_total = (
        object_type_summary
        .agg(F.sum("object_count").alias("total"))
        .first()["total"]
    )

    map_total = parking_map.count()

    print("=== GOLD DATA QUALITY ===")
    print("Silver:", silver_total)
    print("District mart:", district_total)
    print("Adm area mart:", adm_area_total)
    print("Object type mart:", object_type_total)
    print("Parking map:", map_total)

    if district_total != silver_total:
        raise ValueError("district_summary total mismatch")

    if adm_area_total != silver_total:
        raise ValueError("adm_area_summary total mismatch")

    if object_type_total != silver_total:
        raise ValueError("object_type_summary total mismatch")

    if map_total != silver_total:
        raise ValueError("parking_map total mismatch")

    print("Gold Data Quality checks passed ✓")


def main():
    spark = get_spark()

    parking_objects = spark.read.parquet(SILVER_PATH)

    parking_objects.createOrReplaceTempView(
        "parking_objects"
    )

    district_summary = build_district_summary(spark)
    adm_area_summary = build_adm_area_summary(spark)
    object_type_summary = build_object_type_summary(spark)
    parking_map = build_parking_map(spark)

    validate_marts(
        parking_objects,
        district_summary,
        adm_area_summary,
        object_type_summary,
        parking_map,
    )

    district_summary.write \
        .mode("overwrite") \
        .parquet(DISTRICT_OUTPUT)

    adm_area_summary.write \
        .mode("overwrite") \
        .parquet(ADM_AREA_OUTPUT)

    object_type_summary.write \
        .mode("overwrite") \
        .parquet(OBJECT_TYPE_OUTPUT)

    parking_map.write \
        .mode("overwrite") \
        .parquet(PARKING_MAP_OUTPUT)

    print("\n=== GOLD MARTS SAVED ===")
    print(DISTRICT_OUTPUT)
    print(ADM_AREA_OUTPUT)
    print(OBJECT_TYPE_OUTPUT)
    print(PARKING_MAP_OUTPUT)


if __name__ == "__main__":
    main()
