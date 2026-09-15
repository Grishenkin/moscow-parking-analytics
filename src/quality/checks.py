
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SILVER_PATH = "data/silver/parking_objects"

DISTRICT_PATH = "data/gold/district_summary"
ADM_AREA_PATH = "data/gold/adm_area_summary"
OBJECT_TYPE_PATH = "data/gold/object_type_summary"
PARKING_MAP_PATH = "data/gold/parking_map"


def get_spark():
    return (
        SparkSession.builder
        .appName("MoscowParkingQualityChecks")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def check_silver(df):
    print("=== SILVER QUALITY CHECKS ===")

    total_rows = df.count()

    required_columns = [
        "global_id",
        "object_type",
        "parking_name",
        "adm_area",
        "district",
        "longitude",
        "latitude",
        "capacity",
    ]

    # NULL в обязательных полях
    null_condition = F.col(required_columns[0]).isNull()

    for column in required_columns[1:]:
        null_condition = (
            null_condition
            | F.col(column).isNull()
        )

    null_rows = df.filter(null_condition).count()

    # Дубли
    duplicate_keys = (
        df
        .groupBy("global_id", "object_type")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    # Координаты
    invalid_coordinates = (
        df
        .filter(
            ~(
                F.col("longitude").between(-180, 180)
                & F.col("latitude").between(-90, 90)
            )
        )
        .count()
    )

    # Вместимость
    invalid_capacity = (
        df
        .filter(F.col("capacity") <= 0)
        .count()
    )

    # Проверка типов объектов
    actual_types = {
        row["object_type"]
        for row in (
            df
            .select("object_type")
            .distinct()
            .collect()
        )
    }

    expected_types = {
        "TAXI_PARKING",
        "INTERCEPT_PARKING",
        "PAID_PARKING",
    }

    print("Количество объектов:", total_rows)
    print("NULL в обязательных полях:", null_rows)
    print("Дублирующихся ключей:", duplicate_keys)
    print("Некорректных координат:", invalid_coordinates)
    print("Некорректной capacity:", invalid_capacity)
    print("Типы объектов:", sorted(actual_types))

    if total_rows == 0:
        raise ValueError(
            "Silver quality failed: dataset is empty"
        )

    if null_rows > 0:
        raise ValueError(
            "Silver quality failed: NULL values"
        )

    if duplicate_keys > 0:
        raise ValueError(
            "Silver quality failed: duplicate keys"
        )

    if invalid_coordinates > 0:
        raise ValueError(
            "Silver quality failed: invalid coordinates"
        )

    if invalid_capacity > 0:
        raise ValueError(
            "Silver quality failed: invalid capacity"
        )

    if actual_types != expected_types:
        raise ValueError(
            "Silver quality failed: unexpected object types"
        )

    print("Silver checks passed ✓")

    return total_rows


def check_gold(
    silver,
    silver_total,
    district,
    adm_area,
    object_type,
    parking_map,
):
    print("\n=== GOLD QUALITY CHECKS ===")

    district_total = (
        district
        .agg(F.sum("total_objects").alias("total"))
        .first()["total"]
    )

    adm_area_total = (
        adm_area
        .agg(F.sum("total_objects").alias("total"))
        .first()["total"]
    )

    object_type_total = (
        object_type
        .agg(F.sum("object_count").alias("total"))
        .first()["total"]
    )

    map_total = parking_map.count()

    silver_capacity = (
        silver
        .agg(F.sum("capacity").alias("total"))
        .first()["total"]
    )

    gold_capacity = (
        object_type
        .agg(F.sum("total_capacity").alias("total"))
        .first()["total"]
    )

    print("Silver objects:", silver_total)
    print("District objects:", district_total)
    print("Adm area objects:", adm_area_total)
    print("Object type objects:", object_type_total)
    print("Map objects:", map_total)

    print(
        "Silver total capacity:",
        silver_capacity
    )

    print(
        "Gold total capacity:",
        gold_capacity
    )

    if district_total != silver_total:
        raise ValueError(
            "Gold quality failed: district total mismatch"
        )

    if adm_area_total != silver_total:
        raise ValueError(
            "Gold quality failed: adm_area total mismatch"
        )

    if object_type_total != silver_total:
        raise ValueError(
            "Gold quality failed: object_type total mismatch"
        )

    if map_total != silver_total:
        raise ValueError(
            "Gold quality failed: parking_map total mismatch"
        )

    if silver_capacity != gold_capacity:
        raise ValueError(
            "Gold quality failed: capacity mismatch"
        )

    print("Gold checks passed ✓")


def main():
    spark = get_spark()

    silver = spark.read.parquet(SILVER_PATH)

    district = spark.read.parquet(
        DISTRICT_PATH
    )

    adm_area = spark.read.parquet(
        ADM_AREA_PATH
    )

    object_type = spark.read.parquet(
        OBJECT_TYPE_PATH
    )

    parking_map = spark.read.parquet(
        PARKING_MAP_PATH
    )

    silver_total = check_silver(silver)

    check_gold(
        silver,
        silver_total,
        district,
        adm_area,
        object_type,
        parking_map,
    )

    print("\n==============================")
    print("ALL DATA QUALITY CHECKS PASSED ✓")
    print("==============================")


if __name__ == "__main__":
    main()
