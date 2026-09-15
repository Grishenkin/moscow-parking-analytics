
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


TAXI_PATH = "data/silver/staging/taxi_parking"
INTERCEPT_PATH = "data/silver/staging/intercept_parking"
PAID_PATH = "data/silver/staging/paid_parking"

OUTPUT_PATH = "data/silver/parking_objects"


def get_spark():
    return (
        SparkSession.builder
        .appName("MoscowParkingUnifiedDataset")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def validate_data(df):
    total_rows = df.count()

    # Обязательные поля
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

    null_conditions = [
        F.col(column).isNull()
        for column in required_columns
    ]

    null_condition = null_conditions[0]

    for condition in null_conditions[1:]:
        null_condition = null_condition | condition

    null_rows = df.filter(null_condition).count()

    # Дубли
    duplicate_keys = (
        df.groupBy("global_id", "object_type")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    # Некорректные координаты
    invalid_coordinates = (
        df.filter(
            ~(
                F.col("longitude").between(-180, 180)
                & F.col("latitude").between(-90, 90)
            )
        )
        .count()
    )

    # Вместимость должна быть положительной
    invalid_capacity = (
        df.filter(F.col("capacity") <= 0)
        .count()
    )

    print("=== DATA QUALITY ===")
    print("Всего строк:", total_rows)
    print("Строк с NULL в обязательных полях:", null_rows)
    print("Дублирующихся ключей:", duplicate_keys)
    print("Некорректных координат:", invalid_coordinates)
    print("Некорректной capacity:", invalid_capacity)

    if null_rows > 0:
        raise ValueError(
            "Data Quality failed: NULL in required columns"
        )

    if duplicate_keys > 0:
        raise ValueError(
            "Data Quality failed: duplicate keys"
        )

    if invalid_coordinates > 0:
        raise ValueError(
            "Data Quality failed: invalid coordinates"
        )

    if invalid_capacity > 0:
        raise ValueError(
            "Data Quality failed: invalid capacity"
        )

    print("Data Quality checks passed ✓")


def main():
    spark = get_spark()

    taxi = spark.read.parquet(TAXI_PATH)
    intercept = spark.read.parquet(INTERCEPT_PATH)
    paid = spark.read.parquet(PAID_PATH)

    parking_objects = (
        taxi
        .unionByName(intercept)
        .unionByName(paid)
    )

    print("=== SOURCE COUNTS ===")
    print("Taxi:", taxi.count())
    print("Intercept:", intercept.count())
    print("Paid:", paid.count())

    print("\n=== UNIFIED DATASET ===")
    print("Всего объектов:", parking_objects.count())

    validate_data(parking_objects)

    parking_objects.write \
        .mode("overwrite") \
        .partitionBy("object_type") \
        .parquet(OUTPUT_PATH)

    print("\nSilver dataset saved:")
    print(OUTPUT_PATH)


if __name__ == "__main__":
    main()
