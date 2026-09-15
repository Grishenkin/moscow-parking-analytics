
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


TAXI_PATH = "data/raw/api/taxi_parking/data.geojson"
INTERCEPT_PATH = "data/raw/api/intercept_parking/data.geojson"
PAID_PATH = "data/raw/api/paid_parking/data.geojson"

TAXI_OUTPUT = "data/silver/staging/taxi_parking"
INTERCEPT_OUTPUT = "data/silver/staging/intercept_parking"
PAID_OUTPUT = "data/silver/staging/paid_parking"


def get_spark():
    return (
        SparkSession.builder
        .appName("MoscowParkingNormalize")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_features(spark, path):
    """
    Читает GeoJSON FeatureCollection и возвращает
    по одной строке Spark DataFrame на каждый Feature.
    """
    raw = (
        spark.read
        .option("multiline", "true")
        .json(path)
    )

    return raw.select(
        F.explode("features").alias("feature")
    )


def normalize_taxi(df):
    return df.select(
        F.col(
            "feature.properties.attributes.global_id"
        ).cast("long").alias("global_id"),

        F.lit("TAXI_PARKING").alias("object_type"),

        F.trim(
            F.col("feature.properties.attributes.Name")
        ).alias("parking_name"),

        F.trim(
            F.col("feature.properties.attributes.AdmArea")
        ).alias("adm_area"),

        F.trim(
            F.col("feature.properties.attributes.District")
        ).alias("district"),

        F.trim(
            F.col("feature.properties.attributes.Address")
        ).alias("address"),

        F.col(
            "feature.geometry.coordinates"
        )[0].cast("double").alias("longitude"),

        F.col(
            "feature.geometry.coordinates"
        )[1].cast("double").alias("latitude"),

        F.col(
            "feature.properties.attributes.CarCapacity"
        ).cast("int").alias("capacity"),

        F.trim(
            F.col("feature.properties.attributes.Mode")
        ).alias("schedule"),

        F.lit("data_mos_621_api").alias("source"),

        F.current_date().alias("load_date"),
    )


def normalize_intercept(df):
    return df.select(
        F.col(
            "feature.properties.attributes.global_id"
        ).cast("long").alias("global_id"),

        F.lit("INTERCEPT_PARKING").alias("object_type"),

        F.trim(
            F.col(
                "feature.properties.attributes.ParkingName"
            )
        ).alias("parking_name"),

        F.trim(
            F.col("feature.properties.attributes.AdmArea")
        ).alias("adm_area"),

        F.trim(
            F.col("feature.properties.attributes.District")
        ).alias("district"),

        F.when(
            F.length(
                F.trim(
                    F.col(
                        "feature.properties.attributes."
                        "LocationDescription"
                    )
                )
            ) > 0,
            F.trim(
                F.col(
                    "feature.properties.attributes."
                    "LocationDescription"
                )
            ),
        ).alias("address"),

        F.col(
            "feature.geometry.coordinates"
        )[0].cast("double").alias("longitude"),

        F.col(
            "feature.geometry.coordinates"
        )[1].cast("double").alias("latitude"),

        F.col(
            "feature.properties.attributes.CarCapacity"
        ).cast("int").alias("capacity"),

        F.trim(
            F.col("feature.properties.attributes.Schedule")
        ).alias("schedule"),

        F.lit("data_mos_622_api").alias("source"),

        F.current_date().alias("load_date"),
    )


def normalize_paid(df):
    # MultiLineString:
    # [[[lon, lat], [lon, lat], ...]]
    #
    # flatten превращает его в:
    # [[lon, lat], [lon, lat], ...]
    df = df.withColumn(
        "points",
        F.flatten(
            F.col("feature.geometry.coordinates")
        ),
    )

    longitude = F.expr("""
        CASE
            WHEN size(points) > 0
            THEN aggregate(
                points,
                CAST(0.0 AS DOUBLE),
                (acc, point) -> acc + point[0]
            ) / size(points)
        END
    """)

    latitude = F.expr("""
        CASE
            WHEN size(points) > 0
            THEN aggregate(
                points,
                CAST(0.0 AS DOUBLE),
                (acc, point) -> acc + point[1]
            ) / size(points)
        END
    """)

    return df.select(
        F.col(
            "feature.properties.attributes.global_id"
        ).cast("long").alias("global_id"),

        F.lit("PAID_PARKING").alias("object_type"),

        F.trim(
            F.col(
                "feature.properties.attributes.ParkingName"
            )
        ).alias("parking_name"),

        F.trim(
            F.col("feature.properties.attributes.AdmArea")
        ).alias("adm_area"),

        F.trim(
            F.col("feature.properties.attributes.District")
        ).alias("district"),

        F.trim(
            F.col("feature.properties.attributes.Address")
        ).alias("address"),

        longitude.alias("longitude"),

        latitude.alias("latitude"),

        F.col(
            "feature.properties.attributes.CarCapacity"
        ).cast("int").alias("capacity"),

        F.lit(None).cast("string").alias("schedule"),

        F.lit("data_mos_623_api").alias("source"),

        F.current_date().alias("load_date"),
    )


def main():
    spark = get_spark()

    taxi_raw = read_features(
        spark,
        TAXI_PATH,
    )

    intercept_raw = read_features(
        spark,
        INTERCEPT_PATH,
    )

    paid_raw = read_features(
        spark,
        PAID_PATH,
    )

    taxi = normalize_taxi(taxi_raw)
    intercept = normalize_intercept(intercept_raw)
    paid = normalize_paid(paid_raw)

    taxi.write \
        .mode("overwrite") \
        .parquet(TAXI_OUTPUT)

    intercept.write \
        .mode("overwrite") \
        .parquet(INTERCEPT_OUTPUT)

    paid.write \
        .mode("overwrite") \
        .parquet(PAID_OUTPUT)

    print("=== API NORMALIZATION COMPLETE ===")
    print("Taxi:", taxi.count())
    print("Intercept:", intercept.count())
    print("Paid:", paid.count())

    print("\n=== NULL COORDINATES ===")

    for name, dataframe in [
        ("Taxi", taxi),
        ("Intercept", intercept),
        ("Paid", paid),
    ]:
        null_coordinates = dataframe.filter(
            F.col("longitude").isNull()
            | F.col("latitude").isNull()
        ).count()

        print(
            f"{name}: {null_coordinates}"
        )


if __name__ == "__main__":
    main()
