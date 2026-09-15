
import json

from src.jobs.normalize_parking import normalize_taxi


def test_normalize_taxi(spark):

    source_record = {
        "feature": {
            "geometry": {
                "type": "Point",
                "coordinates": [
                    37.696289,
                    55.767273
                ]
            },
            "properties": {
                "attributes": {
                    "global_id": 1045388721,
                    "Name": "  Тестовая парковка такси  ",
                    "AdmArea": "Центральный административный округ",
                    "District": "Басманный район",
                    "Address": "Госпитальная улица, дом 14/1",
                    "CarCapacity": 4,
                    "Mode": "круглосуточно"
                }
            }
        }
    }

    df = spark.read.json(
        spark.sparkContext.parallelize([
            json.dumps(
                source_record,
                ensure_ascii=False
            )
        ])
    )

    result = normalize_taxi(df)

    row = result.first()

    assert result.count() == 1

    assert row.global_id == 1045388721
    assert row.object_type == "TAXI_PARKING"

    # Проверяем trim
    assert row.parking_name == "Тестовая парковка такси"

    assert row.adm_area == (
        "Центральный административный округ"
    )

    assert row.district == "Басманный район"

    assert row.address == (
        "Госпитальная улица, дом 14/1"
    )

    assert row.longitude == 37.696289
    assert row.latitude == 55.767273

    assert row.capacity == 4
    assert row.schedule == "круглосуточно"
    assert row.source == "data_mos_621_api"

    assert row.load_date is not None


from src.jobs.normalize_parking import normalize_intercept


def test_normalize_intercept(spark):

    source_record = {
        "feature": {
            "geometry": {
                "type": "Point",
                "coordinates": [
                    37.5357025,
                    55.7749505
                ]
            },
            "properties": {
                "attributes": {
                    "global_id": 1044511639,
                    "ParkingName": "  Перехватывающая парковка Беговая  ",
                    "AdmArea": "Северный административный округ",
                    "District": "Хорошёвский район",
                    "LocationDescription": "",
                    "CarCapacity": 120,
                    "Schedule": "круглосуточно"
                }
            }
        }
    }

    df = spark.read.json(
        spark.sparkContext.parallelize([
            json.dumps(
                source_record,
                ensure_ascii=False
            )
        ])
    )

    result = normalize_intercept(df)

    row = result.first()

    assert result.count() == 1

    assert row.global_id == 1044511639
    assert row.object_type == "INTERCEPT_PARKING"

    assert row.parking_name == (
        "Перехватывающая парковка Беговая"
    )

    assert row.adm_area == (
        "Северный административный округ"
    )

    assert row.district == "Хорошёвский район"

    # Пустая строка источника должна стать NULL
    assert row.address is None

    assert row.longitude == 37.5357025
    assert row.latitude == 55.7749505

    assert row.capacity == 120
    assert row.schedule == "круглосуточно"
    assert row.source == "data_mos_622_api"

    assert row.load_date is not None


from src.jobs.normalize_parking import normalize_paid


def test_normalize_paid(spark):

    source_record = {
        "feature": {
            "geometry": {
                "type": "MultiLineString",
                "coordinates": [
                    [
                        [37.8, 55.7],
                        [37.9, 55.8]
                    ]
                ]
            },
            "properties": {
                "attributes": {
                    "global_id": 2755298624,
                    "ParkingName": "  Тестовая платная парковка  ",
                    "AdmArea": "Восточный административный округ",
                    "District": "район Новогиреево",
                    "Address": "Напольный проезд, дом 10",
                    "CarCapacity": 12
                }
            }
        }
    }

    df = spark.read.json(
        spark.sparkContext.parallelize([
            json.dumps(
                source_record,
                ensure_ascii=False
            )
        ])
    )

    result = normalize_paid(df)

    row = result.first()

    assert result.count() == 1

    assert row.global_id == 2755298624
    assert row.object_type == "PAID_PARKING"

    assert row.parking_name == (
        "Тестовая платная парковка"
    )

    assert row.adm_area == (
        "Восточный административный округ"
    )

    assert row.district == "район Новогиреево"

    assert row.address == (
        "Напольный проезд, дом 10"
    )

    # Среднее между 37.8 и 37.9
    assert abs(row.longitude - 37.85) < 1e-9

    # Среднее между 55.7 и 55.8
    assert abs(row.latitude - 55.75) < 1e-9

    assert row.capacity == 12

    # У paid parking schedule отсутствует в API
    assert row.schedule is None

    assert row.source == "data_mos_623_api"

    assert row.load_date is not None
