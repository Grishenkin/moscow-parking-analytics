
import os
import json
from pathlib import Path

import requests
import yaml
from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT_DIR / "config" / "sources.yaml"
ENV_PATH = ROOT_DIR / ".env"


def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def get_api_key():
    load_dotenv(ENV_PATH)

    api_key = os.getenv("MOS_API_KEY")

    if not api_key:
        raise ValueError(
            "MOS_API_KEY not found in .env"
        )

    return api_key


def get_dataset_count(
    session,
    base_url,
    dataset_id,
    api_key,
):
    url = f"{base_url}/datasets/{dataset_id}/count"

    response = session.get(
        url,
        params={
            "api_key": api_key,
        },
        timeout=60,
    )

    response.raise_for_status()

    return int(response.text)


def download_dataset(
    session,
    base_url,
    dataset_id,
    api_key,
    page_size,
):
    url = f"{base_url}/datasets/{dataset_id}/features"

    expected_count = get_dataset_count(
        session=session,
        base_url=base_url,
        dataset_id=dataset_id,
        api_key=api_key,
    )

    print(
        f"Dataset {dataset_id}: "
        f"API count = {expected_count}"
    )

    all_features = []
    skip = 0

    while skip < expected_count:

        response = session.get(
            url,
            params={
                "$top": page_size,
                "$skip": skip,
                "api_key": api_key,
            },
            timeout=60,
        )

        response.raise_for_status()

        payload = response.json()

        features = payload.get("features", [])

        if not features:
            break

        all_features.extend(features)

        skip += len(features)

        print(
            f"  downloaded: "
            f"{len(all_features)}/{expected_count}"
        )

        if len(features) < page_size:
            break

    actual_count = len(all_features)

    if actual_count != expected_count:
        raise ValueError(
            f"Dataset {dataset_id}: "
            f"expected {expected_count}, "
            f"downloaded {actual_count}"
        )

    return {
        "type": "FeatureCollection",
        "features": all_features,
    }


def save_geojson(data, output_path):
    full_path = ROOT_DIR / output_path

    full_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with open(
        full_path,
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            data,
            file,
            ensure_ascii=False,
        )

    return full_path


def main():
    config = load_config()
    api_key = get_api_key()

    base_url = config["api"]["base_url"]
    page_size = config["api"]["page_size"]

    session = requests.Session()

    print("=== MOSCOW OPEN DATA DOWNLOAD ===")

    for dataset_name, dataset_config in (
        config["datasets"].items()
    ):
        dataset_id = dataset_config["dataset_id"]
        title = dataset_config["title"]
        output_path = dataset_config["output_path"]

        print("\n" + "=" * 60)
        print(f"{dataset_id} — {title}")
        print("=" * 60)

        data = download_dataset(
            session=session,
            base_url=base_url,
            dataset_id=dataset_id,
            api_key=api_key,
            page_size=page_size,
        )

        saved_path = save_geojson(
            data,
            output_path,
        )

        print(
            f"Saved: {saved_path}"
        )

    print("\n==============================")
    print("ALL DATASETS DOWNLOADED ✓")
    print("==============================")


if __name__ == "__main__":
    main()
