
from datetime import timedelta
from pathlib import Path

import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROJECT_PYTHON = PROJECT_ROOT / ".venv-project" / "bin" / "python"


def project_command(script_path: str) -> str:
    return (
        f'cd "{PROJECT_ROOT}" && '
        f'"{PROJECT_PYTHON}" "{script_path}"'
    )


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="moscow_parking_pipeline",
    description="ETL pipeline for Moscow parking infrastructure analytics",

    start_date=pendulum.datetime(
        2026,
        9,
        1,
        tz="Europe/Moscow",
    ),

    # Каждый понедельник в 06:00 по Москве
    schedule="0 6 * * 1",

    catchup=False,
    max_active_runs=1,

    default_args=default_args,

    tags=[
        "spark",
        "moscow-open-data",
        "parking",
    ],
) as dag:

    download_sources = BashOperator(
        task_id="download_sources",
        bash_command=project_command(
            "src/ingestion/download_sources.py"
        ),
    )

    normalize_parking = BashOperator(
        task_id="normalize_parking",
        bash_command=project_command(
            "src/jobs/normalize_parking.py"
        ),
    )

    build_unified_dataset = BashOperator(
        task_id="build_unified_dataset",
        bash_command=project_command(
            "src/jobs/build_unified_dataset.py"
        ),
    )

    build_marts = BashOperator(
        task_id="build_marts",
        bash_command=project_command(
            "src/jobs/build_marts.py"
        ),
    )

    quality_checks = BashOperator(
        task_id="quality_checks",
        bash_command=project_command(
            "src/quality/checks.py"
        ),
    )

    (
        download_sources
        >> normalize_parking
        >> build_unified_dataset
        >> build_marts
        >> quality_checks
    )
