from pathlib import Path
from datetime import date, timedelta

import requests

# NYC TLC Parquet storage
BASE_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
)

TAXI_TYPES = ["yellow", "green"]

START_YEAR = 2021
END_YEAR = 2026
END_MONTH = 9

# Local staging directory
DATA_DIR = Path("/opt/airflow/database")

def get_dataset_config(dataset: str) -> dict:
    """Build configuration for a known NYC TLC dataset."""
    parts = dataset.split("_")

    if len(parts) != 3:
        raise ValueError(f"Invalid dataset name: {dataset}")

    taxi_type, year, month = parts

    if taxi_type not in TAXI_TYPES:
        raise ValueError(f"Invalid taxi type: {taxi_type}")

    year = int(year)
    month = int(month)

    if not 1 <= month <= 12:
        raise ValueError(f"Invalid month: {month}")

    filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"

    return {
        "taxi_type": taxi_type,
        "year": year,
        "month": month,
        "file": f"{BASE_URL}{filename}",
        "local_file": str(DATA_DIR / filename),
        "s3_key": (
            f"raw/{taxi_type}/"
            f"year={year}/"
            f"month={month:02d}/"
            f"{filename}"
        ),
    }


def generate_dataset_names(start_year: int, end_year: int, end_month: int = 12) -> list[str]:
    """Generate historical dataset names."""
    return [
        f"{taxi_type}_{year}_{month:02d}"
        for taxi_type in TAXI_TYPES
        for year in range(start_year, end_year + 1)
        for month in range(
            1,
            end_month + 1 if year == end_year else 13,
        )
    ]


def get_recent_available_datasets(lookback_months: int = 6,) -> list[str]:
    """
    Discover recently published datasets.
    Checks the previous completed months for both
    yellow and green taxi data.
    """

    today = date.today()
    last_month = today.replace(day=1) - timedelta(days=1)
    available_datasets = []

    for offset in range(lookback_months):
        year = last_month.year
        month = last_month.month - offset

        while month <= 0:
            month += 12
            year -= 1

        for taxi_type in TAXI_TYPES:

            dataset = f"{taxi_type}_{year}_{month:02d}"
            config = get_dataset_config(dataset)

            try:
                response = requests.head(
                    config["file"],
                    timeout=20,
                    allow_redirects=True,
                )

                response.raise_for_status()

                available_datasets.append(dataset)

                print(f"Dataset available: {dataset}")

            except requests.HTTPError as error:

                if error.response is not None and (
                    error.response.status_code == 404
                ):
                    print(f"Not published: {dataset}")
                    continue
                raise

    return available_datasets


def extract_data(dataset: str) -> Path:
    """
    Download a Parquet dataset directly from NYC TLC.
    Returns the local path for the downloaded file.
    """
    config = get_dataset_config(dataset)

    source_url = config["file"]
    local_file = Path(config["local_file"])

    DATA_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Reuse an existing completed download
    if local_file.exists() and local_file.stat().st_size > 0:
        print(f"Already downloaded: {dataset}")
        return local_file

    print(f"Downloading: {dataset}")
    print(f"Source: {source_url}")

    # Download into a temporary file first
    temp_file = local_file.with_suffix(".parquet.tmp")

    try:
        with requests.get(
            source_url,
            stream=True,
            timeout=(10, 300),
        ) as response:

            response.raise_for_status()

            with open(temp_file, "wb") as file:

                for chunk in response.iter_content(
                    chunk_size=8 * 1024 * 1024
                ):
                    if chunk:
                        file.write(chunk)

        # Publish only after the download completes
        temp_file.replace(local_file)

    except Exception:
        temp_file.unlink(missing_ok=True)
        raise

    print(f"Downloaded: {dataset}")
    print(f"Local file: {local_file}")
    print(
        f"Size: {local_file.stat().st_size / (1024**2):.2f} MB"
    )

    return local_file


if __name__ == "__main__":
    dataset = "yellow_2020_01"
    downloaded_file = extract_data(dataset)
    print(f"Successfully downloaded: {downloaded_file}")
