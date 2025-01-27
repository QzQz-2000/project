import requests
import os
import yaml
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt, wait_fixed
from slugify import slugify  # Ensure you have `python-slugify` installed

# Simple logging configuration to log only to the console (terminal)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]  # Only output to console
)

def sanitize_platform_name(platform_name: str, warn_on_change: bool = False) -> str:
    """Function to sanitize the platform name.

    Parameters
    ----------
    platform_name : str
        The platform name.

    Returns
    -------
    str
        The sanitized platform name.
    """
    platform_sanitized = slugify(platform_name, replacements=[(".", ""), ("@", "at")], separator="-")
    if warn_on_change and platform_sanitized != platform_name:
        msg = "Sanitized (slugified) platform name from %s to %s" % (platform_name, platform_sanitized)
        logging.info(msg)
        RuntimeWarning(msg)
    return platform_sanitized

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def download_file(platform_name, date, base_url="https://dsa-sor-data-dumps.s3.eu-central-1.amazonaws.com/"):
    """Download a single file for a given platform and date."""
    file_name = f"sor-{platform_name}-{date}-full.zip"
    download_url = f"{base_url}{file_name}"

    output_dir = f"data/{platform_name}"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.info(f"Created directory: {output_dir}")

    output_file = os.path.join(output_dir, f"{platform_name}_{date}.csv.zip")

    try:
        logging.info(f"Downloading {download_url} to {output_file}")
        response = requests.get(download_url, stream=True)
        response.raise_for_status()

        with open(output_file, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        logging.info(f"File downloaded successfully: {output_file}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to download {download_url}: {e}")
        raise

def generate_date_range(start_date, end_date):
    """Generate a list of dates between start_date and end_date."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    date_list = []
    while start <= end:
        date_list.append(start.strftime("%Y-%m-%d"))
        start += timedelta(days=1)
    return date_list

def run_data_download(config_file="config.yml"):
    """
    Main function to orchestrate the download process for all platforms
    based on the configuration file.
    """
    # Load configuration
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Configuration file {config_file} does not exist")

    with open(config_file, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    logging.info(f"Loaded configuration file: {config_file}")

    # Iterate over platforms and their date ranges
    platform_date_ranges = config["platform_date_ranges"]

    for platform, (start_date, end_date) in platform_date_ranges.items():
        # Sanitize the platform name
        sanitized_platform = sanitize_platform_name(platform)
        logging.info(f"Sanitized platform name: {sanitized_platform}")
        
        logging.info(f"Processing platform: {sanitized_platform} | Date range: {start_date} to {end_date}")
        date_range = generate_date_range(start_date, end_date)

        # Use ThreadPoolExecutor for concurrent downloads
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(download_file, sanitized_platform, date) for date in date_range]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error while downloading: {e}")

    logging.info("All downloads completed successfully.")

# For Airflow, you would call this function in your task definition
def download_task(**kwargs):
    """
    Airflow task wrapper to execute the run_data_download function.
    """
    config_file = kwargs.get("config_file", "config.yml")
    run_data_download(config_file=config_file)

# Call the function to trigger the download process
download_task(config_file='config.yml')
