from fetcher import fetch_data_from_api
from helpers import load_config
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


if __name__ == "__main__":
    try:
        
        config = load_config()
        url = config.get("url")
        api_key = config.get("api_key")

        fetched_data = fetch_data_from_api(url, api_key)
        print(fetched_data)

        logging.info("Reading from API")

    except Exception as e:
        logging.error(f"Failed to execute pipeline: {e}")
