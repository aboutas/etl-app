import requests
from typing import Any

def fetch_data_from_api(url: str, api_key: str) -> list[dict[str, Any]]:
    headers = {
        "X-API-Key": api_key
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raises an error for non-2xx responses
        data = response.json()
        return data.get("results", []) if isinstance(data, dict) else []
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []
