import requests
from typing import Any

#pythhonApp/Thread logic

# Has to improve for making two or multiple API calls
def fetch_data_from_api(url: str, api_key: str) -> list[dict[str, Any]]:
    headers = {"X-API-Key": api_key}
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  
        data = response.json()
        return data.get("results", []) if isinstance(data, dict) else []
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []
