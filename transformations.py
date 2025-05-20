import re
from typing import Dict, List, Tuple
from datetime import datetime

class Transformations:
    # ------------------ Data Cleaning ------------------
    @staticmethod
    def standardize_format(data: Dict, fields: List[str]) -> Tuple[Dict, bool]:
        transformed = {}
        for key, value in data.items():
            if key in fields and isinstance(value, str):
                transformed[key] = value.lower().strip()
            else:
                transformed[key] = value
        return transformed, False
    # ------------------ Data Aggregation ------------------
    @staticmethod
    def summarization(data: Dict, fields: List[str]) -> Tuple[Dict, bool]:
        total = sum(value for key, value in data.items() if key in fields and isinstance(value, (int, float)))
        return {"total_sum": total}, False

    # ------------------ Data Standardization ------------------
    @staticmethod
    def renaming_columns(data: Dict, rename_map: Dict[str, str]) -> Tuple[Dict, bool]:
        transformed = {rename_map.get(key, key): value for key, value in data.items()}
        return transformed, False

    @staticmethod
    def capitalization_rules(data: Dict, fields: List[str]) -> Tuple[Dict, bool]:
        transformed = {key: value.upper() if key in fields and isinstance(value, str) else value for key, value in data.items()}
        return transformed, False

    # ------------------ Data Validation ------------------
    @staticmethod
    def range_checks(data: Dict, fields: List[str]) -> Tuple[Dict, bool]:
        transformed = {key: value for key, value in data.items() if key in fields and isinstance(value, (int, float)) and 0 <= value <= 10000}
        return transformed, False

    # ------------------ Data Transformation ------------------
    @staticmethod
    def type_conversion(data: Dict, fields: List[str]) -> Tuple[Dict, bool]:
        transformed = {}
        for key, value in data.items():
            if key in fields and isinstance(value, str) and value.replace('.', '', 1).isdigit():
                transformed[key] = float(value)
            else:
                transformed[key] = value
        return transformed, False

    @staticmethod
    def normalization(data: Dict, fields: List[str]) -> Tuple[Dict, bool]:
        max_val = max((value for key, value in data.items() if key in fields and isinstance(value, (int, float))), default=1)
        transformed = {key: (value / max_val if key in fields and isinstance(value, (int, float)) else value) for key, value in data.items()}
        return transformed, False

    @staticmethod
    def denormalization(data: Dict, fields: List[str] = None) -> Tuple[Dict, bool]:
        street = data.get("street", "")
        city = data.get("city", "")
        full_address = ", ".join([part for part in [street, city] if part])
        return {"full_address": full_address}, False

    # ------------------ Text Manipulation ------------------
    from datetime import datetime

    @staticmethod
    def trimming(data: Dict, fields: List[str]) -> Tuple[Dict, bool]:
        transformed = {}
        for key, value in data.items():
            if key in fields and isinstance(value, str):
                val = value.strip()
                parsed = None

                # Try standard UTC (Z) format
                try:
                    parsed = datetime.strptime(val, "%Y-%m-%dT%H:%M:%SZ")
                except ValueError:
                    pass

                # Try offset format like -07:00
                if not parsed:
                    try:
                        parsed = datetime.strptime(val, "%Y-%m-%dT%H:%M:%S%z")
                    except ValueError:
                        pass

                # If parsed successfully, format it
                if parsed:
                    transformed[key] = parsed.strftime("%Y/%m/%d")
                else:
                    transformed[key] = val  # fallback
            else:
                transformed[key] = value
        return transformed, False


    @staticmethod
    def regex_operations(data: Dict, fields: List[str]) -> Tuple[Dict, bool]:
        transformed = {}
        for key, value in data.items():
            if key in fields and isinstance(value, str):
                transformed[f"{key}_digits"] = re.findall(r"\d+", value)
            else:
                transformed[key] = value
        return transformed, False

    # ------------------ Time Transformations ------------------
    @staticmethod
    def date_extraction(data: Dict, fields: List[str]) -> Tuple[Dict, bool]:
        transformed = {}
        for key, value in data.items():
            if key in fields and isinstance(value, str) and re.match(r"\d{4}-\d{2}-\d{2}", value):
                transformed[f"{key}_year"] = value[:4]
        return transformed, False

    # ------------------ Anonymization ------------------
    @staticmethod
    def data_masking(data: Dict, fields: List[str]) -> Tuple[Dict, bool]:
        transformed = {}
        for key, value in data.items():
            if isinstance(value, (str, int)) and any(key.endswith(f) for f in fields):
                transformed[key] = f"XXXX-{str(value)[-4:]}"
            else:
                transformed[key] = value
        return transformed, False
