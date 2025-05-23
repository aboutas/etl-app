import re
from typing import Dict, List, Tuple
from dateutil.parser import parse as dt_parse


class Transformations:   
##Order 1 | If this rule exists have to execute first. If value out of range, no other transformations has to implemet on this record
    # Data Validation  
    @staticmethod
    def range_checks(data: Dict, fields: List[str]) -> Tuple[Dict, bool]:
        transformed = {}
        for key, value in data.items():
            if key in fields:
                if isinstance(value, (int, float)) and 0 <= value <= 100:
                    transformed[key] = value
            else:
                transformed[key] = value # untouched
        return transformed, False
    
## Order 2. Transformations per tuple.
    # Data Cleaning 
    @staticmethod
    def lower_case(data: Dict, fields: List[str]) -> Tuple[Dict, bool]:
        transformed = {}
        for key, value in data.items():
            if key in fields and isinstance(value, str):
                transformed[key] = value.lower().strip()
            else:
                transformed[key] = value
        return transformed, False
    
    @staticmethod
    def capitalization_rules(data: Dict, fields: List[str]) -> Tuple[Dict, bool]:
        transformed = {}
        for key, value in data.items():
            if key in fields and isinstance(value, str):
                transformed[key] = value.upper()
            else:
                transformed[key] = value
        return transformed, False
    
    # Data Transformation
    @staticmethod
    def str_to_float(data: Dict, fields: List[str]) -> Tuple[Dict, bool]:
        transformed = {}
        for key, value in data.items():
            if key in fields and isinstance(value, str) and value.replace('.', '', 1).isdigit():
                transformed[key] = float(value)
            else:
                transformed[key] = value
        return transformed, False

    @staticmethod
    def trimming(data: Dict, fields: List[str]) -> Tuple[Dict, bool]:
        transformed = {}
        for key, value in data.items():
            if key in fields and isinstance(value, str):
                val = value.strip()
                try:
                    parsed = dt_parse(val)
                    # Output as YYYY/MM/DD
                    transformed[key] = parsed.strftime("%Y/%m/%d")
                except Exception:
                    transformed[key] = val
            else:
                transformed[key] = value
        return transformed, False

    @staticmethod
    def year_extraction(data: Dict, fields: List[str]) -> Tuple[Dict, bool]:
        transformed = {}
        for key, value in data.items():
            if key in fields and isinstance(value, str):
                try:
                    parsed = dt_parse(value)
                    transformed[f"{key}_year"] = str(parsed.year)
                except Exception:
                    pass  # If not a date, skip
        return transformed, False

    @staticmethod
    def increment_value(data: Dict, fields: List[str], increment: float = 1) -> Tuple[Dict, bool]:
        """
        Increments numeric fields by 'increment'. Default increment is 1.
        """
        transformed = data.copy()
        for key in fields:
            val = data.get(key)
            if isinstance(val, (int, float)):
                transformed[key] = val + increment
        return transformed, False

    # Anonymization
    @staticmethod
    def data_masking(data: Dict, fields: List[str]) -> Tuple[Dict, bool]:
        transformed = {}
        for key, value in data.items():
            if key in fields and isinstance(value, (str, int)):
                v = str(value)
                masked = "XXXX-" + v[-4:] if len(v) >= 4 else "XXXX-" + v
                transformed[key] = masked
            else:
                transformed[key] = value
        return transformed, False
    
    #data_standardization
    @staticmethod
    def renaming_columns(data: Dict, fields: List[str], rename_map: Dict[str, str]) -> Tuple[Dict, bool]:
        transformed = {}
        for key, value in data.items():
            if key in fields and key in rename_map:
                transformed[rename_map[key]] = value
            elif key not in fields:
                transformed[key] = value
        return transformed, False

## Order 3. Transformations combining tuples | create a new column for aggregation_result. (maybe on map?)
    # Data Aggregation 
    @staticmethod
    def summarization(data: Dict, fields: List[str]) -> Tuple[Dict, bool]:
        """
        Sums all numeric values of specified fields and returns a single new field: sum_<fields...>
        Example: summarization(data, ["a", "b"]) => {"sum_a_b": value}
        """
        total = 0
        for key in fields:
            value = data.get(key)
            if isinstance(value, (int, float)):
                total += value
        sum_field = "sum_" + "_".join(fields)
        return {sum_field: total}, False
    
    @staticmethod
    def concatination(data: Dict, fields: List[str], sep: str = " ") -> Tuple[Dict, bool]:
        """
        Concatenate specified fields into a new field called 'concat_result' (or customize as needed).
        Example: concatination(data, ["first_name", "last_name"]) => data["concat_result"] = "John Doe"
        """
        values = [str(data.get(key, "")) for key in fields]
        result = sep.join(values)
        # Add to result dict (could customize the result field name)
        concat_field = "concat_" + "_".join(fields)
        transformed = data.copy()
        transformed[concat_field] = result
        return transformed, False



