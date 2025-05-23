import re
from typing import Dict, List, Tuple
from datetime import datetime
from dateutil import parser as dateparser

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

    # Time Transformations
    @staticmethod
    def trimming(data: Dict, fields: List[str]) -> Tuple[Dict, bool]:
        transformed = {}
        for key, value in data.items():
            if key in fields and isinstance(value, str):
                val = value.strip()
                # Try to parse date in any common format
                try:
                    parsed = dateparser.parse(val)
                    if parsed:
                        transformed[key] = parsed.strftime("%Y/%m/%d")
                    else:
                        transformed[key] = val
                except Exception:
                    transformed[key] = val
            else:
                transformed[key] = value
        return transformed, False

    @staticmethod
    def date_extraction(data: Dict, fields: List[str]) -> Tuple[Dict, bool]:
        transformed = {}
        for key, value in data.items():
            if key in fields and isinstance(value, str):
                try:
                    parsed = dateparser.parse(value)
                    if parsed:
                        transformed[f"{key}_year"] = str(parsed.year)
                except Exception:
                    pass
        return transformed, False


    #New for testing alongside summarization fro ordering execution
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

## Order 3. Transformations combining tuples | create a new column for aggregation_result. (maybe on map?)
    # Data Aggregation 
    @staticmethod
    def summarization(data: Dict, fields: List[str]) -> Tuple[Dict, bool]:
        total = 0
        valid = False
        for key in fields:
            value = data.get(key)
            if isinstance(value, (int, float)):
                total += value
                valid = True
        # Only add the field if at least one valid number found
        result = data.copy()
        if valid:
            result["total_sum"] = total
        return result, False

    
    #New for testing alongside lower-upper rules fro ordering execution
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

