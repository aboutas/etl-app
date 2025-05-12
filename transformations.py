import re

class Transformations:
    # data_cleaning 
    @staticmethod
    def standardize_format(data, fields):
        transformed = {k: (v.lower().strip() if k in fields and isinstance(v, str) else v) for k, v in data.items()}
        return transformed, False  # Never filters out

    # data_aggregation
    @staticmethod
    def summarization(data, fields):
        transformed = {"total_sum": sum(v for k, v in data.items() if k in fields and isinstance(v, (int, float)))}
        return transformed, False

    # data_standardization
    @staticmethod
    def renaming_columns(data, rename_map):
        transformed = {rename_map.get(k, k): v for k, v in data.items()}
        return transformed, False

    @staticmethod
    def capitalization_rules(data, fields):
        transformed = {k: v.upper() if k in fields and isinstance(v, str) else v for k, v in data.items()}
        return transformed, False

    # data_validation
    @staticmethod
    def range_checks(data, fields):
        transformed = {k: v for k, v in data.items() if k in fields and isinstance(v, (int, float)) and 0 <= v <= 10000}
        return transformed, False

    # data_transformation
    @staticmethod
    def type_conversion(data, fields):
        transformed = {k: float(v) if k in fields and isinstance(v, str) and v.replace('.', '', 1).isdigit() else v for k, v in data.items()}
        return transformed, False

    @staticmethod
    def normalization(data, fields):
        max_val = max([v for k, v in data.items() if k in fields and isinstance(v, (int, float))], default=1)
        transformed = {k: v / max(1, max_val) if k in fields and isinstance(v, (int, float)) else v for k, v in data.items()}
        return transformed, False

    @staticmethod
    def denormalization(data):
        transformed = {"full_address": ", ".join(filter(None, [data.get("street", ""), data.get("city", "")]))}
        return transformed, False

    # text_manipulation
    @staticmethod
    def trimming(data, fields):
        transformed = {k: v.strip() if k in fields and isinstance(v, str) else v for k, v in data.items()}
        return transformed, False

    @staticmethod
    def regex_operations(data, fields):
        transformed = {f"{k}_digits": re.findall(r'\\d+', v) if k in fields and isinstance(v, str) else v for k, v in data.items()}
        return transformed, False

    # time_transformations
    @staticmethod
    def date_extraction(data, fields):
        transformed = {f"{k}_year": v[:4] for k, v in data.items() if k in fields and isinstance(v, str) and re.match(r"\\d{4}-\\d{2}-\\d{2}", v)}
        return transformed, False

    # anonymization
    @staticmethod
    def data_masking(data, fields):
        transformed = {k: f"XXXX-{str(v)[-4:]}" if "id" in k.lower() and k in fields and isinstance(v, (str, int)) else v for k, v in data.items()}
        return transformed, False