#data_cleaning
def standardize_format(data, fields):
    return {k: (v.lower().strip() if k in fields and isinstance(v, str) else v) for k, v in data.items()}

#data_filtering
def row_filtering(data):
    return data if any(isinstance(v, (int, float)) and v > 100 for v in data.values()) else None

def column_filtering(data):
    return {k: v for k, v in data.items() if isinstance(v, (int, str))}

