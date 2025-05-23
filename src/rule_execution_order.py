from typing import List

def rule_exectution_order()  -> List[str] :
    execution_order = [    "range_checks",
                            "lower_case",
                            "capitalization_rules",
                            "str_to_float",
                            "trimming",
                            "year_extraction",
                            "increment_value",     
                            "data_masking",
                            "summarization",       
                            "concatination", 
                            "renaming_columns"]                            
    return execution_order 
