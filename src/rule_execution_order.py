from typing import List

def rule_exectution_order()  -> List[str] :
    execution_order = [    "range_checks",
                            "lower_case",
                            "capitalization_rules",
                            "str_to_float",
                            "trimming",
                            "date_extraction",
                            "increment_value",     
                            "data_masking",
                            "summarization",       
                            "concatination" ]                                    
    return execution_order 
