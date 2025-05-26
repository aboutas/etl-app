# tests/test_transformations.py

import pytest
from transformations import Transformations

def test_lower_case_simple():
    data = {"name": "JOHN DOE", "city": "New York"}
    fields = ["name"]
    result, _ = Transformations.lower_case(data, fields)
    assert result["name"] == "john doe"
    assert result["city"] == "New York"

def test_data_masking():
    data = {"id": 123456, "owner.id": "ABCDEFGH"}
    fields = ["id", "owner.id"]
    result, _ = Transformations.data_masking(data, fields)
    assert result["id"].startswith("XXXX-")
    assert result["owner.id"].startswith("XXXX-")

def test_summarization():
    data = {"val1": 10, "val2": 15, "ignore": 99}
    fields = ["val1", "val2"]
    result, _ = Transformations.summarization(data, fields)
    assert result["sum_val1_val2"] == 25

def test_trimming_dates():
    data = {"dt": "  2021-05-12T14:22:00Z  "}
    fields = ["dt"]
    result, _ = Transformations.trimming(data, fields)
    assert result["dt"] == "2021/05/12"

def test_year_extraction():
    data = {"dt": "2021-05-12T14:22:00Z"}
    fields = ["dt"]
    result, _ = Transformations.year_extraction(data, fields)
    assert result["dt_year"] == "2021"

def test_concatination():
    data = {"a": "Hello", "b": "World"}
    fields = ["a", "b"]
    result, _ = Transformations.concatination(data, fields)
    assert result["concat_a_b"] == "Hello World"

def test_renaming_columns():
    data = {"timezone": "Europe/Athens", "other": "foo"}
    fields = ["timezone"]
    rename_map = {"timezone": "zoniwras"}
    result, _ = Transformations.renaming_columns(data, fields, rename_map)
    assert "zoniwras" in result
    assert "timezone" not in result
    assert result["zoniwras"] == "Europe/Athens"
