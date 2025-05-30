from pyflink.datastream.functions import MapFunction
import json
from datetime import datetime

class LowerCase(MapFunction):
    def map(self, value):
        value['text'] = value.get('text', '').lower()
        return value

class UpperCase(MapFunction):
    def map(self, value):
        value['country'] = value.get('country', '').upper()
        return value

class Trimming(MapFunction):
    def map(self, value):
        value['text'] = value.get('text', '').strip()
        return value

class YearExtraction(MapFunction):
    def map(self, value):
        ts = value.get('timestamp')
        try:
            if isinstance(ts, str):
                ts = int(ts)
            year = datetime.utcfromtimestamp(ts).year
            value['year'] = year
        except Exception:
            value['year'] = None
        return value
