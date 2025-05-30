from pyflink.datastream.functions import WindowFunction, MapFunction, ReduceFunction, ProcessWindowFunction, RuntimeContext

# --- WINDOWED AGGREGATION: per user, 1-min tumbling window ---    
class UserStats(WindowFunction):
        def apply(self, key, window, records):
            recs = list(records)
            return [{
                "user_id": key,
                "count": len(recs),
                "window_start": window.get_start(),
                "window_end": window.get_end(),
                "first_ts": min([r['timestamp'] for r in recs]),
                "last_ts": max([r['timestamp'] for r in recs])
            }]

# --- WINDOWED AGGREGATION: per country, 5-min sliding window (unique users) ---
class UniqueUsers(WindowFunction):
        def apply(self, key, window, records):
            users = set([r['user_id'] for r in records])
            return [{
                "country": key,
                "unique_users": len(users),
                "window_start": window.get_start(),
                "window_end": window.get_end()
            }]