from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction
import json
import schema_manager
import rule_manager_transform    

env = StreamExecutionEnvironment.get_execution_environment()

with open('/opt/flink/app/input.json', 'r') as f:
    json_inputs = json.load(f)

data_stream = env.from_collection([json.dumps(item) for item in json_inputs])
transformed_stream = data_stream.map(rule_manager_transform.RuleManagerTransform(schema_manager.schema_registry))
# with open('rules.json', 'r') as f:
#     rules = json.load(f)
# transformed_stream = data_stream.map(rule_manager_transform.RuleManagerTransform(schema_manager.schema_registry, rules))

def write_to_json_file(value):
    """
    Writes the transformed JSON data to an output file.

    Args:
        value (str): Transformed JSON data in string format.

    This function appends JSON records to a file located in the `/opt/flink/output/`
    directory. Each record is written on a new line.
    """
    data_json = json.loads(value) 
    with open('/opt/flink/output/output_data.json', 'a') as f:
        json.dump(data_json, f)
        f.write('\n') 

transformed_stream.map(write_to_json_file)

env.execute("Dynamic JSON to File with Flink")
