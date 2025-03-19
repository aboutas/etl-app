from pyflink.datastream import StreamExecutionEnvironment
import json
import schema_handler
import rule_manager    

env = StreamExecutionEnvironment.get_execution_environment()

with open('/opt/flink/app/input.json', 'r') as f:
    json_inputs = json.load(f)
    
with open('/opt/flink/app/selected_rules.json', 'r') as f:
    selected_rules = json.load(f)

rule_manager_transform = rule_manager.RuleManagerTransform(
    schema_handler.schema_registry, selected_rules
)

data_stream = env.from_collection([json.dumps(item) for item in json_inputs])
transformed_stream = data_stream.map(rule_manager_transform)

def write_to_json_file(value):
    """Writes the transformed JSON data to an output file."""
    data_json = json.loads(value) 
    with open('/opt/flink/output/output_data.json', 'a') as f:
        json.dump(data_json, f)
        f.write('\n') 

transformed_stream.map(write_to_json_file)

env.execute("Dynamic JSON to File with Flink")
