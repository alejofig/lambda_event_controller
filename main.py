import base64
import json
import copy
 
 
def create_partitions(message):
    print("MESSAGE ", message)
    partition_keys = {
        "sns_topic": message["sns_topic"]
    }
    return partition_keys
 
 
def check_topic(record):
    return record['TopicArn'].split(":")[-1]
 
 
def lambda_handler(event, context):
    output = []
 
    for record in event['records']:
        # Decode from base64 (Firehose records are base64 encoded)
        payload = base64.b64decode(record['data'])
        # Read json as utf-8
        json_string = payload.decode("utf-8")
        message = json.loads(json_string)
        sns_topic = check_topic(message)
        message["sns_topic"] = sns_topic
        partitions = create_partitions(message)
        # Add a line break
        final_message = json.loads(message["Message"])
        final_message.update({"approximateArrivalTimestamp": record["approximateArrivalTimestamp"],
                              "recordId": record["recordId"]})
        output_json_with_line_break = json.dumps(final_message) + "\n"
 
        # Encode the data
        encoded_bytes = base64.b64encode(bytearray(output_json_with_line_break, 'utf-8'))
        encoded_string = str(encoded_bytes, 'utf-8')
 
        # Create a deep copy of the record and append to output with transformed data
        output_record = copy.deepcopy(record)
        output_record['data'] = encoded_string
        output_record['result'] = 'Ok'
        output_record['metadata'] = {'partitionKeys': partitions}
 
        output.append(output_record)
 
    return {'records': output}
