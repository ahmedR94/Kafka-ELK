"""consumer"""

from kafka import KafkaConsumer
import json
import requests
import os


# ------------------------------
os.environ['KAFKA_TOPIC'] = "FirstTopic"
os.environ['SERVER_END_POINT'] = "http://localhost:9200"
# -----------------------------



class ElasticSearchKafkaUploadRecord:

    def __init__(self, json_data, hash_key, index_name):
        self.hash_key = hash_key
        self.json_data = json_data
        self.index_name = index_name.lower()

    def upload(self):
        """
        Uploads records on Elastic Search Cluster
        :return
        """
        URL = "{}/{}/_doc/{}".format(
            os.getenv("SERVER_END_POINT"), self.index_name, self.hash_key
        )
        print(URL)

        headers = {"Content-Type": "application/json"}

        response = requests.request(
            "PUT", URL, headers=headers, data=json.dumps(self.json_data),
        )
        print(response)

        return {"status": 200, "data": {"message": "record uploaded to Elastic Search"}}


def main():

    consumer = KafkaConsumer(os.getenv("KAFKA_TOPIC"),auto_offset_reset='earliest')

    for msg in consumer:
        print ("%s:%d:%d: key=%s value=%s" % (msg.topic, msg.partition,
                                                msg.offset, msg.key,
                                                msg.value))
        payload = json.loads(msg.value)
        payload["meta_data"]={
            "topic":msg.topic,
            "partition":msg.partition,
            "offset":msg.offset,
            "timestamp":msg.timestamp,
            "timestamp_type":msg.timestamp_type,
            "key":msg.key,
        }

        helper = ElasticSearchKafkaUploadRecord(json_data=payload,
                                                index_name=payload.get("meta_data").get("topic"),
                                                hash_key=payload.get("meta_data").get("offset"),
                                                )
        response = helper.upload()


main()
