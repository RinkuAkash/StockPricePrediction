from alpha_vantage.timeseries import TimeSeries
from kafka import KafkaProducer
import json
import time

def get_data():
    with open("alpha_vantage_key") as file:
        lines = file.readlines()
    time_series = TimeSeries(key=lines[0])
    data, meta_data = time_series.get_intraday(symbol='GOOG', interval='1min')
    return data

def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value)
        producer_instance.flush()
        print("Message published successfully")
    except Exception as ex:
        print("Exception in publishing message")
        print(str(ex))

def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda value: json.dumps(value).encode('utf-8'))
    except Exception as ex:
        print("Exception while connecting Kafka")
        print(str(ex))
    finally:
        return _producer

if __name__=="__main__":
    while True:
        data = get_data()
        if len(data) > 0:
            kafka_producer = connect_kafka_producer()
            for key in data:
                publish_message(kafka_producer, 'stockprices', key, data[key])
                time.sleep(5)
