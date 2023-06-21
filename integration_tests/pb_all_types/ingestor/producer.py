import os
import time
from kafka import KafkaProducer

KAFKA_BROKER = os.environ.get('KAFKA_BROKER')
TOPIC_NAME = os.environ.get('TOPIC_NAME')
DATA_FILE = os.environ.get('DATA_FILE')

def read_file_in_chunks(file_path):
    with open(file_path, "rb") as file:
        while chunk := file.read(1024):
            yield chunk

def main():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
    for msg in read_file_in_chunks(DATA_FILE):
        producer.send(TOPIC_NAME, msg)
        time.sleep(1)  # to ensure messages are sent in order

    producer.close()

if __name__ == "__main__":
    main()
