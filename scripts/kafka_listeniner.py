import kafka
import os
import json
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv
from datetime import datetime
import logging
import threading
import subprocess

load_dotenv()

# Kafka configuration
TOPIC_NEW_DATA = "new_data"
# TOPIC_UPDATE_WAREHOUSE = "update_warehouse"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", '172.31.70.181:9092')

# Initialize logging
log_path = os.path.expanduser('~/airflow/logs/data_pipeline.log')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler(),
    ],
)

def kafka_send_message(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=None, key=None, value=None):
    """
    Sends a message to a Kafka topic.
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else str(k).encode(),
        )

        future = producer.send(topic, key=key, value=value)
        result = future.get(timeout=60)
        logging.info(f"Message sent to topic {topic} with key {key}: {value}")
        producer.flush()
        producer.close()
        return True
    except Exception as e:
        logging.error(f"Error sending message to Kafka: {e}")
        return False


def trigger_new_data(timestamp=None):
    """
    Triggers a new data event.
    """
    if timestamp is None:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        logging.info(f"Triggering new data event with timestamp: {timestamp}")
        message_key = f"new_data_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        message_value = {
            "status": "new_data",
            "timestamp": timestamp,
            "source": "etl_process",
        }
        kafka_send_message(topic=TOPIC_NEW_DATA, key=message_key, value=message_value)
    except Exception as e:
        logging.error(f"Error triggering new data event: {e}")
        return False
    return True


def trigger_update_warehouse(timestamp=None):
    """
    Triggers an update warehouse event.
    """
    if timestamp is None:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        logging.info(f"Triggering update warehouse event with timestamp: {timestamp}")
        message_key = f"update_warehouse_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        message_value = {
            "status": "update_warehouse",
            "timestamp": timestamp,
            "source": "update_process",
        }
        kafka_send_message(topic=TOPIC_UPDATE_WAREHOUSE, key=message_key, value=message_value)
    except Exception as e:
        logging.error(f"Error triggering update warehouse event: {e}")
        return False
    return True


def consume_messages(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=None, group_id=None, callback=None):
    """
    Consumes messages from a Kafka topic and handles them.
    """
    try:
        # consumer = KafkaConsumer(
        #     topic,
        #     bootstrap_servers=bootstrap_servers,
        #     auto_offset_reset="earliest",
        #     enable_auto_commit=True,
        #     group_id=group_id,
        #     value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        # )
        consumer = KafkaConsumer(
            topic,
            auto_offset_reset='latest',  # Đọc từ tin nhắn mới nhất
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )

        for message in consumer:
            try:
                logging.info(f"Received message: {message.value} from topic: {message.topic}")
                callback(message)
            except Exception as e:
                logging.error(f"Error processing message: {e}")
                continue

    except Exception as e:
        logging.error(f"Error consuming messages from Kafka: {e}")



def handle_update_warehouse_event(event):
    try:
        # Parse message value
        message_value = event.value
        
        timestamp = message_value.get("timestamp")
        logging.info(f"Handling update warehouse event with timestamp: {timestamp}")

        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        script_path = os.path.join(project_root, "app", "app.py")

        if os.path.exists(script_path):
            logging.info(f"Executing app script: {script_path}")
            subprocess.Popen(["streamlit", "run", script_path])
        else:
            logging.error(f"App script not found: {script_path}")
            return False
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing message JSON: {e}")
        return False
    except Exception as e:
        logging.error(f"Error handling update warehouse event: {e}")
        return False
    return True

def handle_new_data_event(event):
    """
    Handles a new data event.
    """
    try:
        # Parse message value
        message_value = event.value # event.value chứa dữ liệu JSON
        
        # Lấy timestamp từ message
        timestamp = message_value.get("timestamp")
        logging.info(f"Handling new data event with timestamp: {timestamp}")

        # Xác định đường dẫn đến file ETL.py
        # scripts_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts")
        # script_path = os.path.join(scripts_dir, "ETL.py")

        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        script_path = os.path.join(project_root, "scripts",  "ETL.py")

        if os.path.exists(script_path):
            logging.info(f"Executing ETL script: {script_path}")
            subprocess.run(["python3", script_path], check=True)
        else:
            logging.error(f"ETL script not found: {script_path}")
            return False
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing message JSON: {e}")
        return False
    except Exception as e:
        logging.error(f"Error handling new data event: {e}")
        return False
    return True


def start_kafka_consumer():
    """
    Starts Kafka consumers for multiple topics in separate threads.
    """
    try:
        # Thread for consuming messages from TOPIC_NEW_DATA
        new_data_thread = threading.Thread(
            # target=consume_messages,
            # kwargs={"bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS, "topic": TOPIC_NEW_DATA, "group_id": "new_data_group"},
            # daemon=True,
            target=consume_messages,
            args=(KAFKA_BOOTSTRAP_SERVERS, TOPIC_NEW_DATA, 'new_data_group', handle_new_data_event),
            daemon=True
        )


        # Start the threads
        new_data_thread.start()

        logging.info("Kafka consumers started for both topics.")
        new_data_thread.join()

    except Exception as e:
        logging.error(f"Error starting Kafka consumer threads: {e}")


if __name__ == "__main__":
    logging.info("Starting the Kafka data pipeline...")
    try:
        start_kafka_consumer()
    except KeyboardInterrupt:
        logging.info("Shutting down the Kafka data pipeline...")

