"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

BROKER_URL = "PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

class Producer:
    """Defines and provides common functionality amongst Producers"""

    existing_topics = set([])

    def __init__(self, topic_name, key_schema, value_schema=None, num_partitions=1, num_replicas=1):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,
            "schema.registry.url": SCHEMA_REGISTRY_URL
        }

        self.admin_client = AdminClient({"bootstrap.servers": BROKER_URL})
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema
        )
    def exist_topic(self,topic_name):
        topic_metadata = self.admin_client.list_topics(timeout=5)
        return topic_name in topic_metadata.topics
    
    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        
        if self.exist_topic(self.topic_name):
            logger.info(f"Topic {self.topic_name} already exists")
            return

        new_topic = NewTopic(self.topic_name, num_partitions=self.num_partitions, replication_factor=self.num_replicas)
        topic_created = self.admin_client.create_topics([new_topic])
        for topic_name, future in topic_created.items():
            try:
                future.result()
                logger.info(f"Topic {topic_name} create successfully")
            except Exception as e:
                logger.error(f"{self.topic_name} is not created: {e}")

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush(timeout=5)

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
