from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
import time

TOPIC_NAME = "Demo"
BROKER = "localhost:9092"

def reset_topic():
    admin_client = KafkaAdminClient(bootstrap_servers=BROKER)

    
    try:
        print(f"Deleting topic '{TOPIC_NAME}' if it exists...")
        admin_client.delete_topics([TOPIC_NAME])
        
        time.sleep(1)
        print(f"Deleted topic '{TOPIC_NAME}'.")
    except UnknownTopicOrPartitionError:
        print(f"Topic '{TOPIC_NAME}' does not exist. Nothing to delete.")
    except Exception as e:
        print(f"Warning while deleting topic: {e}")

    #waiting
    time.sleep(1)

    try:
        topic = NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1)
        admin_client.create_topics([topic])
        print(f"Created topic '{TOPIC_NAME}' successfully!")
    except TopicAlreadyExistsError:
        print(f"Topic '{TOPIC_NAME}' already exists.")
    except Exception as e:
        print(f"Error while creating topic: {e}")

    admin_client.close()

if __name__ == "__main__":
    reset_topic()
