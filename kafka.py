from confluent_kafka import Consumer, KafkaException, KafkaError
import os


class KafkaConsumer:
    def __init__(self, broker, group_id, topic):
        self.conf = {
            "bootstrap.servers": broker,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        self.consumer = Consumer(self.conf)
        self.consumer.subscribe([topic])

    def consume_messages(self, timeout=1.0):
        messages = []
        try:
            while True:
                msg = self.consumer.poll(timeout)
                if msg is None:
                    break
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        raise KafkaException(msg.error())
                messages.append(msg.value().decode("utf-8"))
                self.consumer.commit(asynchronous=False)
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()
        return messages
