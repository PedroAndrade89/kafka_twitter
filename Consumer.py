import json
from kafka import KafkaConsumer

def consume():
    # To consume latest messages and auto-commit offsets
    consumer = KafkaConsumer('tweets',
                             bootstrap_servers=['pandrade-prem-2.openstacklocal.com:6667'],value_deserializer=lambda m: json.loads(m.decode('ascii')),consumer_timeout_ms=10000)

    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                              message.offset, message.key,
                                              message.value))

    consumer.close()


if __name__ == '__main__':
   consume()