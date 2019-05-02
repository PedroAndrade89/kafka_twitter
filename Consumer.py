import json
from kafka import KafkaConsumer

def consume():
    # To consume latest messages and auto-commit offsets and also decode from raw bytes to utf-8
    consumer = KafkaConsumer('tweets',
                             bootstrap_servers=['pandrade-prem-2.openstacklocal.com:6667'],value_deserializer=lambda m: json.loads(m.decode('utf-8')),consumer_timeout_ms=10000)

    for message in consumer:
        # message value and key are raw bytes -- need to decode

        print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                              message.offset, message.key,
                                              message.value))

    consumer.close()


if __name__ == '__main__':
   consume()