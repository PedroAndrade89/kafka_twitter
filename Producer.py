from kafka import KafkaProducer
from twython import Twython
import json


def injest_data(dictionay):
    producer = KafkaProducer(bootstrap_servers='pandrade-prem-2.openstacklocal.com:6667', acks='all', batch_size=1024,value_serializer=lambda m: json.dumps(m).encode('ascii'))

    for value in dictionay:
        producer.send('kafka-dev-101', value)

    producer.close()


def main():

    # Load credentials from json file
    with open("twitter_credentials.json", "r") as file:
        creds = json.load(file)

    # Instantiate
    python_tweets = Twython(creds['CONSUMER_KEY'], creds['CONSUMER_SECRET'])

    # query
    query = {'q': 'venezuela',
             'result_type': 'mixed',
             'count': 10
      #       'lang': 'en'
             }

    result = python_tweets.search(**query)['statuses']

    injest_data(result)


if __name__ == '__main__':
   main()




