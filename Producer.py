import json, time, avro
from kafka import KafkaProducer
from twython import Twython


def injest_data(list):


    #serialize dict to string via json and encode to bytes via utf-8
    p = KafkaProducer(bootstrap_servers='pandrade-prem-2.openstacklocal.com:6667', acks='all',value_serializer=lambda m: json.dumps(m).encode('utf-8'), batch_size=1024)

    for item in list:
        p.send('tweets', value=item)

    p.flush(100)
    p.close()


def main():

    # Load credentials from json file
    with open("twitter_credentials1.json", "r") as file:
        creds = json.load(file)

    # Instantiate
    python_tweets = Twython(creds['CONSUMER_KEY'], creds['CONSUMER_SECRET'])

    # search query
    query = {'q': 'cloudera', 'result_type': 'mixed', 'count': 10}

    #result is a python dict of tweets
    result = python_tweets.search(**query)['statuses']

    injest_data(result)


if __name__ == '__main__':
    main()




