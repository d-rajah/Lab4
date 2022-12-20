import sys
import tweepy
from kafka import KafkaProducer
from kafka.errors import KafkaError

def main(argv):
    # kafka broker address
    producer = KafkaProducer(bootstrap_servers=[argv])
    
    #Query tweets that contain these topics
    #Topics were gathered December 17th
    query = 'Colts OR Matt Ryan OR Ravens OR Lamar OR Greg Roman OR Cubs OR Harbaugh OR P-22 OR Vikes OR Jeff Saturday OR Dansby -is:retweet -has:media -has:images'
    response = client.search_recent_tweets(query=query, max_results=100,tweet_fields=['created_at', 'lang'])
    for tweet in response.data:
        # send msg to topic Tweets
        future = producer.send("Tweets", bytes(tweet.text,encoding='utf-8'))
        try:
            record_metadata = future.get(timeout=10)
            print(record_metadata)
        except KafkaError as e:
            print(e)
    producer.flush()


if __name__ == '__main__':
    main(sys.argv[1])
