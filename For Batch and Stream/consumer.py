from confluent_kafka import Consumer
from time import sleep
import schedule
from collections import Counter
import time


def print_hashtags_list(l):
        if len(l) > 0:
            text=""
            for i in l:
               sr=i.decode('utf-8')
               text=text+" "+sr
            hashtag_list=[]
            for word in text.split():
                   if word[0] == '#':
                         hashtag_list.append(word)
            c = Counter(hashtag_list)
            print(hashtag_list)
        return []

class ExampleConsumer:
    broker = "localhost:9092"
    topic = "tweets"
    group_id = "consumer-1"

    def start_listener(self):
        consumer_config = {
            'bootstrap.servers': self.broker,
            'group.id': self.group_id,
            'auto.offset.reset': 'largest',
            'enable.auto.commit': 'false',
            'max.poll.interval.ms': '86400000'
        }

        consumer = Consumer(consumer_config)
        consumer.subscribe([self.topic])

        try:
            l=[]
            start_time = time.time()
            mins_15=[i for i in range(890,901)]
            for i in range(0,11):
            	mins_15.append(i)
            while True:
                #print("Listening")
                # read single message at a time
                msg = consumer.poll(0)
                if msg is None:
                    sleep(5)
                    continue
                if msg.error():
                    print("Error reading message : {}".format(msg.error()))
                    continue
                # You can parse message and save to data base here
                #print(int(time.time() - start_time))
                l.append(msg.value())
                #print(int(time.time()-start_time))
                print(msg.value())
                if int(time.time() - start_time)%900 in mins_15:
                    l=print_hashtags_list(l)
                consumer.commit()

        except Exception as ex:
            print("Kafka Exception : {}", ex)

        finally:
            print("closing consumer")
            consumer.close()

#RUNNING CONSUMER FOR READING MESSAGE FROM THE KAFKA TOPIC
my_consumer = ExampleConsumer()
my_consumer.start_listener()

