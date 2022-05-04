from confluent_kafka import Consumer
from time import sleep
import datetime
from collections import Counter

class BatchManager:
    def __init__(self, batch_interval_seconds, poll_interval_seconds):
        self.micro_batch_interval_millis = batch_interval_seconds * 1000

        self.upper_boundary = 0
        self.current_boundary = 0
        self.current_boundary_empty_records = 0
        self.is_first_iteration = True

        self.poll_interval = poll_interval_seconds

    def calculate_iteration_boundaries(self, new_current_boundary):
        change_iteration = False  # by default change iteration is always false

        if self.is_first_iteration:
            self.current_boundary = new_current_boundary
            # as an exception, commit the boundaries w/o changing the batch, to get
            # actual values for lower and upper boundaries for further processing
            self.commit_iteration_boundaries()

            # no longer the first iteration, can be set here as there is no need in transaction
            # if the application is failed, it will anyway restart with is_first_iteration = true
            self.is_first_iteration = False

        elif new_current_boundary >= self.upper_boundary:
            change_iteration = True

        # set current_boundary to the current latest timestamp
        # received non-empty ConsumerRecords, so reset the empty boundaries
        self.current_boundary = self.current_boundary_empty_records = new_current_boundary

        return change_iteration

    def calculate_iteration_boundaries_empty(self):
        if self.upper_boundary == 0:  # if upper boundary is unset
            return False  # forbid iteration change, only possible when start application and no data coming

        self.current_boundary_empty_records += (self.poll_interval * 1000)
        change_iteration = self.current_boundary_empty_records >= self.upper_boundary

        if change_iteration:
            # here the change of iteration means that it was finished by the poll (or series of polls)
            # with no data in ConsumerRecords, so it should continue as it were a first iteration
            self.is_first_iteration = True

            # refresh boundaries for the case when empty ConsumerRecords continue coming in order to break
            # the change_iteration flag (it has no effect when real data comes as we have isFirstIteration = true)
            self.current_boundary = self.current_boundary_empty_records

        return change_iteration

    def commit_iteration_boundaries(self):
        self.upper_boundary = self.current_boundary + self.micro_batch_interval_millis

class BatchConsumer:
    def __init__(self, consumer, batch_manager):
        self.consumer = consumer
        self.batch_manager = batch_manager
        self.records_list = list()

    def consume_records(self):
        try:
            while True:
                # this method returns immediately if there are records available -
                # otherwise, it will await the passed timeout if the timeout expires,
                # an empty record set will be returned (poll is executed, new loop started)
                record = self.consumer.poll(self.batch_manager.poll_interval)

                if record is not None:
                    if record.error():  # error in consumer message
                        logging.error(f'Consumer error: {record.error()}')
                    else:  # valid non-none message passed
                        self.parse_kafka_message(record)

                        if self.batch_manager.calculate_iteration_boundaries(record.timestamp()[1]):
                            self.finish_micro_batch()
                elif self.batch_manager.calculate_iteration_boundaries_empty():  # no message consumed
                    self.finish_micro_batch()
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()
    


    def finish_micro_batch(self):
        if len(self.records_list) > 0:
            text=""
            for i in self.records_list:
               sr=i.decode('utf-8')
               text=text+" "+sr
            hashtag_list=[]
            for word in text.split():
                   if word[0] == '#':
                         hashtag_list.append(word)
            c = Counter(hashtag_list)
            print(hashtag_list)
            #print(self.records_list)
        self.records_list.clear()
        self.consumer.commit(asynchronous=False)
        self.batch_manager.commit_iteration_boundaries()

    def parse_kafka_message(self, record):
        record_time = datetime.datetime.utcfromtimestamp(record.timestamp()[1] / 1000).strftime('%Y-%m-%d %H:%M:%S')

        self.records_list.append(record.value())
        
batch_manager = BatchManager(int(900), int(10))

consumer = Consumer({'bootstrap.servers': "localhost:9092",
                         'group.id': "consumer-1",
                         'auto.offset.reset': 'earliest',
                         'enable.auto.commit': False})

consumer.subscribe(["tweets"])

batch_consumer = BatchConsumer(consumer, batch_manager)
batch_consumer.consume_records()

