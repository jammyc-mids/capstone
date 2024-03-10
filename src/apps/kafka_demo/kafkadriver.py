from confluent_kafka import Producer, Consumer
import json

class kafkaConnector:
    def __init__(self):
        self.kafkasrv = "localhost"
        self.port = "9092"
        self.bootstrap_srv = f"{self.kafkasrv}:{self.port}"
        self.supported_topics = ['classification', 'disaggregation']
        self.producer_conf = {'bootstrap.servers': self.bootstrap_srv, 'client.id': 'kafka-python-producer'}
        self.consumer_conf = {'bootstrap.servers': self.bootstrap_srv, 'group.id': 'kafka-python-consumer', 
                              'auto.offset.reset': 'earliest'}

    def send_message(self, topic, msg):
        self.producer.produce(topic, json.dumps(msg).encode('utf-8'), callback=self.delivery_report)
        self.producer.flush()

    def delivery_report(self, err, msg):
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def receive_messages(self):
        while True:
            print(f"Staring consumer polling...")
            msg = self.consumer.poll(1.0)  # Poll for new messages
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue
            data = json.loads(msg.value().decode('utf-8'))
            #print("Received message: ", data)
            return data

    def start_producer(self):
        print("Started producer...")
        self.producer = Producer(self.producer_conf)

    def start_consumer(self, topic):
        if hasattr(self, 'consumer'):
            print("Close existing consumer...")
            self.consumer.close() 
        print(f"Started consumer on topic [{topic}]...")
        self.consumer = Consumer(self.consumer_conf)
        self.consumer.subscribe([topic])

