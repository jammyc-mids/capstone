from kafkadriver import kafkaConnector

kf = kafkaConnector()
kf.start_producer()
kf.start_consumer('classification')

while True:
    try:
        data = kf.receive_messages()
        print(f"[CLASSIFICATION]: Received {data}")
        print(f"[CLASSIFICATION]: Perform binary classification...")
        print(f"[CLASSIFICATION]: Relaying data to disaggregation...")
        kf.send_message('disaggregation', data)
    except:
        kf.consumer.close()

