from kafkadriver import kafkaConnector

kf = kafkaConnector()
kf.start_consumer('disaggregation')

while True:
    try:
        data = kf.receive_messages()
        print(f"[DISAGGREGATION]: {data}")
    except:
        kf.consumer.close()

