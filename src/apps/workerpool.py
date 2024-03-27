from kafkadriver import kafkaConnector
import subprocess

class workerPool:
    def __init__(self, type, num_workers):
        self.kafkaConn = kafkaConnector()
        self.modelhost = self.get_EC2_public_address("csprod-mi-01")

    def get_EC2_public_address(self, instance):
        acmd = f"aws ec2 describe-instances --filters \"Name=tag:Name,Values={instance}\" --query 'Reservations[*].Instances[*].PublicDnsName' --output text"
        p = subprocess.Popen(acmd, stdout=subprocess.PIPE, shell=True)
        output,error = p.communicate()
        return output.decode()

    def start_classification_worker(self, num_workers):
        self.kafkaConn.start_producer()
        self.kafkaConn.start_consumer_threads('classification', num_workers)

	while True:
    	    try:
                data = self.kafkaConn.receive_messages()
                print(f"[CLASSIFICATION]: Received {data}")
                print(f"[CLASSIFICATION]: Perform binary classification...")
                print(f"[CLASSIFICATION]: Relaying data to disaggregation...")
                self.kafkaConn.send_message('disaggregation', data)
            except:
                self.kafkaConn.consumer.close()

    def start_disaggregation_worker(self, num_workers):
        self.kafkaConn.start_producer()
        self.kafkaConn.start_consumer_threads('disaggregation', num_workers)

	while True:
    	    try:
                data = self.kafkaConn.receive_messages()
                print(f"[DISAGGREGATION]: Received {data}")
                print(f"[DISAGGREGATION]: Perform disaggregation...")
                print(f"[DISAGGREGATION]: Relaying data to result recorder...")
                self.kafkaConn.send_message('result_recorder', data)
            except:
                self.kafkaConn.consumer.close()

    def start_result_recorder_workers(self, num_workers):
        self.kafkaConn.start_consumer_threads('result_recorder', num_workers)

	while True:
    	    try:
                data = self.kafkaConn.receive_messages()
                print(f"[RESULTRECORDER]: Received {data}")
                print(f"[RESULTRECORDER]: Perform result recording...")
            except:
                self.kafkaConn.consumer.close()


