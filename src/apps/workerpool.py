from kafkadriver import kafkaConnector
from pgdriver import pgConnector
import requests
import subprocess
import json, re
import pdb

class workerPool:
    def __init__(self):
        self.kafkaConn = kafkaConnector()
        self.dis_modelhost = self.get_EC2_public_address("csprod-mi-dis")
        self.clf_modelhost = self.get_EC2_public_address("csprod-mi-clf")
        self.port = 20200
        self.dis_base_url = "/predictions/disagg_cpu"
        self.clf_base_url = "/predictions/clf_cpu"
        self.headers = {'Content-Type': 'application/json'}
        self.pv_clf_threshold = 0.5
        self.pgh = pgConnector()

    def get_EC2_public_address(self, instance):
        acmd = f"aws ec2 describe-instances --filters \"Name=tag:Name,Values={instance}\" --query 'Reservations[*].Instances[*].PublicDnsName' --output text"
        p = subprocess.Popen(acmd, stdout=subprocess.PIPE, shell=True)
        output,error = p.communicate()
        return output.decode().splitlines()[0]

    def _requestPrediction(self, rtype, payload):
        if rtype == 'disaggregation':
            url = f"http://{self.dis_modelhost}:{self.port}{self.dis_base_url}"
        elif rtype == 'classification':
            url = f"http://{self.clf_modelhost}:{self.port}{self.clf_base_url}"
        else:
            print(f"Invalid request type [{rtype}]")
            return {}
        resp = requests.post(url, data=json.dumps(payload), headers=self.headers)
        if resp.status_code == 200:
            try:
                return resp.json()
            except:
                print(f"_requestPrediction: no return data from [{rtype}] prediction service.")
                pass
        else:
            print(f"_requestPrediction: failed, status: {resp.status_code}")
        return {}

    def start_classification_workers(self, num_workers=1):
        topic = 'classification'
        print(f"[{topic.upper()}]: Model host: [{self.clf_modelhost}]")
        self.kafkaConn.start_producer()
        print(f"[{topic.upper()}]: Started producer.")
        self.kafkaConn.start_consumer(topic)
        print(f"[{topic.upper()}]: Started consumer on topic [{topic}].")
        while True:
            try:
                data = self.kafkaConn.receive_messages()
                print(f"[{topic.upper()}]: Perform binary classification...")
                # extract the 1-D net load frames
                # need to revisit this
                req = {'net' : data['clf_net']}
                if len(data['clf_net']) == 0:
                    print("f[{topic.upper()}]: Bad clf_net day frame [data], skipped.")
                    continue

                presult = self._requestPrediction(topic, req)
                data['clf_result'] = presult[0]
                if presult[0] > self.pv_clf_threshold:
                    print(f"[{topic.upper()}]: PV installation detected. [{presult[0]}]")
                    print(f"[{topic.upper()}]: Relaying data to disaggregation...")
                    data['has_solar'] = True
                    self.kafkaConn.send_message('disaggregation', data)
                else:
                    # we will store the last day net-load signal to DB
                    print(f"[{topic.upper()}]: No PV installation found. [{presult[0]}]")
                    print(f"[{topic.upper()}]: Relaying data to result recording...")
                    data['has_solar'] = False
                    self.kafkaConn.send_message('result_recorder', data)
            except:
                self.kafkaConn.consumer.close()
                break

    def start_disaggregation_workers(self, num_workers=1):
        topic = 'disaggregation'
        print(f"[{topic.upper()}]: Model host: [{self.dis_modelhost}]")
        self.kafkaConn.start_producer()
        print(f"[{topic.upper()}]: Started producer.")
        self.kafkaConn.start_consumer(topic)
        print(f"[{topic.upper()}]: Started consumer on topic [{topic}].")
        while True:
            try:
                data = self.kafkaConn.receive_messages()
                # only need both net and irrad data
                req = {'net' : data['net'], 'irrad' : data['irrad']}
                print(f"[{topic.upper()}]: Perform disaggregation...")
                presult = self._requestPrediction(topic, req)
                data['pv_result'] = presult[0]
                print(f"[{topic.upper()}]: PV prediction: [{presult[0]}]")
                print(f"[{topic.upper()}]: Relaying data to result recorder...")
                self.kafkaConn.send_message('result_recorder', data)
            except:
                self.kafkaConn.consumer.close()
                break

    def start_result_recorder_workers(self, num_workers=1):
        topic = 'result_recorder'
        self.kafkaConn.start_consumer(topic)
        print(f"[{topic.upper()}]: Started consumer on topic [{topic}].")
        while True:
            try:
                data = self.kafkaConn.receive_messages()
                print(f"[{topic.upper()}]: Received data:\n\t{data}")
                query = f"update house set has_solar={data['has_solar']} where house_id={data['house_id']}"
                try:
                    self.pgh._update(query)
                except:
                    print(f"Unable to update house property: {data['house_id']}")
                    pass

                if 'pv_result' not in data:
                    # set default for non-PV house to 0.0
                    data['pv_result'] = 0.0

                query = f"insert into prediction_results (house_id, timestamp, clf_result, pv_result) values "
                query += f"({data['house_id']}, TO_TIMESTAMP('{data['current_ts']}', 'YYYY-MM-DD HH24:MI'), {data['clf_result']}, {data['pv_result']}) returning *"
                try:
                    self.pgh._update(query)
                except:
                    print(f"Unable to insert prediction_results: {data['house_id']}")
                    pass

                #Store predicted PV step in pv_load_ts table
                query = f"insert into pv_load_ts (meter_id, h_timestamp, month, day, year, timestamp, pv_value) values "
                query += f"({data['meter_id']}, TO_TIMESTAMP('{data['current_hm']}', 'HH24:MI'), {data['current_month']}, {data['current_day']}, {data['current_year']}, "
                query += f"TO_TIMESTAMP('{data['current_ts']}', 'YYYY-MM-DD HH24:MI'), {data['pv_result']}) returning *"
                try:
                    self.pgh._update(query)
                except:
                    print(f"Unable to insert pv_load_ts: {data['house_id']}")
                    pass
                self.pgh.dbh.commit()

            except:
                self.kafkaConn.consumer.close()
                break

