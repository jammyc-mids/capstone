from pgdriver import pgConnector
from datetime import datetime
import subprocess
import json
import requests
import pdb

def get_EC2_public_address(instance):
    acmd = f"aws ec2 describe-instances --filters \"Name=tag:Name,Values={instance}\" --query 'Reservations[*].Instances[*].PublicDnsName' --output text"
    p = subprocess.Popen(acmd, stdout=subprocess.PIPE, shell=True)
    output,error = p.communicate()
    return output.decode().splitlines()[0]

house_id=4340
# what else we need to put in the request???
payload = {'house_id' : house_id, 'net' : [], 'irrad' : []}
pgh = pgConnector()

# generate payload first using historical data in PSQL
# Disaggregation: provided timestamp - 192 steps sliding window
# Classifcation: provided timestamp - 96 - extract full 96 steps on that day
timestamp = '2018-11-23 05:15'
timeobj = datetime.strptime(timestamp, "%Y-%m-%d %H:%M")

query = f"select nl.year,nl.month,nl.day,nl.timestamp,nl.load_value from net_load_ts nl, smartmeter m, house h where nl.meter_id=m.meter_id and m.house_id = h.house_id and h.house_id=4340 and nl.timestamp < '{timestamp}' order by timestamp desc limit 192"

data = pgh._read(query)
payload['net'] = [i[4] for i in reversed(data)]
query = f"select w.year, w.month, w.day, w.timestamp, w.irradiance_value from weather w, smartmeter m, smartmeter_weather_map swm, house h where w.weather_id = swm.weather_id and m.meter_id = swm.meter_id and m.house_id = h.house_id and h.house_id={house_id} and w.timestamp < '{timestamp}' order by timestamp desc limit 192"
data = pgh._read(query)
payload['irrad'] = [i[4] for i in reversed(data)]

query = f"select nl.year,nl.month,nl.day,nl.timestamp,nl.load_value from net_load_ts nl, smartmeter m, house h where nl.meter_id=m.meter_id and m.house_id = h.house_id and h.house_id=4340 and nl.year={timeobj.year} and nl.month={timeobj.month} and nl.day = {timeobj.day - 1} order by timestamp"
data = pgh._read(query)
payload['clf_net'] = [i[4] for i in data]

print(payload)
headers = {'Content-Type': 'application/json'}
port = 20100

app_host = get_EC2_public_address("csprod-infra-01")
url = f"http://{app_host}:{port}/predict"
resp = requests.post(url, data=json.dumps(payload), headers=headers)
if resp.status_code == 200:
    print(f"Prediction request posted successfully to [{url}]")
else:
    print(f"Prediction request failed [{url}]")


