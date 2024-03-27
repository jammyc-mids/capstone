from pgdriver import pgDriver
import subprocess
import json
import requests

def get_EC2_public_address(instance):
    acmd = f"aws ec2 describe-instances --filters \"Name=tag:Name,Values={instance}\" --query 'Reservations[*].Instances[*].PublicDnsName' --output text"
    p = subprocess.Popen(acmd, stdout=subprocess.PIPE, shell=True)
    output,error = p.communicate()
    return output.decode().splitlines()[0]

house_id=4340
payload = {'net' : [], 'irrad' : []}
pgh = pgDriver()

# generate payload first using historical data in PSQL
query = f"select nl.year,nl.month,nl.day,nl.timestamp,nl.load_value from net_load_ts nl, smartmeter m, house h where nl.meter_id=m.meter_id and m.house_id = h.house_id and h.house_id={house_id} order by (year,month,day,timestamp) desc limit 192"
data = pgh._read(query)
payload['net'] = [i[4] for i in reversed(data)]
query = f"select w.year, w.month, w.day, w.timestamp, w.irradiance_value from weather w, smartmeter m, smartmeter_weather_map swm, house h where w.weather_id = swm.weather_id and m.meter_id = swm.meter_id and m.house_id = h.house_id and h.house_id={house_id} order by (year, month, day, timestamp) desc limit 192"
data = pgh._read(query)
payload['irrad'] = [i[4] for i in reversed(data)]

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


