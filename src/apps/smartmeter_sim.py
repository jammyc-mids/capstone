from pgdriver import pgConnector
import pandas as pd
from psycopg2 import Error
from datetime import datetime, timedelta
import subprocess
import json, re
import requests
import time
import pdb

def get_EC2_public_address(instance):
    acmd = f"aws ec2 describe-instances --filters \"Name=tag:Name,Values={instance}\" --query 'Reservations[*].Instances[*].PublicDnsName' --output text"
    p = subprocess.Popen(acmd, stdout=subprocess.PIPE, shell=True)
    output,error = p.communicate()
    return output.decode().splitlines()[0]

def get_irradiance_sequence(idata, day, hour, minute):
    if day not in idata:
        print(f"Day not found in irradiance data: {day} {hour} {minute}")
        return 0

    #print(f"irradance size: {len(idata[day])} on day [{day}]")
    # NOTE: Solcast data is 30 mins interval data
    index = int(hour) * 2 + int(minute) // 30
    if 0 <= index < 48:
        try:
            return idata[day][index]['ghi']
        except:
            print(f"Missing irradiance data: default to 0")
            return 0
    else:
        print(f"Bad index {index}: hour:{hour}, min:{minute} for day {day}")
        return 0

#payload = {'house_id' : house_id, 'net' : [], 'irrad' : []}
pgh = pgConnector(transaction=True)

df = pd.read_csv("/opt/data/pecan/pecan_meta.csv")
houses = list(set(df['house_id']))
#print(houses)
headers = {'Content-Type': 'application/json'}
port = 20100
app_host = get_EC2_public_address("csprod-infra-01")

url = f"http://{app_host}:{port}/getRadianceData"
# use startdate and duration
#houses = [3687, 9160]
houses = [
#203, 1222, 3039, 2606, 8386, 2818, 3864, 8565, 4031, 4495, 4934, 5679, 8733, 9160, 9019, 9213, 2358, 3488, 9278, 5997, 3700, 3996, 9612, 9775, 27, 9836, 9922, 4283, 3687, 2318, 914, 1450, 1524, 2096, 1642, 142, 5587, 1240, 387, 1417, 5058, 9053, 2335, 3456, 4767, 2361, 3538, 4373, 5746, 6139
#387, 1417, 142, 2318, 914, 3996, 3488, 5679, 2096, 558, 2358, 950, 3000, 4283, 3517, 5058, 4550, 1222, 5587, 1240, 9053, 5982, 5997, 3700
#6377, 6547, 661, 7536, 7062, 7719, 7901, 7951, 8156, 8342,
#1731, 558, 5982, 3517, 950, 4550, 3000
7800
]

for h in houses:
   # get raw time series data from Pecan
   house_data = pd.read_csv(f"/opt/data/pecan/{h}.csv")

   # get irradiance data from Redis
   query = f"select city, state from house, city, state where city.city_id = house.city_id and state.state_id = house.state_id and house.house_id={h}"
   hloc = pgh._read(query)
   payload = {'city' : hloc[0][0], 'state' : hloc[0][1]}
   resp = requests.post(url, data=json.dumps(payload), headers=headers)
   if resp.status_code == 200:
      irradiance = resp.json()
      irradiance = irradiance['result']
   else:
      print(f"Unable to find Irradiance data for house {h}, skipped.")
      continue

   query = f"select meter_id from smartmeter where house_id={h}"
   meter_id = pgh._read(query)[0][0]

   # iterate each row
   payload = {'net' : [], 'irrad' : [], 'clf_net' : []}
   for idx, entry in house_data.iterrows():
      stime = entry['local_15min']
      stime = stime[:-3]
      timeobj = datetime.strptime(stime, "%Y-%m-%d %H:%M:%S")
      year = timeobj.year
      mon = timeobj.month
      day = timeobj.day
      hour = timeobj.hour
      mins = timeobj.minute
      full_ts = timeobj.strftime('%Y-%m-%d %H:%M')
      print(f"Processing timestamp [{full_ts}]...")
      hm_ts = timeobj.strftime('%H:%M')
      #if not (mon == 1 and day in [1,2]):
      if not (mon == 1 and day in [1,2]):
         # get the net_load and irradiance data
         #time.sleep(1)
         payload['house_id'] = h
         payload['meter_id'] = meter_id
         payload['current_ts'] = full_ts
         payload['current_hm'] = hm_ts
         payload['current_year'] = year
         payload['current_month'] = mon
         payload['current_day'] = day
         query = f"select nl.year,nl.month,nl.day,nl.timestamp,nl.load_value from net_load_ts nl, smartmeter m, house h where nl.meter_id=m.meter_id and m.house_id = h.house_id and h.house_id={h} and nl.timestamp < '{full_ts}' order by timestamp desc limit 192"
         data = pgh._read(query)
         payload['net'] = [i[4] for i in reversed(data)]
         query = f"select w.year, w.month, w.day, w.timestamp, w.irradiance_value from weather w, smartmeter m, smartmeter_weather_map swm, house h where w.weather_id = swm.weather_id and m.meter_id = swm.meter_id and m.house_id = h.house_id and h.house_id={h} and w.timestamp < '{full_ts}' order by timestamp desc limit 192"
         data = pgh._read(query)
         payload['irrad'] = [i[4] for i in reversed(data)]

         yesterday_ts = datetime.strptime(f"{year}-{mon}-{day}", '%Y-%m-%d') - timedelta(days=1)

         query = f"select nl.year,nl.month,nl.day,nl.timestamp,nl.load_value from net_load_ts nl, smartmeter m, house h where nl.meter_id=m.meter_id and m.house_id = h.house_id and h.house_id={h} and nl.year={yesterday_ts.year} and nl.month={yesterday_ts.month} and nl.day={yesterday_ts.day} order by timestamp"
         data = pgh._read(query)
         payload['clf_net'] = [i[4] for i in data]
         murl = f"http://{app_host}:{port}/predict"
         resp = requests.post(murl, data=json.dumps(payload), headers=headers)
         if resp.status_code == 200:
             print(f"[H:{h}, M:{meter_id}] Prediction request posted successfully to [{murl}]")
         else:
             print(f"[H:{h}, M:{meter_id}] Prediction request failed [{murl}]")

      # insert net_load
      query = f"insert into net_load_ts (meter_id, month, day, year, h_timestamp, timestamp, load_value) values ({meter_id}, {mon}, {day}, {year}, '{hm_ts}', '{full_ts}', {entry['grid']}) returning *"
      #print(f"{h} {query}")
      try:
         rc = pgh._insert(query)
      except:
         pass

      # insert irradiance/weather
      ghi = get_irradiance_sequence(irradiance, f"{year}-{mon:02d}-{day:02d}", hour, mins)
      query = f"insert into weather (month, day, year, h_timestamp, timestamp, irradiance_value) values ({mon}, {day}, {year}, '{hm_ts}', '{full_ts}', {ghi}) returning *"
      #print(f"{h} {query}")
      wid = 0
      try:
         wid = pgh._insert(query)
      except:
         pass

      # insert smartmeter/weather map
      query = f"insert into smartmeter_weather_map (meter_id, weather_id) values ({meter_id}, {wid}) returning *"
      #print(f"{h} {query}")
      try:
         rc = pgh._insert(query)
      except:
         pass

      pgh.dbh.commit()
