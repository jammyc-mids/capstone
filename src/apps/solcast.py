import requests
import json
from rejsondriver import rejsonConnector
import re

########
# JSON structure of irradiance cache date in Redis
# lat+long (key) : 
#	<YYYY-MM-DD> : [
#			 'air_temp' : <value>,
#			 'ghi' : <value>
#                      ],
#                      [ 
#			 'air_temp' : <value>,
#			 'ghi' : <value>
#                      ],
#                      ... (for the whole day, 30 mins each)
#       <YYYY-MM-DD> : [
#			 'air_temp' : <value>,
#			 'ghi' : <value>
#                      ],
#                      [ 
#			 'air_temp' : <value>,
#			 'ghi' : <value>
#                      ],
#                      ... (for the whole day, 30 mins each, total 48 per day)
#       ... (For other days)
# ... (For other lat+long keys)

class solcastData():
    def __init__(self):
        self.api_key = "jlJtypiMie8xwaN_1lPJ_tX3agCbvwbo"
        self.url_root = "https://api.solcast.com.au/data/"
        self.dtype = "historic/radiation_and_weather"
        self.rdconn = rejsonConnector()

    def updateSolarData(self, lat, long, start, duration, format='json'):
        headers = {'Content-Type' : 'application/json'}
        url = self.url_root + self.dtype
        url += f"?latitude={lat}&longitude={long}&start={start}&duration=P{duration}D&format={format}"
        url += f"&api_key={self.api_key}"
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            apidata = resp.json()
            # store data to Redis
            return self.storeSolarData(lat, long, apidata)
        else:
            print(f"Unable to get data from SolCast: {resp.status_code}")
            return False

    def convertLatLongToString(self, lat, long):
       latstr = str(lat).replace('.', '_')
       longstr = str(long).replace('.', '_')
       return f"{latstr}+{longstr}"

    def convertLatLongFromString(self, llstr):
       latstr, longstr = llstr.split('+')
       lat = latstr.replace('_', '.')
       long = longstr.replace('_', '.')
       return float(lat), float(long)

    def storeSolarData(self, lat, long, sdata):
       skey = self.convertLatLongToString(lat, long)
       current_date = ""
       idata = {}
       for entry in sdata['estimated_actuals']:
           mat = re.search(r'^(\d{4}\-\d{2}\-\d{2})T', entry['period_end'])
           if mat:
               if current_date != mat.group(1):
                   current_date = mat.group(1) 
                   if current_date not in idata:
                       idata[current_date] = []
               idata[current_date].append({'air_temp' : entry['air_temp'], 'ghi' : entry['ghi']})
       try:
           self.rdconn.rejsonSet(skey, ".", idata) 
           return True
       except:
           print(f"Unable to update Redis for {skey}.")
           return False
        
    def getSolarData(self, lat, long, dlist):
       skey = self.convertLatLongToString(lat, long)
       result = {}
       for dd in dlist: 
           try:
               rdata = self.rdconn.rejsonGet(skey, f".{dd}") 
               if rdata:
                   result[dd] = rdata
           except:
               # skip any days cannot be found 
               print(f"Irradiance data is not available for Lat:{lat}, Long:{long} on {dd}")
       return result

# Berkeley CA
#solcastData().updateSolarData(37.8718, -122.2718, '2024-02-01', 29)
# New York NY
#solcastData().updateSolarData(40.7508, -73.9961, '2024-02-01', 29)
#sdata = solcastData().getSolarData(37.8718, -122.2718, ['2024-02-01', '2024-02-02', '2024-02-06'])
#print(json.dumps(sdata, indent=4))

