import zipcodes
import json
import re

class USZipCodes():
   def __init__(self):
      self.zipfile = "uszips.json"
      self.zipcodes_data = self.getZipData()
      self.latlongmap = {z['zip_code'] : {'coordinates' : (z['lat'], z['long']), 'city' : z['city'], 'state' : z['state'], 'county' : z['county']} for z in self.zipcodes_data if z['active'] == True}
      self.zipcodes = [z for z in self.latlongmap]

   def getZipData(self):
      try:
          with open(self.zipfile) as f:
              zdata = json.loads(f.read()) 
      except:
          # retrieve zip code data online
          zdata = zipcodes.list_all()
          self.cacheZipData(zdata)
      return zdata

   def cacheZipData(self, data):
      with open(self.zipfile, "w+") as f:
         f.write(json.dumps(data, indent=4))
      f.close()

   def getCoordinatesByZip(self, zipcode):
      if zipcode not in self.zipcodes:
         print(f"Unknown zip code {zipcode}")
         return (0.0, 0.0)
      entry = self.latlongmap[zipcode]
      return entry['coordinates']

   def getCoordinatesByCounty(self, county):
      try:
         # there maybe more than one return, just return the first matched entry
         return [v['coordinates'] for z,v in self.latlongmap.items() if re.search(r'^%s' % county.upper(), v['county'].upper())][0]
      except:
         print(f"Unknown country {county}")
         return (0.0, 0.0)

   def getCoordinatesByCity(self, city, state):
      try:
         # there maybe more than one return, just return the first matched entry
         return [v['coordinates'] for z,v in self.latlongmap.items() if city.upper() == v['city'].upper() and state.upper() == v['state'].upper()][0]
      except:
         print(f"Unknown city {city}, {state}")
         return (0.0, 0.0)

   def getCoordinatesByState(self, state):
      try:
         # there maybe more than one return, just return the first matched entry
         return [v['coordinates'] for z,v in self.latlongmap.items() if state.upper() == v['state'].upper()][0]
      except:
         print(f"Unknown state {state}")
         return (0.0, 0.0)

